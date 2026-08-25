// Package workspacelease serializes Delivery writers that target the same
// workspace. Readers never acquire a lease.
//
// A writer leases the workspace for the single write it is about to perform,
// not for the whole turn: path-bound writers (write_file, edit_file, …) lease
// exactly the paths their arguments name, and opaque writers (bash, MCP)
// lease the whole workspace, because their targets cannot be judged from
// arguments. The lease is released when that write finishes, so two sessions
// writing disjoint paths run concurrently and a session that only reads never
// waits. Verification calls re-acquire the whole workspace for the length of
// the command, so a check another session could invalidate mid-run stays
// stable.
//
// Cross-process serialization is a short-lived arbiter file lock guarding a
// claims table: every live acquisition is a row {owner, domain, expiry}, and
// the row is the only thing that outlives the critical section. A crashed
// process is recovered by the expiry alone — the arbiter lock itself dies
// with its process, and the stale row stops blocking after claimTTL — so the
// lease never needs a watcher to clean it up.
package workspacelease

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

const retryInterval = 75 * time.Millisecond

// claimTTL is how long a written claim survives without the writer doing
// anything. It bounds crash recovery (a dead session blocks the paths it had
// leased for at most this long) and doubles as the longest any lease is held
// without being refreshed — which is fine, because the lease is re-acquired
// for every write. Tests shorten it to watch expiry without waiting out the
// real one; the cross-process crash test shortens it in the helper too.
var claimTTL = 30 * time.Second

func init() {
	if v := os.Getenv("REASONIX_WORKSPACE_LEASE_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			claimTTL = d
		}
	}
}

// waitNoticeGrace is how long a contended acquisition stays silent. Contention
// is either milliseconds, another session between two writes, or the length of
// a whole turn, and reporting the first kind leaves a permanent line about a
// wait nobody waited through. Lowered by tests.
var waitNoticeGrace = time.Second

var errHeld = errors.New("workspace write lease is held")

// errClaimConflict is returned inside an arbiter critical section when the
// requested domain overlaps a live claim held by another session.
var errClaimConflict = errors.New("workspace write claim conflicts with another session")

// WaitOutcome says which end of a contended acquisition a Wait reports.
type WaitOutcome int

const (
	// WaitBegan opens a wait that has already outlived waitNoticeGrace.
	WaitBegan WaitOutcome = iota
	// WaitAcquired closes one with the lease in hand.
	WaitAcquired
	// WaitAbandoned closes one without it: the caller's context ended first.
	WaitAbandoned
)

// Wait reports one contended acquisition. A wait under the grace is never
// reported at all, and a reported one always arrives as a pair, so nothing on
// screen is left claiming a wait that is already over.
type Wait struct {
	Outcome WaitOutcome
	Elapsed time.Duration
}

// WaitNotice receives both ends of a reported wait. It must return quickly and
// must not call back into Owner.
type WaitNotice func(Wait)

// Owner is one Delivery session's re-entrant workspace lease. One Owner may be
// shared by the root agent and all of its subagents. Different sessions must
// use different Owners, even when they share a workspace.
type Owner struct {
	root    string // canonical workspace root this owner protects
	wkDir   string // lockDir/<wkKey>: this workspace's per-workspace lock domain
	arbiter string // wkDir/arbiter.lock: short-lived cross-process arbiter
	claims  string // wkDir/claims.json: the live claims table
	id      string // unique identity of this owner inside the claims table
	onWait  WaitNotice

	// localMu serializes arbiter critical sections inside this process; the
	// arbiter file lock serializes them across processes. Held for the length
	// of a critical section only — never for the lease itself, so disjoint
	// leases in one process still run concurrently.
	localMu *sync.Mutex

	mu         sync.Mutex
	activeRuns int
	waiting    bool
	held       []Claim // locally held claims; EndRun releases the rest
}

// arbiterRegistry hands every Owner of one workspace the same in-process
// mutex, so two sessions in one process never race the arbiter file lock.
var arbiterRegistry = struct {
	sync.Mutex
	m map[string]*sync.Mutex
}{m: map[string]*sync.Mutex{}}

// State is a sanitized process-local snapshot used by Desktop to explain a
// workspace conflict. It deliberately contains no path, PID, or lock token.
type State struct {
	Acquired bool
	Waiting  bool
}

// State returns the current acquisition state without performing lease I/O.
func (o *Owner) State() State {
	if o == nil {
		return State{}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return State{Acquired: len(o.held) > 0, Waiting: o.waiting}
}

// New returns a Delivery-session lease owner for workspaceRoot. lockDir must be
// shared by Reasonix processes for cross-process protection; it is kept outside
// the workspace so acquiring a lease never dirties user files.
func New(workspaceRoot, lockDir string, onWait WaitNotice) (*Owner, error) {
	canonical, err := CanonicalWorkspace(workspaceRoot)
	if err != nil {
		return nil, err
	}
	lockDir = strings.TrimSpace(lockDir)
	if lockDir == "" {
		return nil, errors.New("workspace lease directory is unavailable")
	}
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		return nil, fmt.Errorf("create workspace lease directory: %w", err)
	}
	sum := sha256.Sum256([]byte(canonical))
	key := hex.EncodeToString(sum[:])
	wkDir := filepath.Join(lockDir, key)
	if err := os.MkdirAll(wkDir, 0o700); err != nil {
		return nil, fmt.Errorf("create workspace lease domain: %w", err)
	}

	arbiterRegistry.Lock()
	localMu := arbiterRegistry.m[wkDir]
	if localMu == nil {
		localMu = &sync.Mutex{}
		arbiterRegistry.m[wkDir] = localMu
	}
	arbiterRegistry.Unlock()

	return &Owner{
		root:    canonical,
		wkDir:   wkDir,
		arbiter: filepath.Join(wkDir, "arbiter.lock"),
		claims:  filepath.Join(wkDir, "claims.json"),
		id:      newOwnerID(),
		onWait:  onWait,
		localMu: localMu,
	}, nil
}

// newOwnerID returns a random identity for the claims table. The fallback is
// only reachable if the host entropy source fails entirely, and uniqueness
// within one host is all the table needs.
func newOwnerID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// CanonicalWorkspace returns the stable identity used to key a workspace. It
// resolves symlinks when possible and folds case on Windows, where paths are
// case-insensitive by default.
func CanonicalWorkspace(root string) (string, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return "", errors.New("workspace root is empty")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve workspace root: %w", err)
	}
	abs = filepath.Clean(abs)
	if resolved, resolveErr := filepath.EvalSymlinks(abs); resolveErr == nil {
		abs = filepath.Clean(resolved)
	} else if !os.IsNotExist(resolveErr) {
		return "", fmt.Errorf("canonicalize workspace root: %w", resolveErr)
	}
	abs = nearestGitWorktreeRoot(abs)
	if runtime.GOOS == "windows" {
		abs = strings.ToLower(filepath.ToSlash(abs))
	}
	return abs, nil
}

// nearestGitWorktreeRoot folds a repository root and any selected directory
// beneath it into one writer domain. It intentionally detects the .git marker
// through the filesystem instead of invoking Git, so the no-Git Windows path
// keeps the same safety guarantee. Linked worktrees each have their own .git
// marker and therefore remain independent writer domains.
func nearestGitWorktreeRoot(path string) string {
	start := path
	if info, err := os.Stat(path); err == nil && !info.IsDir() {
		start = filepath.Dir(path)
	}
	for current := start; ; current = filepath.Dir(current) {
		if _, err := os.Lstat(filepath.Join(current, ".git")); err == nil {
			return current
		}
		parent := filepath.Dir(current)
		if parent == current {
			return path
		}
	}
}

// BeginRun registers an agent run that participates in this session. The call
// is intentionally cheap and does not acquire any lease; read-only turns
// therefore remain fully concurrent.
func (o *Owner) BeginRun() {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.activeRuns++
	o.mu.Unlock()
}

// EndRun releases every lease this session still holds once the final
// participating run finishes. It is the safety net under explicit per-write
// releases: a write tool that forgot to release its lease (or was cancelled
// mid-flight) stops blocking other sessions here instead of at claimTTL.
func (o *Owner) EndRun() {
	if o == nil {
		return
	}
	o.mu.Lock()
	if o.activeRuns > 0 {
		o.activeRuns--
	}
	toRelease := o.releaseIfIdleLocked()
	o.mu.Unlock()
	for _, c := range toRelease {
		o.releaseClaim(c)
	}
}

func (o *Owner) releaseIfIdleLocked() []Claim {
	if o.activeRuns != 0 || len(o.held) == 0 {
		return nil
	}
	all := o.held
	o.held = nil
	return all
}

// AcquireWrite acquires this session's exclusive whole-workspace lease and
// holds it until EndRun. It is the coarse surface for callers that cannot name
// their write domain (bash, MCP, verification) or that manage the release
// themselves; path-bound tool calls should prefer AcquireWritePaths. It is
// re-entrant across parallel tool calls and shared subagents.
func (o *Owner) AcquireWrite(ctx context.Context) error {
	if o == nil {
		return nil
	}
	return o.acquireClaim(ctx, Claim{WholeWorkspace: true, WorkspaceRoot: o.root})
}

// AcquireWritePaths acquires a lease for exactly the requested write domain
// and returns the release that returns it once the write is done. Disjoint
// claims from other sessions never wait for each other; overlapping ones wait
// until the holder releases or its claim expires. Re-entrant within this
// owner: a claim already held by this session never conflicts with itself.
func (o *Owner) AcquireWritePaths(ctx context.Context, c Claim) (func(), error) {
	if o == nil {
		return noopRelease, nil
	}
	if c.Empty() {
		return noopRelease, nil
	}
	if err := o.acquireClaim(ctx, c); err != nil {
		return nil, err
	}
	var once sync.Once
	return func() {
		once.Do(func() { o.releaseClaim(c) })
	}, nil
}

func noopRelease() {}

func (o *Owner) acquireClaim(ctx context.Context, c Claim) error {
	if ctx == nil {
		ctx = context.Background()
	}
	w := &waitClock{owner: o}
	for {
		acquired, err := o.tryAcquireOnce(ctx, c)
		if err != nil {
			if ctx.Err() != nil {
				w.close(WaitAbandoned)
				return ctx.Err()
			}
			return err
		}
		if acquired {
			o.mu.Lock()
			o.held = append(o.held, c)
			o.waiting = false
			o.mu.Unlock()
			w.close(WaitAcquired)
			return nil
		}
		// The domain conflicts with a live claim: wait and retry.
		w.contend()
		w.report()
		timer := time.NewTimer(retryInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			w.close(WaitAbandoned)
			return ctx.Err()
		}
	}
}

// tryAcquireOnce runs one arbiter-protected attempt. acquired=false with a nil
// error means the domain conflicts with a live claim held by another session;
// the caller retries until the holder releases or its claim expires.
func (o *Owner) tryAcquireOnce(ctx context.Context, c Claim) (acquired bool, err error) {
	err = o.withArbiter(ctx, func() error {
		recs, err := o.readClaims()
		if err != nil {
			return err
		}
		recs = pruneExpired(recs)
		if o.conflicts(recs, c) {
			return errClaimConflict
		}
		recs = append(recs, claimRecord{
			ID:      o.id,
			Whole:   c.WholeWorkspace,
			Root:    c.WorkspaceRoot,
			Paths:   c.Paths,
			Expires: time.Now().Add(claimTTL),
		})
		if err := o.writeClaims(recs); err != nil {
			return err
		}
		acquired = true
		return nil
	})
	if errors.Is(err, errClaimConflict) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return acquired, nil
}

// withArbiter serializes one claims-table critical section. The in-process
// mutex covers sessions in one process; the arbiter file lock covers sessions
// across processes and dies with a crashed process. Never held beyond fn.
func (o *Owner) withArbiter(ctx context.Context, fn func() error) error {
	o.localMu.Lock()
	defer o.localMu.Unlock()
	release, err := o.lockArbiter(ctx)
	if err != nil {
		return err
	}
	defer release()
	return fn()
}

func (o *Owner) lockArbiter(ctx context.Context) (func(), error) {
	for {
		release, err := tryLockFile(o.arbiter)
		if err == nil {
			return release, nil
		}
		if !errors.Is(err, errHeld) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryInterval):
		}
	}
}

// claimRecord is one live acquisition in the claims table.
type claimRecord struct {
	ID      string    `json:"id"`
	Whole   bool      `json:"whole,omitempty"`
	Root    string    `json:"root,omitempty"`
	Paths   []string  `json:"paths,omitempty"`
	Expires time.Time `json:"expires"`
}

func (o *Owner) readClaims() ([]claimRecord, error) {
	data, err := os.ReadFile(o.claims)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var recs []claimRecord
	if err := json.Unmarshal(data, &recs); err != nil {
		return nil, err
	}
	return recs, nil
}

func (o *Owner) writeClaims(recs []claimRecord) error {
	data, err := json.Marshal(recs)
	if err != nil {
		return err
	}
	tmp := o.claims + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, o.claims)
}

// pruneExpired drops claims whose expiry has passed. The table is only
// rewritten inside an arbiter critical section, so pruning is atomic with
// respect to every other reader and writer.
func pruneExpired(recs []claimRecord) []claimRecord {
	now := time.Now()
	out := recs[:0]
	for _, r := range recs {
		if r.Expires.Before(now) {
			continue
		}
		out = append(out, r)
	}
	return out
}

// conflicts reports whether c overlaps any live claim held by another owner.
// This owner's own claims never conflict, which is what makes acquisition
// re-entrant across parallel tool calls and shared subagents.
func (o *Owner) conflicts(recs []claimRecord, c Claim) bool {
	for _, r := range recs {
		if r.ID == o.id {
			continue
		}
		other := Claim{Paths: r.Paths, WholeWorkspace: r.Whole, WorkspaceRoot: r.Root}
		if c.Overlaps(other) {
			return true
		}
	}
	return false
}

// releaseClaim returns one locally held claim to the table. Best effort: if
// the arbiter cannot be reached the stale row expires on its own after
// claimTTL, which is the same recovery a crashed session gets.
func (o *Owner) releaseClaim(c Claim) {
	o.mu.Lock()
	kept := o.held[:0]
	for _, h := range o.held {
		if !claimEqual(h, c) {
			kept = append(kept, h)
		}
	}
	o.held = kept
	o.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = o.withArbiter(ctx, func() error {
		recs, err := o.readClaims()
		if err != nil {
			return err
		}
		out := recs[:0]
		for _, r := range recs {
			if r.ID == o.id && recordMatches(r, c) {
				continue
			}
			out = append(out, r)
		}
		return o.writeClaims(out)
	})
}

func claimEqual(a, b Claim) bool {
	if a.WholeWorkspace != b.WholeWorkspace || !sameFold(a.WorkspaceRoot, b.WorkspaceRoot) {
		return false
	}
	if len(a.Paths) != len(b.Paths) {
		return false
	}
	for i := range a.Paths {
		if !sameFold(a.Paths[i], b.Paths[i]) {
			return false
		}
	}
	return true
}

func recordMatches(r claimRecord, c Claim) bool {
	return r.Whole == c.WholeWorkspace && sameFold(r.Root, c.WorkspaceRoot) && sameFoldSlice(r.Paths, c.Paths)
}

func sameFoldSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !sameFold(a[i], b[i]) {
			return false
		}
	}
	return true
}

func sameFold(a, b string) bool {
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		return strings.EqualFold(a, b)
	}
	return a == b
}

func (o *Owner) notify(w Wait) {
	if o.onWait != nil {
		o.onWait(w)
	}
}

func (o *Owner) markWaiting() {
	o.mu.Lock()
	o.waiting = true
	o.mu.Unlock()
}

// waitClock reports both ends of one contended acquisition, or neither: a wait
// that clears inside the grace never becomes a line someone has to read, and
// one that does not is always closed by the report that ends it.
type waitClock struct {
	owner   *Owner
	started time.Time
	began   bool
}

func (w *waitClock) contend() {
	if w.started.IsZero() {
		w.started = time.Now()
		w.owner.markWaiting()
	}
}

func (w *waitClock) report() {
	if w.began || w.started.IsZero() || time.Since(w.started) < waitNoticeGrace {
		return
	}
	w.began = true
	w.owner.notify(Wait{Outcome: WaitBegan, Elapsed: time.Since(w.started)})
}

func (w *waitClock) close(outcome WaitOutcome) {
	if w.began {
		w.owner.notify(Wait{Outcome: outcome, Elapsed: time.Since(w.started)})
	}
}
