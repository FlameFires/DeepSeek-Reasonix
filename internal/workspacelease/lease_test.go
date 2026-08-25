package workspacelease

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"reasonix/internal/testenv"
)

// withWaitGrace shortens the silence a contended acquisition keeps, so a test
// can watch both ends of a reported wait without waiting out the real one.
func withWaitGrace(t *testing.T, d time.Duration) {
	t.Helper()
	previous := waitNoticeGrace
	waitNoticeGrace = d
	t.Cleanup(func() { waitNoticeGrace = previous })
}

// withClaimTTL shortens how long a written claim survives, so a test can watch
// expiry and crash recovery without waiting out the real one.
func withClaimTTL(t *testing.T, d time.Duration) {
	t.Helper()
	previous := claimTTL
	claimTTL = d
	t.Cleanup(func() { claimTTL = previous })
}

// waitLog records what a session was told about one contended acquisition.
type waitLog struct {
	mu   sync.Mutex
	seen []Wait
}

func (l *waitLog) note(w Wait) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seen = append(l.seen, w)
}

func (l *waitLog) outcomes() []WaitOutcome {
	l.mu.Lock()
	defer l.mu.Unlock()
	got := make([]WaitOutcome, 0, len(l.seen))
	for _, w := range l.seen {
		got = append(got, w.Outcome)
	}
	return got
}

func (l *waitLog) sameAs(want ...WaitOutcome) bool {
	got := l.outcomes()
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestWorkspaceLeaseHelperProcess(t *testing.T) {
	if os.Getenv("REASONIX_WORKSPACE_LEASE_HELPER") != "1" {
		return
	}
	root := os.Getenv("REASONIX_WORKSPACE_LEASE_ROOT")
	locks := os.Getenv("REASONIX_WORKSPACE_LEASE_DIR")
	ready := os.Getenv("REASONIX_WORKSPACE_LEASE_READY")
	o, err := New(root, locks, nil)
	if err != nil {
		t.Fatal(err)
	}
	o.BeginRun()
	if err := o.AcquireWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ready, []byte("ready"), 0o600); err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Hour)
	}
}

func TestCanonicalWorkspaceResolvesSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation requires privileges on some Windows builders")
	}
	real := testenv.TempDir(t)
	link := filepath.Join(testenv.TempDir(t), "workspace-link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	got, err := CanonicalWorkspace(filepath.Join(link, "."))
	if err != nil {
		t.Fatal(err)
	}
	want, err := CanonicalWorkspace(real)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("canonical identities differ: got %q want %q", got, want)
	}
}

func TestCanonicalWorkspaceFoldsRepositorySubdirectoriesWithoutGitBinary(t *testing.T) {
	repo := testenv.TempDir(t)
	if err := os.Mkdir(filepath.Join(repo, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	subdir := filepath.Join(repo, "packages", "app")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatal(err)
	}
	rootIdentity, err := CanonicalWorkspace(repo)
	if err != nil {
		t.Fatal(err)
	}
	subdirIdentity, err := CanonicalWorkspace(subdir)
	if err != nil {
		t.Fatal(err)
	}
	if subdirIdentity != rootIdentity {
		t.Fatalf("repository subdirectory identity = %q, want root identity %q", subdirIdentity, rootIdentity)
	}
}

func TestCanonicalWorkspaceKeepsLinkedWorktreesIndependent(t *testing.T) {
	parent := testenv.TempDir(t)
	first := filepath.Join(parent, "worktree-one")
	second := filepath.Join(parent, "worktree-two")
	for _, root := range []string{first, second} {
		if err := os.MkdirAll(root, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, ".git"), []byte("gitdir: ../common\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	firstIdentity, err := CanonicalWorkspace(first)
	if err != nil {
		t.Fatal(err)
	}
	secondIdentity, err := CanonicalWorkspace(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstIdentity == secondIdentity {
		t.Fatalf("linked worktrees shared identity %q", firstIdentity)
	}
}

func TestRepositoryRootAndSubdirectoryOwnersSerialize(t *testing.T) {
	repo, locks := testenv.TempDir(t), testenv.TempDir(t)
	if err := os.Mkdir(filepath.Join(repo, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	subdir := filepath.Join(repo, "nested", "project")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatal(err)
	}
	rootOwner, err := New(repo, locks, nil)
	if err != nil {
		t.Fatal(err)
	}
	subdirOwner, err := New(subdir, locks, nil)
	if err != nil {
		t.Fatal(err)
	}
	rootOwner.BeginRun()
	subdirOwner.BeginRun()
	if err := rootOwner.AcquireWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	if err := subdirOwner.AcquireWrite(ctx); !errors.Is(err, context.DeadlineExceeded) {
		cancel()
		t.Fatalf("repository subdirectory owner acquired independently: %v", err)
	}
	cancel()
	rootOwner.EndRun()
	if err := subdirOwner.AcquireWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	subdirOwner.EndRun()
}

func TestOwnersSerializeSameWorkspaceAndReportTheWaitAsAPair(t *testing.T) {
	withWaitGrace(t, 20*time.Millisecond)
	root, locks := testenv.TempDir(t), testenv.TempDir(t)
	first, err := New(root, locks, nil)
	if err != nil {
		t.Fatal(err)
	}
	var log waitLog
	second, err := New(root, locks, log.note)
	if err != nil {
		t.Fatal(err)
	}
	first.BeginRun()
	second.BeginRun()
	if err := first.AcquireWrite(context.Background()); err != nil {
		t.Fatal(err)
	}

	acquired := make(chan error, 1)
	go func() { acquired <- second.AcquireWrite(context.Background()) }()
	select {
	case err := <-acquired:
		t.Fatalf("second owner acquired early: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	first.EndRun()
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second owner did not acquire after release")
	}
	if !log.sameAs(WaitBegan, WaitAcquired) {
		t.Fatalf("wait reports = %v, want one began closed by one acquired", log.outcomes())
	}
	second.EndRun()
}

// A wait shorter than the grace is one nobody experienced as a wait, and a
// permanent line about it outlives the condition it describes.
func TestWaitUnderTheGraceIsNeverReported(t *testing.T) {
	withWaitGrace(t, 5*time.Second)
	root, locks := testenv.TempDir(t), testenv.TempDir(t)
	first, _ := New(root, locks, nil)
	var log waitLog
	second, _ := New(root, locks, log.note)
	first.BeginRun()
	second.BeginRun()
	if err := first.AcquireWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	acquired := make(chan error, 1)
	go func() { acquired <- second.AcquireWrite(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	first.EndRun()
	if err := <-acquired; err != nil {
		t.Fatal(err)
	}
	if got := log.outcomes(); len(got) != 0 {
		t.Fatalf("wait reports = %v, want none for a wait inside the grace", got)
	}
	second.EndRun()
}

// A wait the caller gives up on is still closed: the surface that was told the
// session is waiting has to be told it no longer is.
func TestAbandonedWaitIsClosedToo(t *testing.T) {
	withWaitGrace(t, 20*time.Millisecond)
	root, locks := testenv.TempDir(t), testenv.TempDir(t)
	first, _ := New(root, locks, nil)
	var log waitLog
	second, _ := New(root, locks, log.note)
	first.BeginRun()
	second.BeginRun()
	defer first.EndRun()
	defer second.EndRun()
	if err := first.AcquireWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := second.AcquireWrite(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second owner acquired while first held: %v", err)
	}
	if !log.sameAs(WaitBegan, WaitAbandoned) {
		t.Fatalf("wait reports = %v, want one began closed by one abandoned", log.outcomes())
	}
}

func TestStateReportsWaitingAndAcquiredWithoutIdentity(t *testing.T) {
	withWaitGrace(t, 20*time.Millisecond)
	root, locks := testenv.TempDir(t), testenv.TempDir(t)
	first, err := New(root, locks, nil)
	if err != nil {
		t.Fatal(err)
	}
	waiting := make(chan struct{}, 1)
	second, err := New(root, locks, func(Wait) { waiting <- struct{}{} })
	if err != nil {
		t.Fatal(err)
	}
	first.BeginRun()
	second.BeginRun()
	if err := first.AcquireWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := first.State(); !got.Acquired || got.Waiting {
		t.Fatalf("first owner state = %+v, want acquired and not waiting", got)
	}
	acquired := make(chan error, 1)
	go func() { acquired <- second.AcquireWrite(context.Background()) }()
	select {
	case <-waiting:
	case <-time.After(2 * time.Second):
		t.Fatal("second owner did not report waiting")
	}
	if got := second.State(); got.Acquired || !got.Waiting {
		t.Fatalf("second owner state = %+v, want waiting and not acquired", got)
	}
	first.EndRun()
	if err := <-acquired; err != nil {
		t.Fatal(err)
	}
	if got := second.State(); !got.Acquired || got.Waiting {
		t.Fatalf("second owner state after acquire = %+v, want acquired and not waiting", got)
	}
	second.EndRun()
}

func TestIndependentWorkspacesDoNotBlockEachOther(t *testing.T) {
	locks := testenv.TempDir(t)
	first, err := New(testenv.TempDir(t), locks, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(testenv.TempDir(t), locks, nil)
	if err != nil {
		t.Fatal(err)
	}
	first.BeginRun()
	second.BeginRun()
	if err := first.AcquireWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if err := second.AcquireWrite(ctx); err != nil {
		t.Fatalf("independent workspace was blocked: %v", err)
	}
	first.EndRun()
	second.EndRun()
}

func TestLeaseMetadataNeverDirtiesWorkspace(t *testing.T) {
	root, locks := testenv.TempDir(t), testenv.TempDir(t)
	before, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	o, err := New(root, locks, nil)
	if err != nil {
		t.Fatal(err)
	}
	o.BeginRun()
	if err := o.AcquireWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	o.EndRun()
	after, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("workspace entries changed after lease: before=%d after=%d", len(before), len(after))
	}
}

func TestAcquireIsReentrantWithinOwner(t *testing.T) {
	o, err := New(testenv.TempDir(t), testenv.TempDir(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	o.BeginRun()
	if err := o.AcquireWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := o.AcquireWrite(ctx); err != nil {
		t.Fatalf("re-entrant acquire failed: %v", err)
	}
	o.EndRun()
}

func TestCancelledWaitDoesNotLeakLocalLease(t *testing.T) {
	root, locks := testenv.TempDir(t), testenv.TempDir(t)
	first, _ := New(root, locks, nil)
	second, _ := New(root, locks, nil)
	third, _ := New(root, locks, nil)
	first.BeginRun()
	second.BeginRun()
	third.BeginRun()
	if err := first.AcquireWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if err := second.AcquireWrite(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled acquire = %v, want deadline", err)
	}
	second.EndRun()
	first.EndRun()
	if err := third.AcquireWrite(context.Background()); err != nil {
		t.Fatalf("lease leaked after cancellation: %v", err)
	}
	third.EndRun()
}

func TestLeaseWaitsForLastRun(t *testing.T) {
	root, locks := testenv.TempDir(t), testenv.TempDir(t)
	first, _ := New(root, locks, nil)
	second, _ := New(root, locks, nil)
	first.BeginRun()
	first.BeginRun()
	second.BeginRun()
	if err := first.AcquireWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	first.EndRun()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := second.AcquireWrite(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second acquired before final run ended: %v", err)
	}
	first.EndRun()
	if err := second.AcquireWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	second.EndRun()
}

func TestCrossProcessLeaseBlocksAndCrashReleases(t *testing.T) {
	root, locks := testenv.TempDir(t), testenv.TempDir(t)
	ready := filepath.Join(testenv.TempDir(t), "ready")
	cmd := exec.Command(os.Args[0], "-test.run=^TestWorkspaceLeaseHelperProcess$")
	cmd.Env = append(os.Environ(),
		"REASONIX_WORKSPACE_LEASE_HELPER=1",
		"REASONIX_WORKSPACE_LEASE_ROOT="+root,
		"REASONIX_WORKSPACE_LEASE_DIR="+locks,
		"REASONIX_WORKSPACE_LEASE_READY="+ready,
		// The helper crashes holding the whole-workspace claim; a short claim
		// TTL lets the stale row expire quickly instead of pinning the
		// workspace for the real 30s.
		"REASONIX_WORKSPACE_LEASE_TTL=300ms",
	)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("helper process did not acquire lease")
		}
		time.Sleep(20 * time.Millisecond)
	}

	o, err := New(root, locks, nil)
	if err != nil {
		t.Fatal(err)
	}
	o.BeginRun()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	if err := o.AcquireWrite(ctx); !errors.Is(err, context.DeadlineExceeded) {
		cancel()
		t.Fatalf("cross-process acquire while helper lived = %v, want deadline", err)
	}
	cancel()

	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_, _ = cmd.Process.Wait()
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := o.AcquireWrite(ctx); err != nil {
		t.Fatalf("OS lease did not release after helper crash: %v", err)
	}
	o.EndRun()
}

// --- path-level leases (B + C) ---

func pathClaim(root string, paths ...string) Claim {
	return Claim{Paths: paths, WorkspaceRoot: root}
}

// The whole point of path leases: two sessions writing disjoint files in the
// same workspace acquire concurrently instead of serializing behind one lock.
func TestDisjointPathsAcquireConcurrently(t *testing.T) {
	root, locks := t.TempDir(), t.TempDir()
	first, _ := New(root, locks, nil)
	second, _ := New(root, locks, nil)
	first.BeginRun()
	second.BeginRun()
	defer first.EndRun()
	defer second.EndRun()

	releaseA, err := first.AcquireWritePaths(context.Background(), pathClaim(root, filepath.Join(root, "src", "a.go")))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	releaseB, err := second.AcquireWritePaths(ctx, pathClaim(root, filepath.Join(root, "src", "b.go")))
	if err != nil {
		t.Fatalf("disjoint path blocked behind another writer: %v", err)
	}
	releaseA()
	releaseB()
}

// The same path from two owners is still one writer at a time.
func TestSamePathSerializesAcrossOwners(t *testing.T) {
	root, locks := t.TempDir(), t.TempDir()
	target := filepath.Join(root, "src", "a.go")
	first, _ := New(root, locks, nil)
	second, _ := New(root, locks, nil)
	first.BeginRun()
	second.BeginRun()
	defer first.EndRun()
	defer second.EndRun()

	releaseA, err := first.AcquireWritePaths(context.Background(), pathClaim(root, target))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if _, err := second.AcquireWritePaths(ctx, pathClaim(root, target)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("same path acquired while first held it: %v", err)
	}
	releaseA()
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	releaseB, err := second.AcquireWritePaths(ctx, pathClaim(root, target))
	if err != nil {
		t.Fatalf("same path stayed blocked after release: %v", err)
	}
	releaseB()
}

// Whole-workspace claims (bash, verification) and path claims are mutually
// exclusive in both directions.
func TestWholeBlocksPathsAndPathsBlockWhole(t *testing.T) {
	root, locks := t.TempDir(), t.TempDir()
	target := filepath.Join(root, "src", "a.go")

	first, _ := New(root, locks, nil)
	second, _ := New(root, locks, nil)
	first.BeginRun()
	second.BeginRun()
	defer first.EndRun()
	defer second.EndRun()

	// Whole held by first blocks a path claim from second.
	if err := first.AcquireWrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	if _, err := second.AcquireWritePaths(ctx, pathClaim(root, target)); !errors.Is(err, context.DeadlineExceeded) {
		cancel()
		t.Fatalf("path acquired while whole workspace held: %v", err)
	}
	cancel()
	first.EndRun()
	// first.EndRun drops activeRuns to 0 and releases the whole claim.

	// Path held by second blocks a whole claim from first.
	releasePath, err := second.AcquireWritePaths(context.Background(), pathClaim(root, target))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 80*time.Millisecond)
	if err := first.AcquireWrite(ctx); !errors.Is(err, context.DeadlineExceeded) {
		cancel()
		t.Fatalf("whole workspace acquired while path held: %v", err)
	}
	cancel()
	releasePath()
}

// A parent directory claim conflicts with a child file claim.
func TestParentPathBlocksChildPath(t *testing.T) {
	root, locks := t.TempDir(), t.TempDir()
	dir := filepath.Join(root, "src")
	file := filepath.Join(dir, "a.go")
	first, _ := New(root, locks, nil)
	second, _ := New(root, locks, nil)
	first.BeginRun()
	second.BeginRun()
	defer first.EndRun()
	defer second.EndRun()

	releaseDir, err := first.AcquireWritePaths(context.Background(), pathClaim(root, dir))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	if _, err := second.AcquireWritePaths(ctx, pathClaim(root, file)); !errors.Is(err, context.DeadlineExceeded) {
		cancel()
		t.Fatalf("child path acquired while parent dir held: %v", err)
	}
	cancel()
	releaseDir()
}

// A crashed owner's stale claim stops blocking once it expires.
func TestExpiredClaimStopsBlocking(t *testing.T) {
	withClaimTTL(t, 60*time.Millisecond)
	root, locks := t.TempDir(), t.TempDir()
	target := filepath.Join(root, "src", "a.go")
	first, _ := New(root, locks, nil)
	second, _ := New(root, locks, nil)
	first.BeginRun()
	second.BeginRun()
	defer first.EndRun()
	defer second.EndRun()

	releaseA, err := first.AcquireWritePaths(context.Background(), pathClaim(root, target))
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a crash: never release, let the row expire.
	_ = releaseA
	time.Sleep(150 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	releaseB, err := second.AcquireWritePaths(ctx, pathClaim(root, target))
	if err != nil {
		t.Fatalf("expired claim kept blocking: %v", err)
	}
	releaseB()
}

// EndRun releases claims a write tool forgot to release explicitly.
func TestEndRunReleasesUnreleasedClaims(t *testing.T) {
	root, locks := t.TempDir(), t.TempDir()
	target := filepath.Join(root, "src", "a.go")
	first, _ := New(root, locks, nil)
	second, _ := New(root, locks, nil)
	first.BeginRun()
	second.BeginRun()
	defer second.EndRun()

	if _, err := first.AcquireWritePaths(context.Background(), pathClaim(root, target)); err != nil {
		t.Fatal(err)
	}
	first.EndRun()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	releaseB, err := second.AcquireWritePaths(ctx, pathClaim(root, target))
	if err != nil {
		t.Fatalf("EndRun did not release the held claim: %v", err)
	}
	releaseB()
}

// A claim is re-entrant inside one owner even across parallel acquisitions.
func TestPathsReentrantWithinOwner(t *testing.T) {
	root, locks := t.TempDir(), t.TempDir()
	target := filepath.Join(root, "src", "a.go")
	o, _ := New(root, locks, nil)
	o.BeginRun()
	defer o.EndRun()
	releaseA, err := o.AcquireWritePaths(context.Background(), pathClaim(root, target))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	releaseB, err := o.AcquireWritePaths(ctx, pathClaim(root, target))
	if err != nil {
		t.Fatalf("re-entrant path acquire failed: %v", err)
	}
	releaseA()
	releaseB()
}
