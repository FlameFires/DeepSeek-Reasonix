package workspacelease

import (
	"path/filepath"
	"runtime"
	"strings"
)

// Claim is the write domain one acquisition requests. A claim names either the
// whole workspace (WholeWorkspace, used by bash / MCP / verification calls,
// whose write targets cannot be judged from their arguments) or a set of
// concrete workspace paths (used by path-bound writers such as write_file and
// edit_file, whose arguments expose exact targets).
//
// Two claims conflict when their write domains overlap: whole-workspace claims
// collide with every other claim under the same root, and path claims collide
// when one path contains the other. Claims over disjoint paths never conflict,
// which is what lets two Delivery sessions write different files in the same
// workspace concurrently.
type Claim struct {
	// Paths are absolute, cleaned workspace paths this claim covers.
	Paths []string
	// WholeWorkspace claims the entire workspace root.
	WholeWorkspace bool
	// WorkspaceRoot is the absolute root used for whole-workspace claims.
	WorkspaceRoot string
}

// Empty reports whether the claim covers nothing (read-only work never
// acquires a lease, so an empty claim is a caller bug rather than a no-op).
func (c Claim) Empty() bool {
	return !c.WholeWorkspace && len(c.Paths) == 0
}

// Overlaps reports whether two claims describe overlapping write domains.
// The semantics mirror the agent package's WritePathSet.Overlaps so both
// layers disagree with no one.
func (c Claim) Overlaps(other Claim) bool {
	if c.Empty() || other.Empty() {
		return false
	}
	if c.WholeWorkspace || other.WholeWorkspace {
		// Whole-workspace claims collide with every other writer claim that
		// shares the same workspace root (or has an empty root).
		if c.WorkspaceRoot == "" || other.WorkspaceRoot == "" {
			return true
		}
		return pathWithinFold(c.WorkspaceRoot, other.WorkspaceRoot) ||
			pathWithinFold(other.WorkspaceRoot, c.WorkspaceRoot)
	}
	for _, a := range c.Paths {
		for _, b := range other.Paths {
			if pathWithinFold(a, b) || pathWithinFold(b, a) {
				return true
			}
		}
	}
	return false
}

// pathWithinFold reports whether path is inside root, folding case on
// case-insensitive filesystems so a claim written one way blocks a claim
// written another way.
func pathWithinFold(root, path string) bool {
	if root == "" || path == "" {
		return false
	}
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		root = strings.ToLower(root)
		path = strings.ToLower(path)
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}
