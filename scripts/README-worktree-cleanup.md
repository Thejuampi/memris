Worktree cleanup script

This repository includes `scripts/remove_memris_worktrees.ps1` — a PowerShell helper to remove non-main git worktrees.

Default behavior:
- Discovers all worktrees for the repository you specify with `-Repo` (default `.`).
- Preserves the main worktree and removes all other worktrees.
- Prompts for confirmation unless you pass `-Force`.
- Offers a dry-run mode with `-DryRun` to preview actions.

Example usage (from PowerShell):

```powershell
# Preview actions only
.
\scripts\remove_memris_worktrees.ps1 -Repo "G:\dev\repos\memris" -DryRun

# Interactive run (prompts before removing and deleting directories)
.
\scripts\remove_memris_worktrees.ps1 -Repo "G:\dev\repos\memris"

# Force removal and delete directories without prompts
.
\scripts\remove_memris_worktrees.ps1 -Repo "G:\dev\repos\memris" -Force
```

Important safety notes:
- This script does NOT push or preserve unpushed or detached commits. If you want to preserve commits before removing a worktree, create a branch at the detached HEAD and push it to origin manually.
- Always run with `-DryRun` first if you are uncertain.
