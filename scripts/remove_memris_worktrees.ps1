<#
PowerShell script to remove non-main git worktrees for the memris repo.

Usage examples (run from PowerShell):
  # Dry-run (shows actions, does not change):
  .\scripts\remove_memris_worktrees.ps1 -Repo "G:\dev\repos\memris" -DryRun

  # Interactive removal (will prompt):
  .\scripts\remove_memris_worktrees.ps1 -Repo "G:\dev\repos\memris"

  # Force removal and delete directories without prompts:
  .\scripts\remove_memris_worktrees.ps1 -Repo "G:\dev\repos\memris" -Force

Notes:
- This script ONLY runs git commands locally; it does not push or preserve unpushed commits.
- By default it will skip the main worktree path you specify with -Repo.
- Use -DryRun to preview actions before executing.
#>
[CmdletBinding()]
param(
    [string]
    $Repo = ".",

    [switch]
    $Force,

    [switch]
    $DryRun
)

function FailIfNoGit {
    if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
        Write-Error "git is not available in PATH. Install git and try again."
        exit 2
    }
}

function Run-Git {
    param($Args)
    $cmd = "git -C `"$Repo`" $Args"
    if ($DryRun) { Write-Host "DRYRUN: $cmd"; return $null }
    & git -C "$Repo" $Args
}

FailIfNoGit

$repoFull = (Resolve-Path -Path $Repo).ProviderPath
Write-Host "Repository root: $repoFull"

# Get worktree list in porcelain format
$porcelain = & git -C "$Repo" worktree list --porcelain 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to run 'git worktree list'. Output:`n$porcelain"
    exit 3
}

# Parse porcelain blocks (blocks separated by blank line)
$blocks = -split ($porcelain -join "`n"), "`n`n"
$worktrees = @()
foreach ($b in $blocks) {
    $lines = $b -split "`n" | Where-Object { $_ -ne "" }
    if ($lines.Count -eq 0) { continue }
    $entry = [ordered]@{ Path = $null; HEAD = $null; Branch = $null }
    foreach ($l in $lines) {
        if ($l -match "^worktree (.+)$") { $entry.Path = $Matches[1].Trim() }
        if ($l -match "^HEAD (.+)$") { $entry.HEAD = $Matches[1].Trim() }
        if ($l -match "^branch (.+)$") { $entry.Branch = $Matches[1].Trim() }
    }
    if ($entry.Path) { $worktrees += $entry }
}

if ($worktrees.Count -eq 0) {
    Write-Host "No worktrees found. Exiting."; exit 0
}

Write-Host "Discovered worktrees:`n"
$index = 0
foreach ($w in $worktrees) {
    $index++
    $isMain = (Resolve-Path -Path $w.Path).ProviderPath -eq $repoFull
    $marker = $isMain ? "(main)" : "(remove)"
    Write-Host "[$index] $($w.Path) $marker -- HEAD: $($w.HEAD) Branch: $($w.Branch)"
}

# Build list of non-main worktrees to remove
$toRemove = $worktrees | Where-Object { (Resolve-Path -Path $_.Path).ProviderPath -ne $repoFull }
if ($toRemove.Count -eq 0) { Write-Host "Only main worktree present. Nothing to do."; exit 0 }

Write-Host "`nPlanned removals:`n"
foreach ($w in $toRemove) { Write-Host "- $($w.Path) -- HEAD: $($w.HEAD) Branch: $($w.Branch)" }

if (-not $Force) {
    $ok = Read-Host "Proceed to remove the listed worktrees? Type 'yes' to continue"
    if ($ok -ne 'yes') { Write-Host "Aborting."; exit 0 }
}

foreach ($w in $toRemove) {
    $p = $w.Path
    Write-Host "\nRemoving worktree reference: $p"
    if ($DryRun) { Write-Host "DRYRUN: git -C \"$Repo\" worktree remove \"$p\""; continue }
    # Attempt normal remove first
    & git -C "$Repo" worktree remove "$p"
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "git worktree remove failed for $p; attempting force remove"
        & git -C "$Repo" worktree remove --force "$p"
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Failed to remove worktree reference for $p. Skipping directory deletion."
            continue
        }
    }

    # Optionally delete directory from disk
    if ($Force) {
        Write-Host "Deleting directory: $p"
        try { Remove-Item -LiteralPath $p -Recurse -Force -ErrorAction Stop; Write-Host "Directory deleted." }
        catch { Write-Warning ("Failed to delete directory {0}: {1}" -f $p, $_) }
    } else {
        $del = Read-Host "Delete directory $p from disk? Type 'yes' to delete"
        if ($del -eq 'yes') {
            try { Remove-Item -LiteralPath $p -Recurse -Force -ErrorAction Stop; Write-Host "Directory deleted." }
            catch { Write-Warning ("Failed to delete directory {0}: {1}" -f $p, $_) }
        } else {
            Write-Host "Left directory intact: $p"
        }
    }
}

Write-Host "\nPruning leftover worktree metadata..."
if ($DryRun) { Write-Host "DRYRUN: git -C \"$Repo\" worktree prune"; exit 0 }
& git -C "$Repo" worktree prune
if ($LASTEXITCODE -ne 0) { Write-Warning "git worktree prune returned non-zero exit code." }

Write-Host "Done. Verify with:"
Write-Host "  git -C `"$Repo`" worktree list"