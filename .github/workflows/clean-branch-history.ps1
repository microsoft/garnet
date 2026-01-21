<#
.SYNOPSIS
    Cleans the git history for a specified branch by creating an temp orphan branch.

.DESCRIPTION
    This script checks out the specified branch, creates an orphan branch with no history,
    and force-pushes to overwrite the remote branch. This results in a clean, trimmed-down
    version of the branch with no commit history.

.PARAMETER BranchName
    The name of the branch to clean. Cannot be 'main' or 'master'.

.EXAMPLE
    .\clean-branch-history.ps1 -BranchName "continuousbenchmark"
#>

param(
    [Parameter(Mandatory = $true)]
    [string]$BranchName
)

# Set strict mode and stop on errors
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host "`n=== $Message ===" -ForegroundColor Cyan
}

function Write-Success {
    param([string]$Message)
    Write-Host "[SUCCESS] $Message" -ForegroundColor Green
}

function Write-ErrorMessage {
    param([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

function Exit-WithError {
    param([string]$Message, [int]$ExitCode = 1)
    Write-ErrorMessage $Message
    exit $ExitCode
}

# =============================================================================
# Safety Check: Prevent running on main or master branch
# =============================================================================
Write-Step "Validating branch name"

$protectedBranches = @("main", "master")
if ($BranchName -in $protectedBranches) {
    Exit-WithError "This script cannot be run on protected branches (main/master). Aborting for safety."
}
Write-Success "Branch name '$BranchName' is not a protected branch."

# =============================================================================
# Check: Verify we are in a git repository
# =============================================================================
Write-Step "Verifying git repository"

try {
    $gitRoot = git rev-parse --show-toplevel 2>&1
    if ($LASTEXITCODE -ne 0) {
        Exit-WithError "Not in a git repository. Please run this script from within a git repository."
    }
    Write-Success "Git repository found at: $gitRoot"
}
catch {
    Exit-WithError "Failed to verify git repository: $_"
}

# =============================================================================
# Check: Verify the target branch exists (locally or remotely)
# =============================================================================
Write-Step "Checking if branch '$BranchName' exists"

# Check local and remote branches 
$localBranch = git branch --list $BranchName 2>&1
$remoteBranch = git ls-remote --heads origin $BranchName 2>&1

if ($LASTEXITCODE -ne 0) {
    Exit-WithError "Failed to check remote branches. Ensure you have network connectivity and proper permissions."
}

$branchExists = $false
if ($localBranch -match $BranchName) {
    Write-Success "Branch '$BranchName' exists locally."
    $branchExists = $true
}
if ($remoteBranch -match $BranchName) {
    Write-Success "Branch '$BranchName' exists on remote."
    $branchExists = $true
}

if (-not $branchExists) {
    Exit-WithError "Branch '$BranchName' does not exist locally or on remote. Please verify the branch name."
}

# =============================================================================
# Safety Check: Ensure we are not currently on main/master
# =============================================================================
Write-Step "Checking current branch"

$currentBranch = git branch --show-current 2>&1
if ($LASTEXITCODE -ne 0) {
    Exit-WithError "Failed to determine current branch."
}

Write-Host "Current branch: $currentBranch"

# =============================================================================
# Step 1: Check out the target branch
# =============================================================================
Write-Step "Step 1: Checking out branch '$BranchName'"

try {
    git checkout $BranchName 2>&1
    if ($LASTEXITCODE -ne 0) {
        Exit-WithError "Failed to checkout branch '$BranchName'."
    }
    Write-Success "Successfully checked out branch '$BranchName'."
}
catch {
    Exit-WithError "Failed to checkout branch '$BranchName': $_"
}

# Double-check we're on the correct branch and NOT on main/master
$currentBranch = git branch --show-current 2>&1
if ($currentBranch -in $protectedBranches) {
    Exit-WithError "Safety check failed: Currently on protected branch '$currentBranch'. Aborting."
}

# =============================================================================
# Step 2: Create a new orphan branch (no history)
# =============================================================================
Write-Step "Step 2: Creating orphan branch 'temp_branch'"

try {
    # Ensure temp_branch does not already exist
    $existingTemp = git branch --list temp_branch
    if ($existingTemp -match "temp_branch") {
        Write-Step "Deleting existing temp_branch"
        git branch -D temp_branch
    }

# DEBUG    git checkout --orphan temp_branch 2>&1
    if ($LASTEXITCODE -ne 0) {
        Exit-WithError "Failed to create orphan branch."
    }
    Write-Success "Successfully created orphan branch 'temp_branch'."
}
catch {
    Exit-WithError "Failed to create orphan branch: $_"
}

# =============================================================================
# Step 3: Add all files to the new branch
# =============================================================================
Write-Step "Step 3: Adding all files to staging"

try {
# DEBUG     git add -A 2>&1
    if ($LASTEXITCODE -ne 0) {
        Exit-WithError "Failed to add files to staging."
    }
    Write-Success "Successfully added all files to staging."
}
catch {
    Exit-WithError "Failed to add files: $_"
}

# =============================================================================
# Step 4: Commit the files
# =============================================================================
Write-Step "Step 4: Committing files"

try {
    $commitMessage = "Clean history for $BranchName"
# DEBUG     git commit -am $commitMessage 2>&1
    if ($LASTEXITCODE -ne 0) {
        Exit-WithError "Failed to commit files."
    }
    Write-Success "Successfully committed with message: '$commitMessage'."
}
catch {
    Exit-WithError "Failed to commit files: $_"
}

# =============================================================================
# Step 5: Delete the old branch locally
# =============================================================================
Write-Step "Step 5: Deleting old local branch '$BranchName'"

try {
# DEBUG git branch -D $BranchName 2>&1
    if ($LASTEXITCODE -ne 0) {
        Exit-WithError "Failed to delete old local branch '$BranchName'."
    }
    Write-Success "Successfully deleted old local branch '$BranchName'."
}
catch {
    Exit-WithError "Failed to delete old branch: $_"
}

# =============================================================================
# Step 6: Rename the new branch to target branch name
# =============================================================================
Write-Step "Step 6: Renaming 'temp_branch' to '$BranchName'"

try {
# DEBUG    git branch -m $BranchName 2>&1
    if ($LASTEXITCODE -ne 0) {
        Exit-WithError "Failed to rename branch to '$BranchName'."
    }
    Write-Success "Successfully renamed branch to '$BranchName'."
}
catch {
    Exit-WithError "Failed to rename branch: $_"
}

# =============================================================================
# Step 7: Force-push to overwrite the remote branch
# =============================================================================
Write-Step "Step 7: Force-pushing to remote"

try {
# DEBUG    git push -f origin $BranchName 2>&1
    if ($LASTEXITCODE -ne 0) {
        Exit-WithError "Failed to force-push to remote. Check your permissions and network connectivity."
    }
    Write-Success "Successfully force-pushed '$BranchName' to remote."
}
catch {
    Exit-WithError "Failed to force-push: $_"
}

# =============================================================================
# Complete
# =============================================================================
Write-Host "`n" -NoNewline
Write-Host "============================================" -ForegroundColor Green
Write-Host " Branch history cleaned successfully!" -ForegroundColor Green
Write-Host " Branch: $BranchName" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
