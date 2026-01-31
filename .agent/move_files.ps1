
$sourceBase = "g:\dev\repos\memris\memris-core\src\main\java\io\memris\spring"
$destBase = "g:\dev\repos\memris\memris-core\src\main\java\io\memris\core"

# Create dest base if not exists
if (!(Test-Path $destBase)) { New-Item -ItemType Directory -Force -Path $destBase }

# Move subdirectories
$subdirs = @("cache", "plan", "util")
foreach ($dir in $subdirs) {
    if (Test-Path "$sourceBase\$dir") {
        Write-Host "Moving $dir..."
        Move-Item -Path "$sourceBase\$dir" -Destination "$destBase\$dir" -Force
    }
}

# Merge converter directory
if (Test-Path "$sourceBase\converter") {
    Write-Host "Merging converter..."
    Get-ChildItem "$sourceBase\converter" | Move-Item -Destination "$destBase\converter" -Force
    Remove-Item "$sourceBase\converter" -Force
}

# Move files in root of spring package
Get-ChildItem "$sourceBase" -File | Move-Item -Destination "$destBase" -Force

# Clean up empty spring directory
# Remove-Item $sourceBase -Force -Recurse # postponing clean up to be safe

# --- TEST SOURCE ---
$testSourceBase = "g:\dev\repos\memris\memris-core\src\test\java\io\memris\spring"
$testDestBase = "g:\dev\repos\memris\memris-core\src\test\java\io\memris\core"

if (!(Test-Path $testDestBase)) { New-Item -ItemType Directory -Force -Path $testDestBase }

# Move subdirectories in test
$testSubdirs = @("plan", "runtime", "scaffold")
foreach ($dir in $testSubdirs) {
    if (Test-Path "$testSourceBase\$dir") {
        Write-Host "Moving test $dir..."
        Move-Item -Path "$testSourceBase\$dir" -Destination "$testDestBase\$dir" -Force
    }
}

# Move files in root of spring test package
Get-ChildItem "$testSourceBase" -File | Move-Item -Destination "$testDestBase" -Force
