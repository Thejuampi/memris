param(
    [string]$Path = "memris-core/target/site/jacoco/jacoco.xml",
    [int]$Top = 20
)

if (-not (Test-Path $Path)) {
    Write-Error "Jacoco XML not found at path: $Path"
    exit 2
}

[xml]$xml = Get-Content -Path $Path

$results = @()

foreach ($pkg in $xml.report.package) {
    $pkgName = $pkg.name
    foreach ($cls in $pkg.class) {
        $lineCounter = $cls.counter | Where-Object { $_.type -eq 'LINE' }
        if (-not $lineCounter) { continue }
        $missed = [int]$lineCounter.missed
        $covered = [int]$lineCounter.covered
        $total = $missed + $covered
        if ($total -eq 0) { $pct = 100.0 } else { $pct = [math]::Round((($covered / $total) * 100.0), 2) }
        $fullName = if ($pkgName -and $pkgName.Trim() -ne '') { "$pkgName.$($cls.name)" } else { $cls.name }
        $results += [pscustomobject]@{
            Class = $fullName
            Package = $pkgName
            Name = $cls.name
            Missed = $missed
            Covered = $covered
            Total = $total
            Percent = $pct
        }
    }
}

$resultsSorted = $results | Sort-Object Percent, Total

Write-Host "Bottom $Top classes by LINE coverage (ascending):" -ForegroundColor Cyan
$resultsSorted | Select-Object -First $Top | Format-Table @{Name='Pct';Expression={$_.Percent};Alignment='Right'}, @{Name='Covered/Missed';Expression={[string]::Format("{0}/{1}", $_.Covered, $_.Missed)}}, @{Name='Total';Expression={$_.Total}}, @{Name='Class';Expression={$_.Class}} -AutoSize

Write-Host ""; Write-Host "Full CSV output:" -ForegroundColor Cyan
$resultsSorted | Select-Object Class,Percent,Covered,Missed,Total | ConvertTo-Csv -NoTypeInformation
