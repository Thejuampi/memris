$files = Get-ChildItem 'g:\dev\repos\memris\memris-core\src\test\java\io\memris\spring\plan\entities\*.java'
foreach ($file in $files) {
    $content = Get-Content $file.FullName -Raw
    $newContent = $content -replace 'package io.memris.query.entities;', 'package io.memris.spring.plan.entities;'
    if ($content -ne $newContent) {
        Set-Content $file.FullName $newContent -NoNewline
        Write-Host "Updated $($file.Name)"
    }
}
