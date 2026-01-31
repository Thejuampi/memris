
$files = Get-ChildItem "g:\dev\repos\memris\memris-core\src" -Recurse -Include *.java
foreach ($file in $files) {
    $content = Get-Content $file.FullName -Raw
    $newContent = $content -replace 'package io.memris.spring', 'package io.memris.core'
    $newContent = $newContent -replace 'import io.memris.spring', 'import io.memris.core'
    
    # Also replace fully qualified names potentially used in code
    $newContent = $newContent -replace 'io.memris.spring\.', 'io.memris.core.'

    if ($content -ne $newContent) {
        Set-Content $file.FullName $newContent -NoNewline
        Write-Host "Updated $($file.Name)"
    }
}
