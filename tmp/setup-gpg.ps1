# GPG Key Setup for Maven Central Publishing
# This script will:
# 1) Generate a GPG key pair (or use existing)
# 2) Upload public key to keys.openpgp.org (so Sonatype can verify signatures)
# 3) Add private key to GitHub secrets (so GitHub Actions can sign artifacts)
# 4) Add Sonatype credentials to GitHub secrets

Write-Host "=== GPG Key Setup for Maven Central ===" -ForegroundColor Cyan
Write-Host ""

# STEP 1 — enter your email
$email = Read-Host "STEP 1: Enter your GitHub primary email"

# STEP 2 — set your full name
$name = "Juan Pablo Abelardo Lescano"

# STEP 3 — find existing key or generate a new one
$fingerprintLine = gpg --list-secret-keys --with-colons $email | Select-String "^fpr" | Select-Object -First 1
if ($null -eq $fingerprintLine) {
    Write-Host ""
    Write-Host "STEP 2: No key found. Generating new GPG key..." -ForegroundColor Yellow
    Write-Host ""
    $passphrase = Read-Host "Enter passphrase for GPG key (REMEMBER THIS)" -AsSecureString
    $passphraseText = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($passphrase))
    
    @"
%echo Generating key
Key-Type: RSA
Key-Length: 4096
Name-Real: $name
Name-Email: $email
Expire-Date: 0
Passphrase: $passphraseText
%commit
%echo done
"@ | gpg --batch --gen-key
    
    # Store passphrase for later use
    $script:gpgPassphrase = $passphraseText
    
    $fingerprintLine = gpg --list-secret-keys --with-colons $email | Select-String "^fpr" | Select-Object -First 1
    Write-Host ""
    Write-Host "Key generated successfully!" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "STEP 2: Existing key found. Using it." -ForegroundColor Green
    Write-Host ""
    $passphrase = Read-Host "Enter passphrase for existing GPG key" -AsSecureString
    $script:gpgPassphrase = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($passphrase))
}

# Get fingerprint
$fingerprint = $fingerprintLine.ToString().Split(':')[9]
Write-Host "Key fingerprint: $fingerprint" -ForegroundColor Green
Write-Host ""
Write-Host "Press Enter to continue..."
Read-Host

# STEP 3 — Upload public key to keyserver
Write-Host ""
Write-Host "=== STEP 3: Upload Public Key ===" -ForegroundColor Cyan
$publicKeyFile = "$env:TEMP\gpg-public-key.asc"
gpg --armor --export $fingerprint | Out-File -FilePath $publicKeyFile -Encoding ASCII
Write-Host "Public key saved to: $publicKeyFile" -ForegroundColor Green
Write-Host ""
Write-Host "1. Open browser: https://keys.openpgp.org/upload" -ForegroundColor White
Write-Host "2. Click 'Choose File' button" -ForegroundColor White
Write-Host "3. Select file: $publicKeyFile" -ForegroundColor Yellow
Write-Host "4. Click 'Upload' button" -ForegroundColor White
Write-Host "5. Check your email ($email) and click the confirmation link" -ForegroundColor White
Write-Host ""
Write-Host "After confirming via email, press Enter to continue..."
Read-Host

# STEP 4 — Add private key to GitHub
Write-Host ""
Write-Host "=== STEP 4: Add Private Key to GitHub ===" -ForegroundColor Cyan
gpg --armor --export-secret-keys $fingerprint | Set-Clipboard
Write-Host "Private key copied to clipboard." -ForegroundColor Green
Write-Host ""
Write-Host "1. Open browser: https://github.com/Thejuampi/memris/settings/secrets/actions" -ForegroundColor White
Write-Host "2. Click 'New repository secret'" -ForegroundColor White
Write-Host "3. Name: GPG_PRIVATE_KEY" -ForegroundColor Yellow
Write-Host "4. Value: Paste from clipboard" -ForegroundColor White
Write-Host "5. Click 'Add secret'" -ForegroundColor White
Write-Host ""
Write-Host "After adding secret, press Enter to continue..."
Read-Host

# STEP 5 — Add GPG passphrase to GitHub
Write-Host ""
Write-Host "=== STEP 5: Add GPG Passphrase to GitHub ===" -ForegroundColor Cyan
Set-Clipboard -Value $script:gpgPassphrase
Write-Host "Passphrase copied to clipboard." -ForegroundColor Green
Write-Host ""
Write-Host "1. Open browser: https://github.com/Thejuampi/memris/settings/secrets/actions" -ForegroundColor White
Write-Host "2. Click 'New repository secret'" -ForegroundColor White
Write-Host "3. Name: GPG_PASSPHRASE" -ForegroundColor Yellow
Write-Host "4. Value: Paste from clipboard" -ForegroundColor White
Write-Host "5. Click 'Add secret'" -ForegroundColor White
Write-Host ""
Write-Host "After adding secret, press Enter to continue..."
Read-Host

# STEP 6 — Add Sonatype username
Write-Host ""
Write-Host "=== STEP 6: Add Sonatype Username to GitHub ===" -ForegroundColor Cyan
Write-Host "Use the username from central.sonatype.com" -ForegroundColor Yellow
Write-Host ""
Write-Host "1. Open browser: https://github.com/Thejuampi/memris/settings/secrets/actions" -ForegroundColor White
Write-Host "2. Click 'New repository secret'" -ForegroundColor White
Write-Host "3. Name: OSSRH_USERNAME" -ForegroundColor Yellow
Write-Host "4. Value: Your Sonatype username" -ForegroundColor White
Write-Host "5. Click 'Add secret'" -ForegroundColor White
Write-Host ""
Write-Host "After adding secret, press Enter to continue..."
Read-Host

# STEP 7 — Generate and add Sonatype token
Write-Host ""
Write-Host "=== STEP 7: Generate Sonatype Token ===" -ForegroundColor Cyan
Write-Host "You need to generate a user token (NOT the same as your password)" -ForegroundColor Yellow
Write-Host ""
Write-Host "1. Open browser: https://central.sonatype.com" -ForegroundColor White
Write-Host "2. Sign in with your username and password" -ForegroundColor White
Write-Host "3. Click your username (top right) > Account" -ForegroundColor White
Write-Host "4. Look for 'Generate User Token' or 'Access Token'" -ForegroundColor White
Write-Host "5. Copy the token value" -ForegroundColor White
Write-Host ""
Write-Host "After copying token, press Enter to continue..."
Read-Host

Write-Host ""
Write-Host "Now add the token to GitHub:" -ForegroundColor Cyan
Write-Host ""
Write-Host "1. Open browser: https://github.com/Thejuampi/memris/settings/secrets/actions" -ForegroundColor White
Write-Host "2. Click 'New repository secret'" -ForegroundColor White
Write-Host "3. Name: OSSRH_TOKEN" -ForegroundColor Yellow
Write-Host "4. Value: Paste the token from Sonatype" -ForegroundColor White
Write-Host "5. Click 'Add secret'" -ForegroundColor White
Write-Host ""
Write-Host "After adding secret, press Enter to continue..."
Read-Host

# Done
Write-Host ""
Write-Host "=== Setup Complete! ===" -ForegroundColor Green
Write-Host ""
Write-Host "You should now have these GitHub secrets configured:" -ForegroundColor Cyan
Write-Host "  - GPG_PRIVATE_KEY" -ForegroundColor White
Write-Host "  - GPG_PASSPHRASE" -ForegroundColor White
Write-Host "  - OSSRH_USERNAME" -ForegroundColor White
Write-Host "  - OSSRH_TOKEN" -ForegroundColor White
Write-Host ""
Write-Host "Next step: Trigger a release by pushing to main branch." -ForegroundColor Yellow
Write-Host ""
