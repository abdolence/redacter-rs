# Installs the redacter command line tool from its GitHub release.
#
#   irm https://raw.githubusercontent.com/abdolence/redacter-rs/master/install.ps1 | iex
#
# REDACTER_INSTALL_DIR  target directory, default %LOCALAPPDATA%\Programs\redacter
# REDACTER_VERSION      release tag such as v0.21.0, default is the latest release
#
# See the "Installation" section of the README for details.
#
# The body runs in a script block so that `irm | iex` leaves no functions or variables
# behind in the caller's session.

& {
    $ErrorActionPreference = 'Stop'
    # Progress rendering makes Invoke-WebRequest very slow in Windows PowerShell 5.1.
    $ProgressPreference = 'SilentlyContinue'
    # Windows PowerShell 5.1 does not enable TLS 1.2 by default, and GitHub requires it.
    [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

    $repo = 'abdolence/redacter-rs'
    $apiUrl = "https://api.github.com/repos/$repo/releases"
    $downloadUrl = "https://github.com/$repo/releases/download"

    # Returns the release archive name for a PROCESSOR_ARCHITECTURE value.
    function Get-AssetName([string] $Architecture) {
        switch ($Architecture) {
            'AMD64' { 'redacter-Windows-x86_64.zip' }
            'ARM64' { 'redacter-Windows-aarch64.zip' }
            'x86' { 'redacter-Windows-i686.zip' }
            default { throw "Unsupported processor architecture '$Architecture'. Supported: AMD64, ARM64, x86." }
        }
    }

    # A 32-bit PowerShell on 64-bit Windows reports x86 in PROCESSOR_ARCHITECTURE and
    # the real architecture in PROCESSOR_ARCHITEW6432.
    $architecture = if ($env:PROCESSOR_ARCHITEW6432) { $env:PROCESSOR_ARCHITEW6432 } else { $env:PROCESSOR_ARCHITECTURE }
    $asset = Get-AssetName $architecture

    $installDir = $env:REDACTER_INSTALL_DIR
    if (-not $installDir) {
        if (-not $env:LOCALAPPDATA) { throw 'LOCALAPPDATA is not set, set REDACTER_INSTALL_DIR.' }
        $installDir = Join-Path $env:LOCALAPPDATA 'Programs\redacter'
    }
    $installDir = $installDir.TrimEnd('\', '/')

    $version = $env:REDACTER_VERSION
    if (-not $version) {
        $releaseUrl = "$apiUrl/latest"
    }
    else {
        if ($version -notmatch '^[A-Za-z0-9._+-]+$') { throw "Invalid REDACTER_VERSION '$version'." }
        if ($version -match '^[0-9]') { $version = "v$version" }
        $releaseUrl = "$apiUrl/tags/$version"
    }

    $tempDir = Join-Path ([IO.Path]::GetTempPath()) ("redacter-install-" + [Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $tempDir | Out-Null
    try {
        try {
            $release = Invoke-RestMethod -Uri $releaseUrl -Headers @{ Accept = 'application/vnd.github+json' }
        }
        catch {
            throw "Cannot fetch release information from $releaseUrl. Check REDACTER_VERSION, and that the GitHub API rate limit is not exhausted. $($_.Exception.Message)"
        }
        $tag = [string] $release.tag_name
        if ($tag -notmatch '^v[0-9]') { throw "$releaseUrl did not return a redacter release." }

        $entry = @($release.assets | Where-Object { $_.name -eq $asset }) | Select-Object -First 1
        $expected = if ($entry) { [string] $entry.digest } else { '' }
        if ($expected -notmatch '^sha256:([0-9a-f]{64})$') {
            throw "Release $tag has no SHA-256 digest for $asset, refusing to install it."
        }
        $expected = $Matches[1]

        Write-Host "Installing redacter $tag ($asset) to $installDir"
        $archive = Join-Path $tempDir $asset
        Invoke-WebRequest -Uri "$downloadUrl/$tag/$asset" -OutFile $archive -UseBasicParsing

        $actual = (Get-FileHash -Algorithm SHA256 -Path $archive).Hash.ToLowerInvariant()
        if ($actual -ne $expected) {
            throw "SHA-256 mismatch for ${asset}: expected $expected, got $actual. Nothing was installed."
        }

        $extractDir = Join-Path $tempDir 'extract'
        Expand-Archive -Path $archive -DestinationPath $extractDir
        $binary = Join-Path $extractDir 'redacter.exe'
        if (-not (Test-Path -LiteralPath $binary)) { throw "$asset does not contain redacter.exe." }

        New-Item -ItemType Directory -Path $installDir -Force | Out-Null
        $target = Join-Path $installDir 'redacter.exe'
        try {
            Copy-Item -LiteralPath $binary -Destination $target -Force
        }
        catch {
            throw "Cannot replace $target, is redacter still running? $($_.Exception.Message)"
        }
    }
    finally {
        Remove-Item -LiteralPath $tempDir -Recurse -Force -ErrorAction SilentlyContinue
    }

    Write-Host "Installed $target"
    $installed = try { & $target --version 2>$null | Select-Object -First 1 } catch { $null }
    Write-Host "Version: $(if ($installed) { $installed } else { $tag })"

    $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
    $entries = @($userPath -split ';' | Where-Object { $_ })
    $normalised = $installDir.TrimEnd('\')
    if (-not ($entries | Where-Object { $_.TrimEnd('\') -ieq $normalised })) {
        $newPath = (@($entries) + $installDir) -join ';'
        [Environment]::SetEnvironmentVariable('Path', $newPath, 'User')
        $env:Path = "$env:Path;$installDir"
        Write-Host "Added $installDir to your user PATH. Open a new terminal for other windows to pick it up."
    }
}
