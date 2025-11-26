# This script adds the necessary MinGW/MSYS2 paths to the user's PATH environment variable 
# to enable building C/C++ dependencies with Cargo on Windows.

# Default installation directory for MSYS2. If your installation is different, update this variable.
$msys2Root = "C:\msys64"

# The required paths for the build toolchain (GCC compiler and MSYS utilities like 'sh.exe').
$requiredPaths = @(
    "$msys2Root\mingw64\bin",
    "$msys2Root\usr\bin"
)

# --- Permanent User PATH Update ---

# Get the current User PATH and split it into an array for reliable checking.
try {
    $userPath = [System.Environment]::GetEnvironmentVariable("Path", "User")
    $userPathArray = $userPath.Split(';') | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
} catch {
    $userPath = ""
    $userPathArray = @()
}


# Find which paths are missing from the User PATH.
$missingPaths = $requiredPaths | Where-Object { $userPathArray -notcontains $_ }

if ($missingPaths.Count -gt 0) {
    Write-Host "The following required paths are missing from your User PATH:"
    $missingPaths | ForEach-Object { Write-Host "- $_" }

    # Append only the missing paths.
    $newPath = ($userPathArray + $missingPaths) -join ';'
    [System.Environment]::SetEnvironmentVariable("Path", $newPath, "User")
    
    Write-Host "`nSuccessfully added missing paths to your permanent User PATH.`n"
} else {
    Write-Host "All required MSYS2 paths are already in your permanent User PATH."
}

# --- Current Session PATH Update ---

# Get the current session's PATH array.
$sessionPathArray = $env:PATH.Split(';') | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }

# Find which paths are missing from the current session's PATH.
$missingSessionPaths = $requiredPaths | Where-Object { $sessionPathArray -notcontains $_ }

if ($missingSessionPaths.Count -gt 0) {
    # Prepend missing paths to the current session's PATH.
    $newSessionPath = ($missingSessionPaths + $sessionPathArray) -join ';'
    $env:PATH = $newSessionPath
    Write-Host "Updated the PATH for the current terminal session. You can now run build commands."
} else {
    Write-Host "All required MSYS2 paths are already in the current session's PATH."
}

Write-Host "`nTo apply the permanent changes, please restart any other open terminals.`n"