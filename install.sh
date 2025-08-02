#!/bin/sh
set -e

# --- Configuration ---
GITHUB_REPO="spineldb/spineldb"
BINARY_NAME="spineldb"
DEFAULT_INSTALL_DIR="/usr/local/bin" # Default for standard systems
INSTALL_DIR=""

# --- Helper Functions ---
msg() {
  echo "\033[0;32m[SpinelDB Installer]\033[0m $1"
}

err_exit() {
  echo "\033[0;31m[SpinelDB Installer ERROR]\033[0m $1" >&2
  exit 1
}

has_command() {
  command -v "$1" >/dev/null 2>&1
}

# --- Environment Detection & Setup ---
setup_environment() {
  # Detect Termux and provide specific instructions
  if [ -n "$TERMUX_VERSION" ] || [ -d "/data/data/com.termux/files/usr" ]; then
    msg "Termux environment detected."
    msg "Pre-compiled binaries are not compatible with Termux."
    msg "Please install SpinelDB by compiling from source."
    echo ""
    echo "1. Install dependencies:"
    echo "   pkg install rust git"
    echo ""
    echo "2. Clone the repository and compile:"
    echo "   git clone https://github.com/spineldb/spineldb.git"
    echo "   cd spineldb"
    echo "   cargo build --release"
    echo ""
    echo "3. The binary will be located at: ./target/release/spineldb"
    exit 0 # Exit successfully after providing instructions
  fi

  INSTALL_DIR="$DEFAULT_INSTALL_DIR"
  msg "Installation directory set to: $INSTALL_DIR"
}

# --- OS and Architecture Detection ---
get_os_arch() {
  OS_TYPE="$(uname -s)"
  ARCH_TYPE="$(uname -m)"
  PLATFORM=""

  case "$OS_TYPE" in
    Linux)
      case "$ARCH_TYPE" in
        x86_64) PLATFORM="x86_64-linux" ;;
        aarch64) PLATFORM="aarch64-linux" ;; # Assuming you have aarch64 linux builds
        *) err_exit "Unsupported Linux Architecture: $ARCH_TYPE" ;; 
      esac
      ;;
    Darwin)
      case "$ARCH_TYPE" in
        x86_64) PLATFORM="x86_64-macos" ;;
        arm64 | aarch64) PLATFORM="aarch64-macos" ;; 
        *) err_exit "Unsupported macOS Architecture: $ARCH_TYPE" ;; 
      esac
      ;;
    *)
      err_exit "Unsupported Operating System: $OS_TYPE"
      ;;
  esac
  echo "$PLATFORM"
}

# --- Main Logic ---
main() {
  setup_environment # Set INSTALL_DIR based on environment

  # Check dependencies
  if ! has_command "curl"; then
    err_exit "'curl' command not found. Please install it first."
  fi

  OS_ARCH_COMBO=$(get_os_arch)
  msg "Detected Platform: $OS_ARCH_COMBO"

  LATEST_RELEASE_API_URL="https://api.github.com/repos/${GITHUB_REPO}/releases/latest"
  msg "Fetching latest release information from $LATEST_RELEASE_API_URL..."

  TAG=$(curl -sL "$LATEST_RELEASE_API_URL" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
  if [ -z "$TAG" ]; then
      err_exit "Could not fetch the latest release tag."
  fi
  msg "Latest tag: $TAG"

  # Construct the expected asset name
  EXPECTED_ASSET_FILENAME="spineldb-${TAG}-${OS_ARCH_COMBO}.tar.gz"

  DOWNLOAD_URL=$(curl -sL "$LATEST_RELEASE_API_URL" | \
    grep "browser_download_url.*${EXPECTED_ASSET_FILENAME}" | \
    cut -d '"' -f 4 | \
    head -n 1)

  if [ -z "$DOWNLOAD_URL" ]; then
      err_exit "Could not find a download URL for asset '$EXPECTED_ASSET_FILENAME'. Please check your GitHub release assets."
  fi

  msg "Download URL: $DOWNLOAD_URL"

  TMP_DIR=$(mktemp -d)
  trap 'rm -rf "$TMP_DIR"' EXIT # Clean up temp dir on exit

  DOWNLOADED_ARCHIVE_PATH="${TMP_DIR}/${EXPECTED_ASSET_FILENAME}"

  msg "Downloading $EXPECTED_ASSET_FILENAME to $DOWNLOADED_ARCHIVE_PATH..."
  if ! curl --progress-bar -Lo "$DOWNLOADED_ARCHIVE_PATH" "$DOWNLOAD_URL"; then
    err_exit "Failed to download the archive."
  fi
  msg "Archive downloaded."

  msg "Extracting $BINARY_NAME from $DOWNLOADED_ARCHIVE_PATH..."
  # The archive contains a directory, so we use --strip-components=1
  if ! tar -xzf "$DOWNLOADED_ARCHIVE_PATH" -C "$TMP_DIR" --strip-components=1; then
      err_exit "Failed to extract '$BINARY_NAME' from the archive."
  fi

  EXTRACTED_BINARY_PATH="${TMP_DIR}/${BINARY_NAME}"

  if [ ! -f "$EXTRACTED_BINARY_PATH" ]; then
    err_exit "Binary '$BINARY_NAME' not found after extraction at $EXTRACTED_BINARY_PATH."
  fi
  msg "Binary extracted."

  # Installation
  SUDO_CMD=""
  if [ -z "$TERMUX_VERSION" ] && [ "$(id -u)" -ne 0 ] && ! [ -w "$INSTALL_DIR" ]; then
    msg "Sudo privileges are required to install to $INSTALL_DIR"
    if has_command "sudo"; then
      SUDO_CMD="sudo"
    else
      err_exit "'sudo' command not found. Please run this script as root or ensure you have write permissions to $INSTALL_DIR."
    fi
  fi

  DEST_PATH="${INSTALL_DIR}/${BINARY_NAME}"
  msg "Installing $BINARY_NAME to $DEST_PATH..."
  if ! ${SUDO_CMD} mv "$EXTRACTED_BINARY_PATH" "$DEST_PATH"; then
    err_exit "Failed to install the binary to $DEST_PATH. (Command: ${SUDO_CMD} mv \"$EXTRACTED_BINARY_PATH\" \"$DEST_PATH\")"
  fi
  ${SUDO_CMD} chmod 755 "$DEST_PATH"
  msg "Binary installed and made executable."
}

# Run the main function
main
