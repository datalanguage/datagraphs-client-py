#!/usr/bin/env bash
set -euo pipefail

VENV_DIR=".venv"

usage() {
    echo "Usage: $0 [--platform linux|macos]"
    echo ""
    echo "Recreates the Python virtual environment for the specified platform."
    echo "If --platform is omitted, auto-detects based on the current OS."
    echo ""
    echo "  linux   - for use inside a Docker container"
    echo "  macos   - for use on the macOS host filesystem"
    exit 1
}

# Parse arguments
PLATFORM=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --platform)
            PLATFORM="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Auto-detect platform if not specified
if [[ -z "$PLATFORM" ]]; then
    case "$(uname -s)" in
        Linux*)  PLATFORM="linux" ;;
        Darwin*) PLATFORM="macos" ;;
        *)
            echo "Error: unable to detect platform. Please specify --platform linux|macos"
            exit 1
            ;;
    esac
    echo "Auto-detected platform: $PLATFORM"
fi

if [[ "$PLATFORM" != "linux" && "$PLATFORM" != "macos" ]]; then
    echo "Error: platform must be 'linux' or 'macos'"
    usage
fi

# Check for uv
if ! command -v uv &>/dev/null; then
    echo "Error: 'uv' is not installed. Install it from https://docs.astral.sh/uv/"
    exit 1
fi

# Remove existing venv
if [[ -d "$VENV_DIR" ]]; then
    echo "Removing existing $VENV_DIR ..."
    rm -rf "$VENV_DIR"
fi

# Create venv and install dependencies
echo "Creating $PLATFORM virtual environment in $VENV_DIR ..."
uv venv "$VENV_DIR"
echo "Installing project with dev dependencies ..."
uv sync

echo ""
echo "Done! Run commands with:"
echo "  uv run <command>"
echo "  e.g. uv run pytest"
