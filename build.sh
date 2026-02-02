#!/usr/bin/env bash
# Build script for Render - installs Python deps + Playwright Chromium with cache handling
set -e

pip install -r requirements.txt

# Playwright Chromium: use PLAYWRIGHT_BROWSERS_PATH for persistence on Render
export PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_BROWSERS_PATH:-/opt/render/project/playwright}"

# Restore from build cache if available (speeds up rebuilds)
if [[ ! -d "$PLAYWRIGHT_BROWSERS_PATH" ]] && [[ -d "${XDG_CACHE_HOME:-$HOME/.cache}/ms-playwright" ]]; then
    echo "Restoring Playwright Chromium from build cache..."
    mkdir -p "$(dirname "$PLAYWRIGHT_BROWSERS_PATH")"
    cp -R "${XDG_CACHE_HOME:-$HOME/.cache}/ms-playwright" "$PLAYWRIGHT_BROWSERS_PATH" 2>/dev/null || true
fi

# Install Chromium if not present
if [[ ! -d "$PLAYWRIGHT_BROWSERS_PATH" ]]; then
    echo "Installing Playwright Chromium..."
    playwright install chromium
fi

# Store in cache for next build
if [[ -d "$PLAYWRIGHT_BROWSERS_PATH" ]] && [[ -n "${XDG_CACHE_HOME}" ]]; then
    echo "Storing Playwright cache for future builds..."
    mkdir -p "$XDG_CACHE_HOME"
    cp -R "$PLAYWRIGHT_BROWSERS_PATH" "$XDG_CACHE_HOME/" 2>/dev/null || true
fi
