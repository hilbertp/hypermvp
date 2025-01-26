#!/bin/bash

# Exit script on error
set -e

# Clear any existing virtual environment
echo "Removing existing virtual environment..."
poetry env remove python3 || true

# Install dependencies
echo "Installing dependencies..."
poetry install

# Activate virtual environment
echo "Activating virtual environment..."
source $(poetry env info --path)/bin/activate

# Verify installed dependencies
echo "Installed dependencies:"
poetry show
