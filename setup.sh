#!/bin/bash

# Remove any existing virtual environment
echo "Removing existing virtual environment..."
poetry env remove python

# Install dependencies using Poetry
echo "Installing dependencies..."
poetry install

# Activate the virtual environment
echo "Activating virtual environment..."
source $(poetry env info --path)/bin/activate

# Update pip
echo "Updating pip..."
pip install --upgrade pip

# Verify installed dependencies
echo "Verifying installed dependencies..."
pip list

echo "Setup complete."