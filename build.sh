#!/bin/bash
set -e

echo "Running tests..."
poetry run pytest tests/integration

echo "Building package..."
poetry build

echo "Build successful!"
