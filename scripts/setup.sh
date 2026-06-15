#!/bin/bash
set -e

# Distributed Logging System Setup Script
# Author: Saurabh Yadav (150096724010)

echo "============================================="
echo "Setting up Distributed Logging System..."
echo "============================================="

# Move to script directory
cd "$(dirname "$0")/.."

# Create .env from .env.example if it doesn't exist
if [ ! -f .env ]; then
  echo "Creating .env configuration file from .env.example..."
  cp .env.example .env
fi

# Create virtual environment if it doesn't exist
if [ ! -d .venv ]; then
  echo "Creating Python virtual environment (.venv)..."
  python3 -m venv .venv
fi

# Upgrade pip and install requirements
echo "Installing Python dependencies..."
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

# Create necessary directories
echo "Creating application directories..."
mkdir -p logs
mkdir -p docs/screenshots

echo "============================================="
echo "Setup complete! To run the local stack:"
echo "1. Run: docker compose up -d"
echo "2. Run: ./scripts/run.sh"
echo "============================================="
