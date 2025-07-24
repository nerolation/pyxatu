#!/bin/bash

# Git push script for PyXatu updates

# Add all changes
git add .

# Commit with concise message
git commit -m "Improve security, performance, and code architecture"

# Push to main branch using SSH
git push origin main

echo "✅ Changes pushed to GitHub successfully!"