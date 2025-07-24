#!/usr/bin/env python3
"""Run all PyXatu tests and generate coverage report."""

import sys
import subprocess
import os

def run_command(cmd):
    """Run a command and return success status."""
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True)
    return result.returncode == 0

def main():
    """Run all tests with coverage."""
    # Change to the directory containing this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    print("=" * 60)
    print("PyXatu Test Suite")
    print("=" * 60)
    
    # Install test dependencies if needed
    print("\n1. Installing test dependencies...")
    if not run_command("pip install -q pytest pytest-asyncio pytest-cov"):
        print("Failed to install test dependencies")
        return 1
    
    # Run security tests first (most critical)
    print("\n2. Running security tests...")
    if not run_command("pytest tests/test_security.py -v"):
        print("Security tests failed!")
        return 1
    
    # Run all tests with coverage
    print("\n3. Running all tests with coverage...")
    test_cmd = (
        "pytest tests/ -v "
        "--cov=pyxatu "
        "--cov-report=term-missing "
        "--cov-report=html "
        "-W ignore::DeprecationWarning"
    )
    
    if not run_command(test_cmd):
        print("\nSome tests failed!")
        return 1
    
    print("\n" + "=" * 60)
    print("All tests passed! ✅")
    print("Coverage report generated in htmlcov/index.html")
    print("=" * 60)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())