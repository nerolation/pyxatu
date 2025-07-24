"""Setup configuration for PyXatu package."""

from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="pyxatu",
    version="1.9.1",
    author="PyXatu Contributors",
    author_email="",
    description="Secure and efficient Python client for querying Ethereum data from Xatu",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nerolation/pyxatu",
    project_urls={
        "Bug Tracker": "https://github.com/nerolation/pyxatu/issues",
        "Documentation": "https://github.com/nerolation/pyxatu/blob/main/README.md",
        "Source Code": "https://github.com/nerolation/pyxatu",
    },
    packages=find_packages(exclude=["tests", "tests.*", "examples", "examples.*"]),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Networking",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Typing :: Typed",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pandas>=2.0.0",
        "requests>=2.31.0",
        "aiohttp>=3.9.0",
        "aiofiles>=23.2.1",
        "pydantic>=2.5.0",
        "backoff>=2.2.1",
        "tqdm>=4.66.0",
        "fastparquet>=2024.2.0",
    ],
    extras_require={
        "cli": [
            "click>=8.1.0",
            "tabulate>=0.9.0",
            "termcolor>=2.4.0",
        ],
        "dev": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.1.0",
            "black>=23.12.0",
            "mypy>=1.8.0",
            "ruff>=0.1.0",
        ],
        "docs": [
            "sphinx>=7.2.0",
            "sphinx-rtd-theme>=2.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "xatu=pyxatu.cli:cli",
        ],
    },
    include_package_data=True,
    package_data={
        "pyxatu": ["config.json", "py.typed"],
    },
    zip_safe=False,
    keywords="ethereum blockchain xatu clickhouse beacon chain consensus layer",
)