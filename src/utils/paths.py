from pathlib import Path

"""
Path configuration module for the project.

Responsibilities:
- Define the base project directory.
- Provide standardized paths to commonly used subdirectories.
"""


# Create the Base Directory 
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Default Subfolder
DATA_DIR = BASE_DIR / "data"
NOOTBOOKS_DIR = BASE_DIR / "nootbooks"
LOGS_DIR = BASE_DIR / "logs"
CONFIG_DIR = BASE_DIR / "config"