import yaml
from utils.paths import BASE_DIR, CONFIG_DIR

"""
Configuration utility module.

Responsibilities:
- Provide helpers to load YAML configuration files from the config directory.
"""

def yaml_read(filename: str):
    """
    Read a YAML configuration file from the config directory.

    Args:
        filename (str): Name of the YAML file located in CONFIG_DIR.

    Returns:
        dict: Parsed YAML content as a Python dictionary.

    Notes:
        - Files are read with UTF-8 encoding.
        - Uses PyYAML safe_load to prevent execution of arbitrary code.
    """
    with open(CONFIG_DIR / filename, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)