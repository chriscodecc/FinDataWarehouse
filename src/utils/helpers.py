import re
import yaml
from utils.paths import BASE_DIR, CONFIG_DIR

def normalize_symbol(self, symbole: str) -> str:
        """
        Normalize a ticker symbol by removing whitespace.

        Args:
            symbole (str): The ticker symbol.

        Returns:
            str: The normalized symbol. Returns an empty string if None.
        """
        if symbole is None:
            return ""
        return re.sub(r"\s+", "", symbole)

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

