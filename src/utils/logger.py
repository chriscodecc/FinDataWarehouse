import logging
from utils.paths import LOGS_DIR

"""
Logging utility module.

Responsibilities:
- Provide a standardized logger instance for project modules.
- Configure consistent logging format and log level.
"""

def get_logger(name: str) -> logging.Logger:
    """
    Create or retrieve a logger with a standard configuration.

    Args:
        name (str): The name of the logger (typically __name__ of the calling module).

    Returns:
        logging.Logger: A configured logger instance.

    Notes:
        - Ensures that multiple calls do not add duplicate handlers.
        - Sets log level to DEBUG.
        - Uses StreamHandler with a standard formatter:
          "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    """
    logger = logging.getLogger(name)

    # No double Config
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        formatter = logging.Formatter ("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter( formatter)
        logger.addHandler(handler)

        log_file = LOGS_DIR / "pipline.log"
        f_handler = logging.FileHandler(log_file)
        f_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        f_handler.setFormatter(f_format)
        logger.addHandler(f_handler)
       

    return logger