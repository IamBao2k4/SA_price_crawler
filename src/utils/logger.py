"""
Logging configuration
"""
import logging
import sys
from pathlib import Path


def setup_logger(name: str = None, level: str = 'INFO') -> logging.Logger:
    """Setup logger with consistent formatting"""

    # Use root logger configuration
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stdout,
        force=True  # Override any existing configuration
    )

    logger = logging.getLogger(name)
    return logger
