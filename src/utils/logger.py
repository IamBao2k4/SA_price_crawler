"""
Logging configuration
"""
import logging
import sys
from pathlib import Path
from loguru import logger as loguru_logger


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


def setup_loguru_file_logging(log_dir: str = "logs", level: str = "DEBUG"):
    """
    Setup loguru to write logs to file for debugging NATS publishing

    Args:
        log_dir: Directory to store log files
        level: Minimum log level to capture
    """
    # Create logs directory
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)

    # Remove default handler
    loguru_logger.remove()

    # Add console handler
    loguru_logger.add(
        sys.stderr,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
        level=level,
        colorize=True
    )

    # Add file handler for all logs
    loguru_logger.add(
        log_path / "nats_publisher.log",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
        level=level,
        rotation="10 MB",
        retention="7 days",
        compression="gz"
    )

    # Add separate file for NATS messages only (DEBUG level)
    loguru_logger.add(
        log_path / "nats_messages.log",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {message}",
        level="DEBUG",
        filter=lambda record: "NATS" in record["message"] or "candles." in record["message"],
        rotation="50 MB",
        retention="3 days"
    )

    loguru_logger.info(f"Loguru file logging initialized. Logs saved to: {log_path.absolute()}")
