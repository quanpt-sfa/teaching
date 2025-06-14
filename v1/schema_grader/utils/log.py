"""
Logging utilities for the database schema grading system.

Provides structured logging with timestamps and different severity levels.
"""

import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional


class GradingLogger:
    """Centralized logger for the grading system."""
    
    def __init__(self, name: str = "schema_grader", log_file: Optional[str] = None):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler (optional)
        if log_file:
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_formatter = logging.Formatter(
                '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
    
    def info(self, msg: str) -> None:
        """Log info message."""
        self.logger.info(msg)
    
    def warning(self, msg: str) -> None:
        """Log warning message."""
        self.logger.warning(msg)
    
    def error(self, msg: str) -> None:
        """Log error message."""
        self.logger.error(msg)
    
    def debug(self, msg: str) -> None:
        """Log debug message."""
        self.logger.debug(msg)


# Legacy function for backward compatibility
def log(msg: str, file: Optional[str] = None) -> None:
    """Legacy logging function."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    out = f"[{timestamp}] {msg}"
    print(out)
    if file:
        with open(file, 'a', encoding='utf8') as f:
            print(out, file=f)


# Default logger instance
default_logger = GradingLogger()

def get_logger(name: str = "schema_grader"):
    """
    Get a logger instance for the specified module.
    
    Args:
        name: Logger name, typically __name__ of the calling module
        
    Returns:
        Standard logging.Logger instance
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    return logger
