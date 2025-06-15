"""
Logging utilities
"""

import logging
import sys
from typing import Optional


def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """
    Get a configured logger instance
    
    Args:
        name: Logger name (usually __name__)
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        # Set default level
        log_level = getattr(logging, (level or 'INFO').upper(), logging.INFO)
        logger.setLevel(log_level)
        
        # Create console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(log_level)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(handler)
        
        # Prevent duplicate logs
        logger.propagate = False
    
    return logger


def setup_logging(config: dict) -> None:
    """
    Setup logging configuration from config dict
    
    Args:
        config: Configuration dictionary containing logging settings
    """
    level = config.get('level', 'INFO')
    format_str = config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=format_str,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
