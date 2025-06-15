"""
Database connection utilities for schema grading system.
"""

import pyodbc
from typing import Optional
from ..utils.config import ConfigManager
from ..utils.logger import get_logger

logger = get_logger(__name__)

# Enable connection pooling
pyodbc.pooling = True


class DatabaseConnection:
    """Database connection manager with connection pooling and error handling."""
    
    def __init__(self, server: str, user: str, password: str, database: str = "master"):
        """Initialize database connection parameters.
        
        Args:
            server: Database server address
            user: Username for database connection
            password: Password for database connection
            database: Database name (default: "master")
        """
        self.server = server
        self.user = user
        self.password = password
        self.database = database
        self._connection = None
    
    def get_connection_string(self) -> str:
        """Generate ODBC connection string.
        
        Returns:
            str: ODBC connection string
        """
        return (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password};"
            "TrustServerCertificate=yes;"
        )
    
    def connect(self, **kwargs):
        """Establish database connection.
        
        Args:
            **kwargs: Additional connection parameters
            
        Returns:
            pyodbc.Connection: Database connection object
            
        Raises:
            Exception: If connection fails
        """
        try:
            conn_str = self.get_connection_string()
            self._connection = pyodbc.connect(conn_str, **kwargs)
            logger.info(f"Connected to database: {self.server}/{self.database}")
            return self._connection
        except Exception as e:
            logger.error(f"Failed to connect to database {self.server}/{self.database}: {e}")
            raise
    
    def close(self):
        """Close database connection."""
        if self._connection:
            try:
                self._connection.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            finally:
                self._connection = None
    
    def __enter__(self):
        """Context manager entry."""
        return self.connect()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def get_connection_string(server: str, user: str, password: str, database: str = "master") -> str:
    """Generate ODBC connection string (legacy function for backward compatibility).
    
    Args:
        server: Database server address
        user: Username for database connection
        password: Password for database connection
        database: Database name (default: "master")
        
    Returns:
        str: ODBC connection string
    """
    return (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};DATABASE={database};UID={user};PWD={password};TrustServerCertificate=yes;"
    )


def open_connection(server: str, user: str, password: str, database: str = "master", **kwargs):
    """Open database connection (legacy function for backward compatibility).
    
    Args:
        server: Database server address
        user: Username for database connection
        password: Password for database connection
        database: Database name (default: "master")
        **kwargs: Additional connection parameters
        
    Returns:
        pyodbc.Connection: Database connection object
    """
    return pyodbc.connect(get_connection_string(server, user, password, database), **kwargs)


# Alias for backward compatibility
get_conn_str = get_connection_string
open_conn = open_connection
