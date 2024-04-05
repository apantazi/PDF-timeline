import sqlite3
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_connection(db_file):
    """Create a database connection to the SQLite database specified by db_file."""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        logger.info(f"Connection to {db_file} is established.")
    except sqlite3.Error as e:
        logger.error(f"Failed to create a database connection to {db_file}: {e}")
    return conn