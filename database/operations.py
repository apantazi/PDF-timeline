from sqlite3 import Connection
import sqlite3
from sqlite3 import Error
from typing import List, Dict, Any  # Depending on your usage
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add to database/operations.py
def fetch_processed_data(conn):
    data = []
    sql = """SELECT * FROM Dates"""  # Example, adjust as needed
    cur = conn.cursor()
    cur.execute(sql)
    columns = [col[0] for col in cur.description]
    rows = cur.fetchall()
    for row in rows:
        data.append(dict(zip(columns, row)))
    return data
    conn.close()

def sync_fetch_processed_data(db_path):
    data = []
    conn = create_connection(db_path)
    if conn is not None:
        data = fetch_processed_data(conn)
        conn.close()
    return data

def initialize_database(conn: Connection):
    if conn is not None:
        create_table(conn, sql_create_pdfs_table)
        create_table(conn, sql_create_dates_table)
    else:
        logger.info(f"Unable to establish a database connection.")


def create_table(conn, create_table_sql):
    """Create a table from the create_table_sql statement."""
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
sql_create_pdfs_table = """CREATE TABLE IF NOT EXISTS PDFs (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                original_path TEXT NOT NULL,
                                ocr_path TEXT,
                                processed BOOLEAN NOT NULL DEFAULT 0
                            );"""

sql_create_dates_table = """CREATE TABLE IF NOT EXISTS Dates (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                pdf_id INTEGER,
                                date_text TEXT NOT NULL,
                                context TEXT,
                                page_number INTEGER,
                                FOREIGN KEY (pdf_id) REFERENCES PDFs(id) ON DELETE CASCADE
                            );"""

def insert_pdf_data(conn, pdf_path, ocr_path, processed):
    """
    Insert a new PDF entry into the database.
    :param conn: Database connection object.
    :param pdf_path: The path to the original PDF file.
    :param ocr_path: The path to the OCR-processed PDF file.
    :param processed: Boolean indicating whether the PDF was OCR processed.
    :return: The id of the inserted PDF.
    """
    sql = ''' INSERT INTO PDFs(original_path, ocr_path, processed)
              VALUES(?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, (pdf_path, ocr_path, processed))
    conn.commit()
    return cur.lastrowid

def insert_date_data(conn, pdf_id, date_text, context, page_number):
    """
    Insert date-related data into the database.
    :param conn: Database connection object.
    :param pdf_id: The id of the PDF from the PDFs table.
    :param date_text: Extracted date as text.
    :param context: Contextual text around the date.
    :param page_number: The page number of the PDF where the date was found.
    """
    sql = ''' INSERT INTO Dates(pdf_id, date_text, context, page_number)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, (pdf_id, date_text, context, page_number))
    conn.commit()