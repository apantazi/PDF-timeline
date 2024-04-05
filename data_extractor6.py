import subprocess
import os
import logging
from pathlib import Path
import tempfile
import fitz  # PyMuPDF
import spacy
import re
from typing import List, Dict
import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """Create a database connection to the SQLite database specified by the db_file."""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print("Connection is established: Database is created in memory.")
    except Error as e:
        print(e)
    return conn

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
def main():
    database_dir = "data"
    if not os.path.exists(database_dir):
        os.makedirs(database_dir)
    database_path = os.path.join(database_dir, "my_project_database.db")

    # create a database connection
    conn = create_connection(database_path)

    # create tables
    if conn is not None:
        # create PDFs table
        create_table(conn, sql_create_pdfs_table)

        # create Dates table
        create_table(conn, sql_create_dates_table)
    else:
        print("Error! Cannot create the database connection.")

    conn.close()

if __name__ == '__main__':
    main()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load the spaCy model
nlp = spacy.load("en_core_web_sm")  # Switched to a smaller model for efficiency

# Define a regex pattern for date extraction
date_regex = re.compile(
    r"""
    (?:\b\d{1,2}\D{0,3})?\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s\d{1,2}\b(?:\D{0,3}\d{2,4})?
    | \b\d{1,4}[-/]\d{1,2}[-/]\d{1,4}\b
    """,
    re.VERBOSE,
)

def perform_ocr_if_needed(pdf_paths: List[str]) -> List[str]:
    ocr_output_paths = []
    for pdf_path in pdf_paths:
        try:
            with fitz.open(pdf_path) as doc:
                if all(page.get_text() for page in doc):
                    logger.info(f"PDF '{pdf_path}' already contains text. Skipping OCR.")
                    ocr_output_paths.append(pdf_path)
                    continue

            ocr_output_path = str(Path(pdf_path).with_suffix('') + "_ocr.pdf")
            subprocess.run(["ocrmypdf", "--skip-text", pdf_path, ocr_output_path], check=True)
            logger.info(f"OCR completed for '{pdf_path}'. Output saved to '{ocr_output_path}'.")
            ocr_output_paths.append(ocr_output_path)
        except Exception as e:
            logger.error(f"Error during OCR process for '{pdf_path}': {e}")
            ocr_output_paths.append(pdf_path)  # Use original if error
    return ocr_output_paths

def extract_dates_from_text(text: str, context_words: int = 50) -> List[Dict[str, str]]:
    dates = []
    doc = nlp(text)
    # Process NER dates
    for ent in doc.ents:
        if ent.label_ == "DATE":
            start = max(0, ent.start - context_words)
            end = min(len(doc), ent.end + context_words)
            context = ' '.join([doc[i].text for i in range(start, end)])
            dates.append({"text": ent.text, "context": context})
    
    # Process regex dates
    for match in date_regex.finditer(text):
        date_text = match.group(0)
        if not any(date["text"] == date_text for date in dates):
            # Approximate start and end positions for context_words around the regex match
            match_start_pos = match.start()
            match_end_pos = match.end()
            # Convert match positions to approximate word counts by splitting the text
            words_before_match = text[:match_start_pos].split()[-context_words:]
            words_after_match = text[match_end_pos:].split()[:context_words]
            # Reconstruct the context with the specified number of words before and after the date
            context = ' '.join(words_before_match + [date_text] + words_after_match)
            dates.append({"text": date_text, "context": context})

    return dates


def process_pdfs(pdf_paths: List[str]) -> List[Dict[str, Any]]:
    processed_data = []
    ocr_pdf_paths = perform_ocr_if_needed(pdf_paths)
    for ocr_pdf_path in ocr_pdf_paths:
        try:
            extracted_text = ""
            with fitz.open(ocr_pdf_path) as doc:
                for page in doc:
                    extracted_text += page.get_text()
            dates = extract_dates_from_text(extracted_text)
            processed_data.append({"pdf_path": ocr_pdf_path, "dates": dates})
        except Exception as e:
            logger.error(f"Error processing PDF '{ocr_pdf_path}': {e}")
    return processed_data
    
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

def process_and_store_pdfs(pdf_paths: List[str], db_path: str):
    """
    Process a list of PDF paths for OCR and date extraction, then store the results in the database.
    :param pdf_paths: List of paths to the PDF files.
    :param db_path: Path to the SQLite database file.
    """
    conn = create_connection(db_path)
    for pdf_path in pdf_paths:
        ocr_pdf_paths = perform_ocr_if_needed([pdf_path])
        for ocr_pdf_path in ocr_pdf_paths:
            try:
                extracted_text = ""
                with fitz.open(ocr_pdf_path) as doc:
                    for page in doc:
                        extracted_text += page.get_text()
                dates = extract_dates_from_text(extracted_text)
                pdf_id = insert_pdf_data(conn, pdf_path, ocr_pdf_path, ocr_pdf_path != pdf_path)
                for date in dates:
                    insert_date_data(conn, pdf_id, date['text'], date['context'], 0) # Assuming 0 as a placeholder for page number
            except Exception as e:
                logger.error(f"Error processing PDF '{ocr_pdf_path}': {e}")
    conn.close()
