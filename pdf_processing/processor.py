from database.connection import create_connection
from database.operations import insert_pdf_data, insert_date_data
import logging
from typing import List, Dict, Any
import fitz  # PyMuPDF
import spacy
import re
import subprocess
from pathlib import Path

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load the spaCy model
nlp = spacy.load("en_core_web_lg")

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
    

def process_and_store_pdfs(pdf_paths: List[str], db_path: str):
    conn = create_connection(db_path)
    if conn:
        try:
            for pdf_path in pdf_paths:
                ocr_pdf_paths = perform_ocr_if_needed([pdf_path])
                for ocr_pdf_path in ocr_pdf_paths:
                    extracted_text = ""  # Logic for extracting text
                    # Assuming text extraction logic here...
                    dates = extract_dates_from_text(extracted_text)
                    pdf_id = insert_pdf_data(conn, pdf_path, ocr_pdf_path, ocr_pdf_path != pdf_path)
                    for date in dates:
                        insert_date_data(conn, pdf_id, date['text'], date['context'], 0)  # Example usage
        except Exception as e:
            logger.error(f"Error processing PDF '{ocr_pdf_path}': {e}")
        finally:
            conn.close()
    else:
        logger.error("Error! Cannot create the database connection.")