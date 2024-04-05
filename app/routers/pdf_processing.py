from fastapi import APIRouter, BackgroundTasks, File, UploadFile
from typing import List
from pdf_processing.processor import process_and_store_pdfs  # Adjust import path as needed
import os

router = APIRouter()

@router.post("/upload-pdfs/")
async def upload_pdfs(background_tasks: BackgroundTasks, files: List[UploadFile] = File(...)):
    data_dir = "data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        
    saved_paths = []
    for file in files:
        out_file_path = os.path.join(data_dir, file.filename)
        with open(out_file_path, "wb") as out_file:
            content = await file.read()  # Read file content
            out_file.write(content)  # Write to disk
        saved_paths.append(out_file_path)
    
    background_tasks.add_task(process_and_store_pdfs, saved_paths, os.path.join(data_dir, 'my_project_database.db'))
    return {"message": "PDFs are being processed in the background."}
