# app/routers/data_retrieval.py
from fastapi import APIRouter, HTTPException
from typing import List
import asyncio
from app.models.models import ExtractedData
from database.operations import sync_fetch_processed_data  # Ensure this function is implemented

router = APIRouter()

@router.get("/processed-data/", response_model=List[ExtractedData])
async def get_processed_data():
    try:
        db_path = "data/my_project_database.db"
        loop = asyncio.get_running_loop()
        # Ensure sync_fetch_processed_data is defined and correctly fetches data from the database
        data = await loop.run_in_executor(None, sync_fetch_processed_data, db_path)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
