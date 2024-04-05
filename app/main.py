from fastapi import FastAPI
from app.routers import pdf_processing, data_retrieval

app = FastAPI()
app.include_router(pdf_processing.router)
app.include_router(data_retrieval.router)
