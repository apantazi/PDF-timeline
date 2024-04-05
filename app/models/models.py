from pydantic import BaseModel
from typing import Optional

class ExtractedData(BaseModel):
    pdf_id: int
    date_text: str
    context: str
    page_number: Optional[int] = None