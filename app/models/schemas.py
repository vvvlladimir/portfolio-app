from pydantic import BaseModel
from typing import List

class TickersRequest(BaseModel):
    tickers: List[str]
    
