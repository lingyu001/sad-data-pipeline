from pydantic import BaseModel, Field
from typing import Optional
import datetime

class StockPrice(BaseModel):
    symbol: str = Field(..., description="Stock ticker symbol")
    date: datetime.date = Field(..., description="Trading date")
    open: float = Field(..., ge=0)
    high: float = Field(..., ge=0)
    low: float = Field(..., ge=0)
    close: float = Field(..., ge=0)
    adj_close: float = Field(..., ge=0)
    volume: Optional[int] = Field(None, ge=0) 