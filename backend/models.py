from __future__ import annotations

from pydantic import BaseModel


class RawEvent(BaseModel):
    event_time: str
    user_id: int
    event_name: str
    category: str
    amount: float
