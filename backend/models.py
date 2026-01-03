from __future__ import annotations

from pydantic import BaseModel


class RawEvent(BaseModel):
    event_time: str
    user_id: int
    event_name: str
    category: str
    amount: float


class Metrics(BaseModel):
    total_events: int
    unique_users: int
    total_amount: float
