import time

from pydantic import BaseModel


class Alert(BaseModel):
    timestamp: int | None = time.time()
    description: str | None = None
    severity: int | None = 1
