from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel


class TopicRequest(BaseModel):
    service: str
    date_from: str
    date_to: str
    parameters: Optional[Dict[str, Any]] = None

class TopicResponse(BaseModel):
    message: str
    topic_id: str
    status: str
    submitted_at: datetime