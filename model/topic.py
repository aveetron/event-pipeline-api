from typing import Dict, Optional, Any

from pydantic import BaseModel, Field


class TopicRequest(BaseModel):
    integration_id: str
    service_type: str = Field(..., description="Service type: 'qi', 'analytics', or 'export'")


class TopicResponse(BaseModel):
    message: str
    status: str


class ConsumerStatus(BaseModel):
    service: str
    status: Dict[str, str]
    messages_processed: int