from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class UploadResponse(BaseModel):
    upload_id: str
    filename: str
    content_type: str
    excerpt: str


class ReportCreateRequest(BaseModel):
    company_id: str
    calendar_quarter: str
    history_window: int = Field(default=12, ge=8, le=16)
    manual_transcript_upload_id: Optional[str] = None
    force_refresh: bool = False


class ReportResponse(BaseModel):
    report_id: str
    company_id: str
    calendar_quarter: str
    structure_dimension_used: str
    coverage_warnings: list[str]
    preview_url: str
    export_pdf_url: str
    payload: dict[str, Any]


class ReportJobResponse(BaseModel):
    job_id: str
    company_id: str
    calendar_quarter: str
    history_window: int
    status: str
    progress: float
    stage: str
    message: str
    report_id: Optional[str] = None
    error: Optional[str] = None
    preview_url: Optional[str] = None
    export_pdf_url: Optional[str] = None
