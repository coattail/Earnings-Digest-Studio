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


class SkillReportCreateRequest(BaseModel):
    company: str
    quarter: str
    history_window: int = Field(default=12, ge=8, le=16)
    manual_transcript_upload_id: Optional[str] = None
    force_refresh: bool = False


class SkillDiagnostic(BaseModel):
    code: str
    stage: str
    severity: str
    message: str
    recovery_hint: Optional[str] = None
    suggestions: list[str] = Field(default_factory=list)


class ReportResponse(BaseModel):
    report_id: str
    company_id: str
    calendar_quarter: str
    structure_dimension_used: str
    coverage_warnings: list[str]
    preview_url: str
    export_pdf_url: str
    payload: dict[str, Any]


class SkillReportResponse(BaseModel):
    report_id: str
    company_id: str
    company_name: str
    english_name: str
    ticker: str
    calendar_quarter: str
    structure_dimension_used: str
    coverage_warnings: list[str]
    preview_url: str
    export_pdf_url: str
    pdf_download_url: Optional[str] = None
    pdf_error: Optional[str] = None
    diagnostics: list[SkillDiagnostic] = Field(default_factory=list)
    payload: dict[str, Any]


class SkillReportJobResponse(BaseModel):
    job_id: str
    company_id: str
    company_name: str
    english_name: str
    ticker: str
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
    pdf_download_url: Optional[str] = None
    pdf_error: Optional[str] = None
    diagnostics: list[SkillDiagnostic] = Field(default_factory=list)


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
