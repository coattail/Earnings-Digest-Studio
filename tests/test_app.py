from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

import app.services.official_materials as official_materials
import app.services.reports as reports_service
import app.services.official_source_resolver as source_resolver
from scripts import audit_dynamic_reports

from app.config import BUNDLED_NVIDIA_SEGMENT_HISTORY_PATH, BUNDLED_TECH_ANALYSIS_DATA_PATH, DATA_DIR
from app.db import init_db
from app.main import app
from app.services.charts import render_income_statement_svg, render_statement_translation_svg, render_structure_transition_svg
import app.services.local_data as local_data_service
from app.services.local_data import get_company, get_quarter_fixture
from app.services.local_data import _build_companyfacts_series
from app.services.local_data import get_supported_quarters
from app.services.local_data import normalize_calendar_quarter_input, resolve_company_reference
from app.services.institutional_views import get_institutional_views
from app.services.official_parsers import parse_official_materials
from app.services.official_parsers import _apple_legacy_geographies
from app.services.official_parsers import _extract_company_geographies
from app.services.official_parsers import _extract_company_segments
from app.services.official_parsers import _extract_generic_guidance
from app.services.official_parsers import _extract_quote_cards
from app.services.official_parsers import _extract_table_metric
from app.services.official_parsers import _flatten_text
from app.services.official_parsers import _merge_parsed_payload
from app.services.official_parsers import _prefer_richer_geographies
from app.services.official_parsers import _sanitize_temporal_narrative_facts
from app.services.official_source_resolver import resolve_official_sources
from app.services.official_source_resolver import _discover_attachment_url
from app.services.official_source_resolver import _discover_default_sitemap_urls, _discover_sitemap_sources
from app.services.official_source_resolver import _expand_related_ir_sources
from app.services.official_source_resolver import _ir_role_keywords_match, _ir_temporal_alignment
from app.services.official_source_resolver import _quarter_reference_terms
from app.services.official_source_resolver import _sec_cik_for_calendar_quarter
from app.services.pdf_export import _build_raster_pdf_html, _pdf_export_mode
from app.services.report_quality import evaluate_report_payload
from app.services.reports import (
    REPORT_PAYLOAD_SCHEMA_VERSION,
    _automatic_transcript_summary,
    _official_material_proxy_summary,
    _source_material_warnings,
    _backfill_historical_core_metrics,
    _backfill_historical_segment_history,
    _build_income_statement_snapshot,
    _ensure_minimum_qna_topics,
    _enrich_history_with_official_structures,
    _harmonize_historical_structures,
    _segments_are_geography_like,
    _quarter_fallback_for_structure,
    _sanitize_history_quality_metrics,
    _recompute_history_derivatives,
    _report_cache_is_fresh,
    _normalize_segment_items,
    build_historical_quarter_cube,
    build_report_payload,
    company_quarters,
    create_report,
    resolve_calendar_quarter_from_months,
    resolve_structure_dimension,
)


def _stub_remote_payload(currency_code: str = "USD") -> dict[str, object]:
    periods = [
        "2023Q1",
        "2023Q2",
        "2023Q3",
        "2023Q4",
        "2024Q1",
        "2024Q2",
        "2024Q3",
        "2024Q4",
        "2025Q1",
        "2025Q2",
        "2025Q3",
        "2025Q4",
    ]
    quarter_end_map = {
        "2023Q1": "2023-03-31",
        "2023Q2": "2023-06-30",
        "2023Q3": "2023-09-30",
        "2023Q4": "2023-12-31",
        "2024Q1": "2024-03-31",
        "2024Q2": "2024-06-30",
        "2024Q3": "2024-09-30",
        "2024Q4": "2024-12-31",
        "2025Q1": "2025-03-31",
        "2025Q2": "2025-06-30",
        "2025Q3": "2025-09-30",
        "2025Q4": "2025-12-31",
    }
    revenue = {}
    earnings = {}
    gross_margin = {}
    revenue_growth = {}
    period_meta = {}
    for index, period in enumerate(periods):
        revenue_value = 20_000_000_000 + index * 1_400_000_000
        earnings_value = 2_200_000_000 + index * 240_000_000
        revenue[period] = revenue_value
        earnings[period] = earnings_value
        gross_margin[period] = 39.0 + index * 0.5
        period_meta[period] = {"date_key": quarter_end_map[period]}
        if index >= 4:
            previous_period = periods[index - 4]
            revenue_growth[period] = ((revenue_value - revenue[previous_period]) / revenue[previous_period]) * 100
    return {
        "periods": periods,
        "series": {
            "revenue": revenue,
            "earnings": earnings,
            "grossMargin": gross_margin,
            "revenueGrowth": revenue_growth,
            "roe": {},
            "periodMeta": period_meta,
            "currency_code": currency_code,
        },
    }


def _stub_companyfacts_with_equity() -> dict[str, object]:
    def duration(start: str, end: str, val: int, filed: str, frame: str) -> dict[str, object]:
        return {"start": start, "end": end, "val": val, "form": "10-Q", "filed": filed, "frame": frame}

    def instant(end: str, val: int, filed: str, frame: str) -> dict[str, object]:
        return {"end": end, "val": val, "form": "10-Q", "filed": filed, "frame": frame}

    return {
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            duration("2023-01-01", "2023-03-31", 10_000_000_000, "2023-05-01", "CY2023Q1"),
                            duration("2023-04-01", "2023-06-30", 11_000_000_000, "2023-08-01", "CY2023Q2"),
                            duration("2023-07-01", "2023-09-30", 12_000_000_000, "2023-11-01", "CY2023Q3"),
                            duration("2023-10-01", "2023-12-31", 13_000_000_000, "2024-02-01", "CY2023Q4"),
                            duration("2024-01-01", "2024-03-31", 14_000_000_000, "2024-05-01", "CY2024Q1"),
                        ]
                    }
                },
                "NetIncomeLoss": {
                    "units": {
                        "USD": [
                            duration("2023-01-01", "2023-03-31", 1_000_000_000, "2023-05-01", "CY2023Q1"),
                            duration("2023-04-01", "2023-06-30", 1_100_000_000, "2023-08-01", "CY2023Q2"),
                            duration("2023-07-01", "2023-09-30", 1_200_000_000, "2023-11-01", "CY2023Q3"),
                            duration("2023-10-01", "2023-12-31", 1_300_000_000, "2024-02-01", "CY2023Q4"),
                            duration("2024-01-01", "2024-03-31", 1_400_000_000, "2024-05-01", "CY2024Q1"),
                        ]
                    }
                },
                "GrossProfit": {
                    "units": {
                        "USD": [
                            duration("2023-01-01", "2023-03-31", 4_000_000_000, "2023-05-01", "CY2023Q1"),
                            duration("2023-04-01", "2023-06-30", 4_400_000_000, "2023-08-01", "CY2023Q2"),
                            duration("2023-07-01", "2023-09-30", 4_800_000_000, "2023-11-01", "CY2023Q3"),
                            duration("2023-10-01", "2023-12-31", 5_200_000_000, "2024-02-01", "CY2023Q4"),
                            duration("2024-01-01", "2024-03-31", 5_600_000_000, "2024-05-01", "CY2024Q1"),
                        ]
                    }
                },
                "StockholdersEquity": {
                    "units": {
                        "USD": [
                            instant("2023-03-31", 10_000_000_000, "2023-05-01", "CY2023Q1I"),
                            instant("2023-06-30", 11_000_000_000, "2023-08-01", "CY2023Q2I"),
                            instant("2023-09-30", 12_000_000_000, "2023-11-01", "CY2023Q3I"),
                            instant("2023-12-31", 13_000_000_000, "2024-02-01", "CY2023Q4I"),
                            instant("2024-03-31", 14_000_000_000, "2024-05-01", "CY2024Q1I"),
                        ]
                    }
                },
            }
        }
    }


def _stub_companyfacts_without_equity() -> dict[str, object]:
    payload = _stub_companyfacts_with_equity()
    payload["facts"]["us-gaap"].pop("StockholdersEquity", None)
    return payload


def _stub_companyfacts_with_eur_units() -> dict[str, object]:
    def duration(start: str, end: str, val: int, filed: str, frame: str) -> dict[str, object]:
        return {"start": start, "end": end, "val": val, "form": "6-K", "filed": filed, "frame": frame}

    def instant(end: str, val: int, filed: str, frame: str) -> dict[str, object]:
        return {"end": end, "val": val, "form": "20-F", "filed": filed, "frame": frame}

    return {
        "facts": {
            "us-gaap": {
                "Revenue": {
                    "units": {
                        "EUR": [
                            duration("2023-01-01", "2023-03-31", 6_700_000_000, "2023-04-20", "CY2023Q1"),
                            duration("2023-04-01", "2023-06-30", 6_900_000_000, "2023-07-19", "CY2023Q2"),
                            duration("2023-07-01", "2023-09-30", 6_700_000_000, "2023-10-18", "CY2023Q3"),
                            duration("2023-10-01", "2023-12-31", 7_200_000_000, "2024-01-24", "CY2023Q4"),
                        ]
                    }
                },
                "NetIncomeLoss": {
                    "units": {
                        "EUR": [
                            duration("2023-01-01", "2023-03-31", 1_900_000_000, "2023-04-20", "CY2023Q1"),
                            duration("2023-04-01", "2023-06-30", 1_950_000_000, "2023-07-19", "CY2023Q2"),
                            duration("2023-07-01", "2023-09-30", 1_850_000_000, "2023-10-18", "CY2023Q3"),
                            duration("2023-10-01", "2023-12-31", 2_100_000_000, "2024-01-24", "CY2023Q4"),
                        ]
                    }
                },
                "GrossProfit": {
                    "units": {
                        "EUR": [
                            duration("2023-01-01", "2023-03-31", 3_400_000_000, "2023-04-20", "CY2023Q1"),
                            duration("2023-04-01", "2023-06-30", 3_500_000_000, "2023-07-19", "CY2023Q2"),
                            duration("2023-07-01", "2023-09-30", 3_450_000_000, "2023-10-18", "CY2023Q3"),
                            duration("2023-10-01", "2023-12-31", 3_800_000_000, "2024-01-24", "CY2023Q4"),
                        ]
                    }
                },
                "StockholdersEquity": {
                    "units": {
                        "EUR": [
                            instant("2023-03-31", 30_000_000_000, "2023-04-20", "CY2023Q1I"),
                            instant("2023-06-30", 31_000_000_000, "2023-07-19", "CY2023Q2I"),
                            instant("2023-09-30", 31_500_000_000, "2023-10-18", "CY2023Q3I"),
                            instant("2023-12-31", 32_000_000_000, "2024-01-24", "CY2023Q4I"),
                        ]
                    }
                },
            }
        }
    }


def _stub_companyfacts_with_annual_delta_fallback() -> dict[str, object]:
    def duration(start: str, end: str, val: int, form: str, frame: str) -> dict[str, object]:
        return {
            "start": start,
            "end": end,
            "val": val,
            "form": form,
            "filed": "2025-02-15",
            "frame": frame,
            "fp": "Q1" if frame.endswith("Q1") else "Q2" if frame.endswith("Q2") else "Q3" if frame.endswith("Q3") else "FY",
        }

    return {
        "facts": {
            "us-gaap": {
                "RevenuesNetOfInterestExpense": {
                    "units": {
                        "USD": [
                            duration("2024-01-01", "2024-03-31", 100_000_000_000, "10-Q", "CY2024Q1"),
                            duration("2024-04-01", "2024-06-30", 120_000_000_000, "10-Q", "CY2024Q2"),
                            duration("2024-07-01", "2024-09-30", 110_000_000_000, "10-Q", "CY2024Q3"),
                            duration("2024-01-01", "2024-12-31", 460_000_000_000, "10-K", "CY2024"),
                        ]
                    }
                },
                "NetIncomeLoss": {
                    "units": {
                        "USD": [
                            duration("2024-01-01", "2024-03-31", 10_000_000_000, "10-Q", "CY2024Q1"),
                            duration("2024-04-01", "2024-06-30", 12_000_000_000, "10-Q", "CY2024Q2"),
                            duration("2024-07-01", "2024-09-30", 11_000_000_000, "10-Q", "CY2024Q3"),
                            duration("2024-01-01", "2024-12-31", 45_000_000_000, "10-K", "CY2024"),
                        ]
                    }
                },
                "GrossProfit": {
                    "units": {
                        "USD": [
                            duration("2024-01-01", "2024-03-31", 40_000_000_000, "10-Q", "CY2024Q1"),
                            duration("2024-04-01", "2024-06-30", 48_000_000_000, "10-Q", "CY2024Q2"),
                            duration("2024-07-01", "2024-09-30", 44_000_000_000, "10-Q", "CY2024Q3"),
                            duration("2024-10-01", "2024-12-31", 52_000_000_000, "10-Q", "CY2024Q4"),
                        ]
                    }
                },
            }
        }
    }


class EarningsDigestStudioTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._backup_root = Path(tempfile.mkdtemp(prefix="earnings-digest-tests-"))
        self._data_backup_path = self._backup_root / "data-backup"
        self.addCleanup(self._restore_data_dir)
        self._previous_source_fetch = os.environ.get("EARNINGS_DIGEST_DISABLE_SOURCE_FETCH")
        os.environ["EARNINGS_DIGEST_DISABLE_SOURCE_FETCH"] = "1"
        self.addCleanup(self._restore_source_fetch_env)
        self._previous_full_coverage = os.environ.get("EARNINGS_DIGEST_REQUIRE_FULL_COVERAGE")
        os.environ["EARNINGS_DIGEST_REQUIRE_FULL_COVERAGE"] = "0"
        self.addCleanup(self._restore_full_coverage_env)
        if DATA_DIR.exists():
            shutil.move(str(DATA_DIR), str(self._data_backup_path))
        init_db()
        self.client = TestClient(app)

    def _write_temp_text(self, filename: str, content: str) -> str:
        path = self._backup_root / filename
        path.write_text(content, encoding="utf-8")
        return str(path)

    def _restore_data_dir(self) -> None:
        if DATA_DIR.exists():
            shutil.rmtree(DATA_DIR, ignore_errors=True)
        if self._data_backup_path.exists():
            shutil.move(str(self._data_backup_path), str(DATA_DIR))
        shutil.rmtree(self._backup_root, ignore_errors=True)

    def _restore_source_fetch_env(self) -> None:
        if self._previous_source_fetch is None:
            os.environ.pop("EARNINGS_DIGEST_DISABLE_SOURCE_FETCH", None)
            return
        os.environ["EARNINGS_DIGEST_DISABLE_SOURCE_FETCH"] = self._previous_source_fetch

    def _restore_full_coverage_env(self) -> None:
        if self._previous_full_coverage is None:
            os.environ.pop("EARNINGS_DIGEST_REQUIRE_FULL_COVERAGE", None)
            return
        os.environ["EARNINGS_DIGEST_REQUIRE_FULL_COVERAGE"] = self._previous_full_coverage

    def test_calendar_quarter_mapping_uses_majority_months(self) -> None:
        self.assertEqual(resolve_calendar_quarter_from_months(["2025-11", "2025-12", "2026-01"]), "2025Q4")

    def test_pdf_export_mode_defaults_to_raster(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(_pdf_export_mode(), "raster")
        with patch.dict(os.environ, {"EARNINGS_DIGEST_PDF_EXPORT_MODE": "vector"}, clear=False):
            self.assertEqual(_pdf_export_mode(), "vector")

    def test_build_raster_pdf_html_embeds_each_page_image(self) -> None:
        html = _build_raster_pdf_html(["page-01.png", "page-02.png"])
        self.assertIn("page-01.png", html)
        self.assertIn("page-02.png", html)
        self.assertIn("@page", html)
        self.assertIn("13.333in 7.5in", html)

    def test_nvidia_history_cube_has_12_points_and_segments(self) -> None:
        cube = build_historical_quarter_cube("nvidia", "2025Q4", 12)
        self.assertEqual(len(cube), 12)
        self.assertEqual(cube[-1]["quarter_label"], "2025Q4")
        self.assertTrue(cube[-1]["segments"])
        self.assertEqual(resolve_structure_dimension("nvidia", cube), "segment")

    def test_bundled_financial_source_data_loads_without_workspace_seed_files(self) -> None:
        local_data_service.load_financial_source_data.cache_clear()
        self.addCleanup(local_data_service.load_financial_source_data.cache_clear)
        with patch.object(local_data_service, "TECH_ANALYSIS_DATA_PATH", BUNDLED_TECH_ANALYSIS_DATA_PATH):
            payload = local_data_service.load_financial_source_data()
        self.assertIn("companies", payload)
        self.assertIn("nvidia", payload["companies"])

    def test_bundled_nvidia_segment_history_loads_without_workspace_seed_files(self) -> None:
        local_data_service.load_nvidia_segment_history.cache_clear()
        self.addCleanup(local_data_service.load_nvidia_segment_history.cache_clear)
        with patch.object(local_data_service, "NVIDIA_SEGMENT_HISTORY_PATH", BUNDLED_NVIDIA_SEGMENT_HISTORY_PATH):
            history = local_data_service.load_nvidia_segment_history()
        self.assertTrue(history)
        self.assertIn("segments", next(iter(history.values())))

    def test_earlier_quarter_can_still_build_full_12_quarter_history(self) -> None:
        cube = build_historical_quarter_cube("nvidia", "2023Q4", 12)
        self.assertEqual(len(cube), 12)
        self.assertEqual(cube[0]["quarter_label"], "2021Q1")
        self.assertEqual(cube[-1]["quarter_label"], "2023Q4")

    def test_sanitize_history_quality_metrics_removes_roe_outlier(self) -> None:
        history = [
            {"quarter_label": "2025Q1", "roe_pct": 7.8},
            {"quarter_label": "2025Q2", "roe_pct": 8.1},
            {"quarter_label": "2025Q3", "roe_pct": 8.4},
            {"quarter_label": "2025Q4", "roe_pct": 8.9},
            {"quarter_label": "2026Q1", "roe_pct": 37.4},
        ]
        sanitized = _sanitize_history_quality_metrics(history)
        self.assertIsNone(sanitized[-1]["roe_pct"])

    def test_evaluate_report_payload_flags_critical_gaps(self) -> None:
        quality = evaluate_report_payload(
            {
                "latest_kpis": {},
                "historical_cube": [],
                "current_segments": [],
                "current_geographies": [],
                "qna_themes": [],
                "management_themes": [],
                "evidence_cards": [],
                "source_materials": [],
            },
            history_window=12,
        )
        self.assertEqual(quality["status"], "fail")
        codes = {item["code"] for item in quality["issues"]}
        self.assertIn("latest_kpi_missing_revenue", codes)
        self.assertIn("history_missing", codes)
        self.assertIn("qna_missing", codes)

    def test_build_report_payload_contains_quality_report(self) -> None:
        payload = build_report_payload("nvidia", "2025Q4", 12, refresh_source_materials=False)
        self.assertIn("quality_report", payload)
        self.assertIn(payload["quality_report"]["status"], {"pass", "review", "fail"})
        self.assertIsInstance(payload["quality_report"]["score"], int)

    def test_evaluate_report_payload_management_mode_structure_gap_is_minor(self) -> None:
        quality = evaluate_report_payload(
            {
                "structure_dimension_used": "management",
                "latest_kpis": {"revenue_bn": 10.0, "net_income_bn": 1.0},
                "historical_cube": [{"quarter_label": f"2024Q{i}", "revenue_bn": 9.0 + i, "net_income_bn": 0.8 + i * 0.05} for i in range(1, 5)],
                "current_segments": [],
                "current_geographies": [],
                "qna_themes": [{"label": "A"}, {"label": "B"}],
                "management_themes": [{"label": "A"}, {"label": "B"}],
                "evidence_cards": [{"title": "A"}, {"title": "B"}],
                "source_materials": [],
            },
            history_window=4,
        )
        self.assertEqual(quality["status"], "pass")
        self.assertTrue(any(item["code"] == "current_structure_missing" and item["severity"] == "minor" for item in quality["issues"]))

    def test_evaluate_report_payload_full_coverage_rejects_annual_fallback_geography(self) -> None:
        payload = {
            "structure_dimension_used": "segment",
            "latest_kpis": {"revenue_bn": 10.0, "net_income_bn": 1.0, "gaap_gross_margin_pct": 40.0},
            "historical_cube": [
                {
                    "quarter_label": "2024Q1",
                    "revenue_bn": 8.0,
                    "net_income_bn": 0.8,
                    "segments": [{"name": "A", "value_bn": 4.0}, {"name": "B", "value_bn": 4.0}],
                    "geographies": [{"name": "US", "value_bn": 5.0}, {"name": "Intl", "value_bn": 3.0}],
                },
                {
                    "quarter_label": "2024Q2",
                    "revenue_bn": 9.0,
                    "net_income_bn": 0.9,
                    "segments": [{"name": "A", "value_bn": 4.5}, {"name": "B", "value_bn": 4.5}],
                    "geographies": [{"name": "US", "value_bn": 5.4}, {"name": "Intl", "value_bn": 3.6}],
                },
                {
                    "quarter_label": "2024Q3",
                    "revenue_bn": 9.5,
                    "net_income_bn": 0.95,
                    "segments": [{"name": "A", "value_bn": 4.8}, {"name": "B", "value_bn": 4.7}],
                    "geographies": [{"name": "US", "value_bn": 5.7}, {"name": "Intl", "value_bn": 3.8}],
                },
                {
                    "quarter_label": "2024Q4",
                    "revenue_bn": 10.0,
                    "net_income_bn": 1.0,
                    "segments": [{"name": "A", "value_bn": 5.0}, {"name": "B", "value_bn": 5.0}],
                    "geographies": [{"name": "US", "value_bn": 6.0}, {"name": "Intl", "value_bn": 4.0}],
                },
            ],
            "current_segments": [{"name": "A", "value_bn": 5.0}, {"name": "B", "value_bn": 5.0}],
            "current_geographies": [
                {"name": "United States", "value_bn": 7.0, "scope": "annual_filing"},
                {"name": "International", "value_bn": 3.0, "scope": "annual_filing"},
            ],
            "qna_themes": [{"label": "A"}, {"label": "B"}, {"label": "C"}],
            "management_themes": [{"label": "A"}, {"label": "B"}, {"label": "C"}],
            "evidence_cards": [{"title": "A"}, {"title": "B"}, {"title": "C"}],
            "source_materials": [
                {"role": "earnings_release", "status": "cached", "text_length": 100},
                {"role": "sec_filing", "status": "cached", "text_length": 100},
                {"role": "earnings_commentary", "status": "cached", "text_length": 100},
            ],
            "coverage_warnings": ["公司未在当季单独披露季度地区拆分时，系统会回退到最新年报中的地区收入口径并显式标注。"],
        }
        quality = evaluate_report_payload(
            payload,
            history_window=4,
            require_full_coverage=True,
        )
        self.assertEqual(quality["status"], "fail")
        codes = {item["code"] for item in quality["issues"]}
        self.assertIn("full_coverage_geography_non_quarterly", codes)
        self.assertIn("full_coverage_fallback_detected", codes)

    def test_evaluate_report_payload_full_coverage_allows_release_surrogate_with_dense_context(self) -> None:
        payload = {
            "structure_dimension_used": "segment",
            "latest_kpis": {"revenue_bn": 10.0, "net_income_bn": 1.0, "gaap_gross_margin_pct": 40.0},
            "historical_cube": [
                {
                    "quarter_label": "2024Q1",
                    "revenue_bn": 8.0,
                    "net_income_bn": 0.8,
                    "segments": [{"name": "A", "value_bn": 4.0}, {"name": "B", "value_bn": 4.0}],
                    "geographies": [{"name": "US", "value_bn": 5.0}, {"name": "Intl", "value_bn": 3.0, "scope": "quarterly_mapped_from_official_geography"}],
                },
                {
                    "quarter_label": "2024Q2",
                    "revenue_bn": 9.0,
                    "net_income_bn": 0.9,
                    "segments": [{"name": "A", "value_bn": 4.5}, {"name": "B", "value_bn": 4.5}],
                    "geographies": [{"name": "US", "value_bn": 5.4}, {"name": "Intl", "value_bn": 3.6, "scope": "quarterly_mapped_from_official_geography"}],
                },
                {
                    "quarter_label": "2024Q3",
                    "revenue_bn": 9.5,
                    "net_income_bn": 0.95,
                    "segments": [{"name": "A", "value_bn": 4.8}, {"name": "B", "value_bn": 4.7}],
                    "geographies": [{"name": "US", "value_bn": 5.7}, {"name": "Intl", "value_bn": 3.8, "scope": "quarterly_mapped_from_official_geography"}],
                },
                {
                    "quarter_label": "2024Q4",
                    "revenue_bn": 10.0,
                    "net_income_bn": 1.0,
                    "segments": [{"name": "A", "value_bn": 5.0}, {"name": "B", "value_bn": 5.0}],
                    "geographies": [{"name": "US", "value_bn": 6.0}, {"name": "Intl", "value_bn": 4.0, "scope": "quarterly_mapped_from_official_geography"}],
                },
            ],
            "current_segments": [{"name": "A", "value_bn": 5.0}, {"name": "B", "value_bn": 5.0}],
            "current_geographies": [
                {"name": "United States", "value_bn": 7.0, "scope": "quarterly_mapped_from_official_geography"},
                {"name": "International", "value_bn": 3.0, "scope": "quarterly_mapped_from_official_geography"},
            ],
            "qna_themes": [{"label": "A"}, {"label": "B"}, {"label": "C"}],
            "management_themes": [{"label": "A"}, {"label": "B"}, {"label": "C"}],
            "evidence_cards": [{"title": "A"}, {"title": "B"}, {"title": "C"}],
            "source_materials": [
                {"role": "sec_filing", "status": "cached", "text_length": 100},
                {"role": "investor_relations", "status": "cached", "text_length": 100},
                {"role": "earnings_commentary", "status": "cached", "text_length": 100},
            ],
            "coverage_warnings": [],
        }
        quality = evaluate_report_payload(
            payload,
            history_window=4,
            require_full_coverage=True,
        )
        self.assertEqual(quality["status"], "pass")
        codes = {item["code"] for item in quality["issues"]}
        self.assertNotIn("full_coverage_source_role_missing_earnings_release", codes)

    def test_recompute_history_derivatives_rebuilds_ttm_roe_from_equity(self) -> None:
        history = [
            {"quarter_label": "2024Q1", "net_income_bn": 3.0, "equity_bn": 30.0, "revenue_bn": 20.0},
            {"quarter_label": "2024Q2", "net_income_bn": 3.2, "equity_bn": 31.0, "revenue_bn": 21.0},
            {"quarter_label": "2024Q3", "net_income_bn": 3.4, "equity_bn": 32.0, "revenue_bn": 22.0},
            {"quarter_label": "2024Q4", "net_income_bn": 3.6, "equity_bn": 33.0, "revenue_bn": 23.0},
            {"quarter_label": "2025Q1", "net_income_bn": 3.8, "equity_bn": 34.0, "revenue_bn": 24.0, "roe_pct": None},
        ]

        recomputed = _recompute_history_derivatives(history)

        expected_ttm_roe = (3.2 + 3.4 + 3.6 + 3.8) / ((30.0 + 31.0 + 32.0 + 33.0 + 34.0) / 5) * 100
        self.assertAlmostEqual(recomputed[-1]["roe_pct"], expected_ttm_roe, places=4)

    def test_build_companyfacts_series_computes_ttm_roe_from_equity(self) -> None:
        series_payload = _build_companyfacts_series(_stub_companyfacts_with_equity())
        self.assertIn("equity", series_payload["series"])
        self.assertAlmostEqual(series_payload["series"]["grossMargin"]["2024Q1"], 40.0)
        expected_ttm_roe = (1.1 + 1.2 + 1.3 + 1.4) / ((10 + 11 + 12 + 13 + 14) / 5) * 100
        self.assertAlmostEqual(series_payload["series"]["roe"]["2024Q1"], expected_ttm_roe, places=4)

    def test_build_companyfacts_series_drops_roe_when_equity_missing(self) -> None:
        series_payload = _build_companyfacts_series(_stub_companyfacts_without_equity())
        self.assertEqual(series_payload["series"]["roe"], {})

    def test_build_companyfacts_series_supports_non_usd_units(self) -> None:
        series_payload = _build_companyfacts_series(_stub_companyfacts_with_eur_units(), preferred_currency_code="EUR")
        self.assertEqual(series_payload["periods"], ["2023Q1", "2023Q2", "2023Q3", "2023Q4"])
        self.assertIn("2023Q4", series_payload["series"]["revenue"])
        self.assertAlmostEqual(series_payload["series"]["grossMargin"]["2023Q4"], (3_800_000_000 / 7_200_000_000) * 100)
        self.assertIn("2023Q4", series_payload["series"]["roe"])

    def test_build_companyfacts_series_can_fill_q4_from_annual_delta(self) -> None:
        series_payload = _build_companyfacts_series(_stub_companyfacts_with_annual_delta_fallback(), preferred_currency_code="USD")
        self.assertIn("2024Q4", series_payload["periods"])
        self.assertEqual(series_payload["series"]["revenue"]["2024Q4"], 130_000_000_000)
        self.assertEqual(series_payload["series"]["earnings"]["2024Q4"], 12_000_000_000)

    def test_historical_official_cache_alignment_allows_geography_only_annual_fallback_cache(self) -> None:
        entry = {"period_end": "2011-06-30"}
        cached_payload = {
            "latest_kpis": {},
            "current_segments": [],
            "current_geographies": [
                {"name": "Europe", "value_bn": 3.377, "scope": "annual_filing"},
                {"name": "Retail", "value_bn": 211.734, "scope": "annual_filing"},
            ],
            "source_date": "2013-03-01",
        }
        self.assertTrue(reports_service._historical_official_cache_is_temporally_aligned(entry, cached_payload))

    def test_historical_official_cache_alignment_keeps_strict_window_for_metric_caches(self) -> None:
        entry = {"period_end": "2009-06-30"}
        cached_payload = {
            "latest_kpis": {"revenue_bn": 31.2},
            "current_segments": [{"name": "BNSF", "value_bn": 5.851}],
            "current_geographies": [
                {"name": "Europe", "value_bn": 28.954, "scope": "quarterly_mapped_from_official_geography"},
            ],
            "source_date": "2011-02-28",
        }
        self.assertFalse(reports_service._historical_official_cache_is_temporally_aligned(entry, cached_payload))

    @patch("app.services.local_data.get_company_series")
    def test_get_supported_quarters_filters_periods_without_core_metrics(self, mock_get_company_series: object) -> None:
        periods = ["2020Q1", "2020Q2", "2020Q3", "2020Q4", "2021Q1", "2021Q2"]
        mock_get_company_series.return_value = (
            periods,
            {
                "revenue": {
                    "2020Q1": 100,
                    "2020Q2": 105,
                    "2020Q3": 110,
                    "2020Q4": 120,
                    "2021Q1": 130,
                    "2021Q2": None,
                },
                "earnings": {
                    "2020Q1": 10,
                    "2020Q2": 11,
                    "2020Q3": None,
                    "2020Q4": 13,
                    "2021Q1": 14,
                    "2021Q2": 15,
                },
            },
        )
        supported = get_supported_quarters("nvidia", history_window=4, fetch_missing=True)
        self.assertEqual(supported, [])

    @patch("app.services.local_data.get_company_series")
    def test_get_supported_quarters_rejects_sparse_annual_like_windows(self, mock_get_company_series: object) -> None:
        periods = ["2021Q4", "2022Q4", "2023Q4", "2024Q4", "2025Q4"]
        mock_get_company_series.return_value = (
            periods,
            {
                "revenue": {period: 100 + index * 10 for index, period in enumerate(periods)},
                "earnings": {period: 10 + index for index, period in enumerate(periods)},
            },
        )
        supported = get_supported_quarters("nvidia", history_window=5, fetch_missing=True)
        self.assertEqual(supported, [])

    def test_company_quarters_ignores_partial_ready_map_until_full_audit_finishes(self) -> None:
        ready_map_path = self._backup_root / "partial-ready-map.json"
        ready_map_path.write_text(
            json.dumps(
                {
                    "history_window": 12,
                    "require_full_coverage": True,
                    "companies": {
                        "nvidia": {
                            "ready_quarters": ["2025Q4"],
                            "all_quarters_audited": False,
                        }
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        with patch.dict(os.environ, {"EARNINGS_DIGEST_REQUIRE_FULL_COVERAGE": "1"}):
            with patch("app.services.reports.FULL_COVERAGE_READY_MAP_PATH", ready_map_path):
                payload = company_quarters("nvidia", 12)

        self.assertFalse(payload["full_coverage_ready_map_applied"])
        self.assertGreater(len(payload["supported_quarters"]), 1)
        self.assertIn("2025Q4", payload["supported_quarters"])

    def test_company_quarters_applies_ready_map_after_full_audit(self) -> None:
        ready_map_path = self._backup_root / "full-ready-map.json"
        ready_map_path.write_text(
            json.dumps(
                {
                    "history_window": 12,
                    "require_full_coverage": True,
                    "companies": {
                        "nvidia": {
                            "ready_quarters": ["2024Q4", "2025Q4"],
                            "all_quarters_audited": True,
                        }
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        with patch.dict(os.environ, {"EARNINGS_DIGEST_REQUIRE_FULL_COVERAGE": "1"}):
            with patch("app.services.reports.FULL_COVERAGE_READY_MAP_PATH", ready_map_path):
                payload = company_quarters("nvidia", 12)

        self.assertTrue(payload["full_coverage_ready_map_applied"])
        self.assertEqual(payload["supported_quarters"], ["2024Q4", "2025Q4"])

    def test_audit_ready_map_forces_full_window_scope(self) -> None:
        output_path = self._backup_root / "audit-output.json"
        ready_map_path = self._backup_root / "ready-map.json"

        def _fake_audit_company(
            company_id: str,
            *,
            history_window: int,
            all_quarters: bool,
            quarters_per_company: int,
            include_unready: bool,
            refresh_source_materials: bool,
            require_full_coverage: bool,
            existing_info: object = None,
            quarter_complete_callback: object = None,
        ) -> tuple[str, dict[str, object], list[dict[str, object]]]:
            self.assertTrue(all_quarters)
            self.assertTrue(include_unready)
            self.assertTrue(require_full_coverage)
            self.assertEqual(history_window, 12)
            return (
                company_id,
                {
                    "audited_quarter_count": 2,
                    "all_window_quarter_count": 2,
                    "status_counts": {"pass": 2, "review": 0, "fail": 0, "error": 0},
                    "issue_counts": {"critical": 0, "major": 0, "minor": 0},
                    "quarters": {
                        "2024Q4": {"ok": True, "quality": {"status": "pass"}},
                        "2025Q4": {"ok": True, "quality": {"status": "pass"}},
                    },
                },
                [
                    {"company_id": company_id, "calendar_quarter": "2024Q4", "ok": True, "quality": {"status": "pass"}},
                    {"company_id": company_id, "calendar_quarter": "2025Q4", "ok": True, "quality": {"status": "pass"}},
                ],
            )

        with patch.object(sys, "argv", [
            "audit_dynamic_reports.py",
            "--company",
            "nvidia",
            "--all-quarters",
            "--write-ready-map",
            str(ready_map_path),
            "--output",
            str(output_path),
        ]):
            with patch.dict(os.environ, {"EARNINGS_DIGEST_DISABLE_SOURCE_FETCH": "0"}):
                with patch("scripts.audit_dynamic_reports._audit_company", side_effect=_fake_audit_company):
                    exit_code = audit_dynamic_reports.main()

        self.assertEqual(exit_code, 0)
        ready_map = json.loads(ready_map_path.read_text(encoding="utf-8"))
        self.assertTrue(ready_map["include_unready_audited"])
        self.assertEqual(ready_map["companies"]["nvidia"]["ready_quarters"], ["2024Q4", "2025Q4"])
        self.assertTrue(ready_map["companies"]["nvidia"]["all_quarters_audited"])

    def test_audit_resume_skips_completed_companies_from_output_checkpoint(self) -> None:
        output_path = self._backup_root / "audit-resume-output.json"
        output_path.write_text(
            json.dumps(
                {
                    "summary": {
                        "history_window": 12,
                        "require_full_coverage": True,
                        "include_unready_audited": True,
                    },
                    "companies": {
                        "nvidia": {
                            "audited_quarter_count": 1,
                            "all_window_quarter_count": 1,
                            "quarters": {
                                "2025Q4": {"ok": True, "quality": {"status": "pass"}}
                            },
                            "status_counts": {"pass": 1, "review": 0, "fail": 0, "error": 0},
                            "issue_counts": {"critical": 0, "major": 0, "minor": 0},
                        }
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        audited_companies: list[str] = []

        def _fake_audit_company(
            company_id: str,
            *,
            history_window: int,
            all_quarters: bool,
            quarters_per_company: int,
            include_unready: bool,
            refresh_source_materials: bool,
            require_full_coverage: bool,
            existing_info: object = None,
            quarter_complete_callback: object = None,
        ) -> tuple[str, dict[str, object], list[dict[str, object]]]:
            audited_companies.append(company_id)
            return (
                company_id,
                {
                    "audited_quarter_count": 1,
                    "all_window_quarter_count": 1,
                    "status_counts": {"pass": 1, "review": 0, "fail": 0, "error": 0},
                    "issue_counts": {"critical": 0, "major": 0, "minor": 0},
                    "quarters": {
                        "2025Q4": {"ok": True, "quality": {"status": "pass"}}
                    },
                },
                [
                    {"company_id": company_id, "calendar_quarter": "2025Q4", "ok": True, "quality": {"status": "pass"}},
                ],
            )

        with patch.object(sys, "argv", [
            "audit_dynamic_reports.py",
            "--company",
            "nvidia",
            "--company",
            "apple",
            "--all-quarters",
            "--write-ready-map",
            str(self._backup_root / "resume-ready-map.json"),
            "--output",
            str(output_path),
            "--resume",
        ]):
            with patch.dict(os.environ, {"EARNINGS_DIGEST_DISABLE_SOURCE_FETCH": "0"}):
                with patch("scripts.audit_dynamic_reports._audit_company", side_effect=_fake_audit_company):
                    exit_code = audit_dynamic_reports.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(audited_companies, ["apple"])
        rendered = json.loads(output_path.read_text(encoding="utf-8"))
        self.assertEqual(set(rendered["companies"].keys()), {"nvidia", "apple"})
        self.assertEqual(rendered["summary"]["companies_audited"], 2)

    def test_audit_resume_continues_company_when_all_quarters_not_finished(self) -> None:
        output_path = self._backup_root / "audit-partial-output.json"
        output_path.write_text(
            json.dumps(
                {
                    "summary": {
                        "history_window": 12,
                        "require_full_coverage": True,
                        "include_unready_audited": True,
                    },
                    "companies": {
                        "nvidia": {
                            "audited_quarter_count": 1,
                            "all_window_quarter_count": 3,
                            "quarters": {
                                "2025Q4": {"ok": True, "quality": {"status": "pass"}}
                            },
                            "status_counts": {"pass": 1, "review": 0, "fail": 0, "error": 0},
                            "issue_counts": {"critical": 0, "major": 0, "minor": 0},
                        }
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        audited_companies: list[str] = []

        def _fake_audit_company(
            company_id: str,
            *,
            history_window: int,
            all_quarters: bool,
            quarters_per_company: int,
            include_unready: bool,
            refresh_source_materials: bool,
            require_full_coverage: bool,
            existing_info: object = None,
            quarter_complete_callback: object = None,
        ) -> tuple[str, dict[str, object], list[dict[str, object]]]:
            audited_companies.append(company_id)
            self.assertIsNotNone(existing_info)
            return (
                company_id,
                {
                    "audited_quarter_count": 3,
                    "all_window_quarter_count": 3,
                    "status_counts": {"pass": 3, "review": 0, "fail": 0, "error": 0},
                    "issue_counts": {"critical": 0, "major": 0, "minor": 0},
                    "quarters": {
                        "2025Q4": {"ok": True, "quality": {"status": "pass"}},
                        "2025Q3": {"ok": True, "quality": {"status": "pass"}},
                        "2025Q2": {"ok": True, "quality": {"status": "pass"}},
                    },
                },
                [
                    {"company_id": company_id, "calendar_quarter": "2025Q4", "ok": True, "quality": {"status": "pass"}},
                    {"company_id": company_id, "calendar_quarter": "2025Q3", "ok": True, "quality": {"status": "pass"}},
                    {"company_id": company_id, "calendar_quarter": "2025Q2", "ok": True, "quality": {"status": "pass"}},
                ],
            )

        with patch.object(sys, "argv", [
            "audit_dynamic_reports.py",
            "--company",
            "nvidia",
            "--all-quarters",
            "--write-ready-map",
            str(self._backup_root / "partial-ready-map.json"),
            "--output",
            str(output_path),
            "--resume",
        ]):
            with patch.dict(os.environ, {"EARNINGS_DIGEST_DISABLE_SOURCE_FETCH": "0"}):
                with patch("scripts.audit_dynamic_reports._audit_company", side_effect=_fake_audit_company):
                    exit_code = audit_dynamic_reports.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(audited_companies, ["nvidia"])

    def test_backfill_historical_segment_history_interpolates_missing_quarters(self) -> None:
        company = get_company("tsmc")
        history = [
            {"quarter_label": "2025Q1", "revenue_bn": 20.0, "segments": []},
            {
                "quarter_label": "2025Q2",
                "revenue_bn": 22.0,
                "segments": [
                    {"name": "HPC", "value_bn": 11.0, "share_pct": 50.0},
                    {"name": "Smartphone", "value_bn": 6.6, "share_pct": 30.0},
                    {"name": "Internet of Things", "value_bn": 2.2, "share_pct": 10.0},
                    {"name": "Automotive", "value_bn": 1.1, "share_pct": 5.0},
                    {"name": "DCE", "value_bn": 0.66, "share_pct": 3.0},
                    {"name": "Others", "value_bn": 0.44, "share_pct": 2.0},
                ],
            },
            {"quarter_label": "2025Q3", "revenue_bn": 24.0, "segments": []},
            {
                "quarter_label": "2025Q4",
                "revenue_bn": 26.0,
                "segments": [
                    {"name": "HPC", "value_bn": 14.3, "share_pct": 55.0},
                    {"name": "Smartphone", "value_bn": 7.8, "share_pct": 30.0},
                    {"name": "Internet of Things", "value_bn": 1.3, "share_pct": 5.0},
                    {"name": "Automotive", "value_bn": 1.3, "share_pct": 5.0},
                    {"name": "DCE", "value_bn": 0.52, "share_pct": 2.0},
                    {"name": "Others", "value_bn": 0.78, "share_pct": 3.0},
                ],
            },
        ]
        enriched = _backfill_historical_segment_history(company, history)
        self.assertEqual(len(enriched[0]["segments"]), 6)
        self.assertTrue(enriched[0]["segments_inferred"])
        self.assertEqual(len(enriched[2]["segments"]), 6)
        self.assertTrue(enriched[2]["segments_inferred"])

    def test_backfill_historical_core_metrics_fills_isolated_gap_from_adjacent_quarters(self) -> None:
        history = [
            {"quarter_label": "2008Q4", "revenue_bn": None, "net_income_bn": None, "gross_margin_pct": None, "equity_bn": 42.5},
            {"quarter_label": "2009Q1", "revenue_bn": 15.0, "net_income_bn": 3.5, "gross_margin_pct": 68.0, "equity_bn": 43.0},
            {"quarter_label": "2009Q2", "revenue_bn": 15.4, "net_income_bn": 3.6, "gross_margin_pct": 68.5, "equity_bn": 43.4},
        ]

        enriched = _backfill_historical_core_metrics(history)

        self.assertAlmostEqual(enriched[0]["revenue_bn"], 15.0, places=3)
        self.assertAlmostEqual(enriched[0]["net_income_bn"], 3.5, places=3)
        self.assertAlmostEqual(enriched[0]["gross_margin_pct"], 68.0, places=3)
        self.assertTrue(enriched[0]["core_metrics_inferred"])

    def test_enrich_history_with_official_structures_reuses_cached_quarter_parse(self) -> None:
        reports_service.HISTORICAL_OFFICIAL_QUARTER_MEMORY_CACHE.clear()
        self.addCleanup(reports_service.HISTORICAL_OFFICIAL_QUARTER_MEMORY_CACHE.clear)
        cache_dir = self._backup_root / "historical-quarter-cache"
        company = get_company("asml")
        history = [
            {
                "quarter_label": "2025Q4",
                "calendar_quarter": "2025Q4",
                "period_end": "2025-12-31",
                "revenue_bn": 30.0,
                "net_income_bn": 7.0,
                "gross_margin_pct": 51.0,
                "revenue_yoy_pct": 12.0,
                "net_income_yoy_pct": 18.0,
                "equity_bn": 20.0,
                "roe_pct": None,
                "segments": [],
                "geographies": [],
                "source_type": "structured_financial_series",
            }
        ]
        parsed_payload = {
            "latest_kpis": {
                "revenue_bn": 30.9,
                "net_income_bn": 7.6,
                "gaap_gross_margin_pct": 52.0,
                "revenue_yoy_pct": 13.0,
                "net_income_yoy_pct": 19.0,
                "ending_equity_bn": 21.0,
            },
            "current_segments": [
                {"name": "Net system sales", "value_bn": 24.0},
                {"name": "Installed Base Management", "value_bn": 6.9},
            ],
            "current_geographies": [
                {"name": "China", "value_bn": 10.0, "share_pct": 32.4},
                {"name": "Taiwan", "value_bn": 8.0, "share_pct": 25.9},
            ],
        }

        with patch("app.services.reports.HISTORICAL_OFFICIAL_QUARTER_CACHE_DIR", cache_dir):
            with patch("app.services.reports.resolve_official_sources", return_value=[{"url": "https://example.com/asml", "date": "2026-01-28"}]) as mock_sources:
                with patch("app.services.reports.hydrate_source_materials", return_value=[{"status": "cached"}]) as mock_materials:
                    with patch("app.services.reports.parse_official_materials", return_value=parsed_payload) as mock_parse:
                        first = _enrich_history_with_official_structures(company, history)

            self.assertEqual(mock_sources.call_count, 1)
            self.assertEqual(mock_materials.call_count, 1)
            self.assertEqual(mock_parse.call_count, 1)
            self.assertTrue(first[0]["segments"])
            self.assertTrue(first[0]["geographies"])

            with patch("app.services.reports.resolve_official_sources", side_effect=AssertionError("should use cache")):
                with patch("app.services.reports.hydrate_source_materials", side_effect=AssertionError("should use cache")):
                    with patch("app.services.reports.parse_official_materials", side_effect=AssertionError("should use cache")):
                        second = _enrich_history_with_official_structures(company, history)

        self.assertTrue(second[0]["segments"])
        self.assertTrue(second[0]["geographies"])
        self.assertAlmostEqual(second[0]["equity_bn"], 21.0)

    def test_enrich_history_with_official_structures_reuses_stale_cache_structures_without_overwriting_metrics(self) -> None:
        company = get_company("nvidia")
        history = [
            {
                "quarter_label": "2023Q1",
                "calendar_quarter": "2023Q1",
                "period_end": "2023-04-30",
                "revenue_bn": 7.192,
                "net_income_bn": 2.043,
                "gross_margin_pct": None,
                "revenue_yoy_pct": 19.0,
                "net_income_yoy_pct": 26.0,
                "equity_bn": None,
                "roe_pct": None,
                "segments": [],
                "geographies": [],
                "source_type": "structured_financial_series",
                "source_url": "",
            }
        ]
        stale_snapshot = {
            "latest_kpis": {
                "revenue_bn": 70.0,
                "net_income_bn": 20.0,
                "gaap_gross_margin_pct": 90.0,
            },
            "current_segments": [
                {"name": "Data Center", "value_bn": 4.284},
                {"name": "Gaming", "value_bn": 2.24},
                {"name": "Professional Visualization", "value_bn": 0.295},
                {"name": "Automotive", "value_bn": 0.296},
                {"name": "OEM and Other", "value_bn": 0.077},
            ],
            "current_geographies": [
                {"name": "United States", "value_bn": 4.198},
                {"name": "International", "value_bn": 2.992},
            ],
            "source_url": "https://example.com/nvidia/2025q1",
            "source_date": "2025-05-24",
        }

        with patch("app.services.reports._load_historical_official_quarter_cache", return_value=stale_snapshot):
            with patch("app.services.reports.resolve_official_sources", side_effect=AssertionError("should reuse stale structures")):
                with patch("app.services.reports.hydrate_source_materials", side_effect=AssertionError("should reuse stale structures")):
                    with patch("app.services.reports.parse_official_materials", side_effect=AssertionError("should reuse stale structures")):
                        enriched = _enrich_history_with_official_structures(company, history)

        self.assertAlmostEqual(enriched[0]["revenue_bn"], 7.192, places=3)
        self.assertAlmostEqual(enriched[0]["net_income_bn"], 2.043, places=3)
        self.assertIsNone(enriched[0]["gross_margin_pct"])
        self.assertEqual(enriched[0]["source_type"], "structured_financial_series")
        self.assertEqual(enriched[0].get("source_url"), "")
        self.assertNotIn("release_date", enriched[0])
        self.assertEqual(len(enriched[0]["segments"]), 5)
        self.assertEqual(len(enriched[0]["geographies"]), 2)

    def test_incomplete_historical_segment_snapshot_is_rejected_and_backfilled(self) -> None:
        company = get_company("apple")
        history = [
            {
                "quarter_label": "2025Q1",
                "revenue_bn": 100.0,
                "segments": [
                    {"name": "iPhone", "value_bn": 52.0, "share_pct": 52.0},
                    {"name": "Mac", "value_bn": 8.0, "share_pct": 8.0},
                    {"name": "iPad", "value_bn": 7.0, "share_pct": 7.0},
                    {"name": "Wearables, Home and Accessories", "value_bn": 9.0, "share_pct": 9.0},
                    {"name": "Services", "value_bn": 24.0, "share_pct": 24.0},
                ],
                "geographies": [],
                "structure_basis": "segment",
            },
            {
                "quarter_label": "2025Q2",
                "revenue_bn": 102.0,
                "segments": [
                    {"name": "Mac", "value_bn": 34.0, "share_pct": 33.3},
                    {"name": "iPad", "value_bn": 28.0, "share_pct": 27.5},
                    {"name": "Wearables, Home and Accessories", "value_bn": 40.0, "share_pct": 39.2},
                ],
                "geographies": [
                    {"name": "Americas", "value_bn": 45.0},
                    {"name": "Europe", "value_bn": 28.0},
                    {"name": "Greater China", "value_bn": 14.0},
                    {"name": "Japan", "value_bn": 7.0},
                    {"name": "Rest of Asia Pacific", "value_bn": 8.0},
                ],
                "structure_basis": "segment",
            },
            {
                "quarter_label": "2025Q3",
                "revenue_bn": 104.0,
                "segments": [
                    {"name": "iPhone", "value_bn": 54.0, "share_pct": 51.9},
                    {"name": "Mac", "value_bn": 8.0, "share_pct": 7.7},
                    {"name": "iPad", "value_bn": 7.0, "share_pct": 6.7},
                    {"name": "Wearables, Home and Accessories", "value_bn": 9.0, "share_pct": 8.7},
                    {"name": "Services", "value_bn": 26.0, "share_pct": 25.0},
                ],
                "geographies": [],
                "structure_basis": "segment",
            },
        ]
        harmonized = _harmonize_historical_structures(company, history)
        self.assertFalse(harmonized[1]["segments"])
        self.assertTrue(harmonized[1]["geographies"])
        enriched = _backfill_historical_segment_history(company, harmonized)
        self.assertEqual(len(enriched[1]["segments"]), 5)
        self.assertTrue(enriched[1]["segments_inferred"])

    def test_harmonize_historical_structures_clears_geography_segments_when_segment_basis_exists(self) -> None:
        company = get_company("tsmc")
        history = [
            {
                "quarter_label": "2025Q1",
                "revenue_bn": 20.0,
                "segments": [
                    {"name": "HPC", "value_bn": 10.0},
                    {"name": "Smartphone", "value_bn": 6.0},
                    {"name": "Internet of Things", "value_bn": 2.0},
                    {"name": "Automotive", "value_bn": 1.0},
                    {"name": "DCE", "value_bn": 0.5},
                    {"name": "Others", "value_bn": 0.5},
                ],
                "geographies": [],
                "structure_basis": "segment",
            },
            {
                "quarter_label": "2025Q2",
                "revenue_bn": 22.0,
                "segments": [
                    {"name": "United States", "value_bn": 16.0},
                    {"name": "China", "value_bn": 4.0},
                    {"name": "Taiwan", "value_bn": 2.0},
                ],
                "geographies": [
                    {"name": "United States", "value_bn": 16.0},
                    {"name": "China", "value_bn": 4.0},
                    {"name": "Taiwan", "value_bn": 2.0},
                ],
                "structure_basis": "geography",
            },
        ]
        harmonized = _harmonize_historical_structures(company, history)
        self.assertEqual(harmonized[0]["structure_basis"], "segment")
        self.assertEqual(harmonized[1]["structure_basis"], None)
        self.assertFalse(harmonized[1]["segments"])
        self.assertTrue(harmonized[1]["geographies"])

    def test_segments_are_geography_like_detects_extended_official_region_labels(self) -> None:
        company = get_company("alphabet")
        self.assertTrue(
            _segments_are_geography_like(
                company,
                [
                    {"name": "United States", "value_bn": 14.933},
                    {"name": "EMEA", "value_bn": 10.785},
                    {"name": "Asia Pacific", "value_bn": 5.09},
                    {"name": "Americas Excluding U.S.", "value_bn": 1.849},
                ],
            )
        )

    def test_harmonize_historical_structures_keeps_geography_only_history_out_of_segments(self) -> None:
        company = get_company("alphabet")
        history = [
            {
                "quarter_label": "2019Q1",
                "revenue_bn": 30.7,
                "segments": [],
                "geographies": [
                    {"name": "United States", "value_bn": 13.8},
                    {"name": "EMEA", "value_bn": 10.1},
                    {"name": "Asia Pacific", "value_bn": 4.9},
                    {"name": "Americas Excluding U.S.", "value_bn": 1.9},
                ],
                "structure_basis": "geography",
            },
            {
                "quarter_label": "2019Q2",
                "revenue_bn": 32.657,
                "segments": [
                    {"name": "United States", "value_bn": 14.933},
                    {"name": "EMEA", "value_bn": 10.785},
                    {"name": "Asia Pacific", "value_bn": 5.09},
                    {"name": "Americas Excluding U.S.", "value_bn": 1.849},
                ],
                "geographies": [
                    {"name": "United States", "value_bn": 14.933},
                    {"name": "EMEA", "value_bn": 10.785},
                    {"name": "Asia Pacific", "value_bn": 5.09},
                    {"name": "Americas Excluding U.S.", "value_bn": 1.849},
                ],
                "structure_basis": "geography",
            },
        ]
        harmonized = _harmonize_historical_structures(company, history)
        self.assertEqual(harmonized[0]["structure_basis"], "geography")
        self.assertEqual(harmonized[1]["structure_basis"], "geography")
        self.assertFalse(harmonized[0]["segments"])
        self.assertFalse(harmonized[1]["segments"])
        self.assertTrue(harmonized[0]["geographies"])
        self.assertTrue(harmonized[1]["geographies"])

    def test_extract_table_metric_handles_sec_footnote_rows(self) -> None:
        sample = """
        Net sales by category:
        iPhone
        (1)
        $ 51,982
        $ 61,104
        (15
        )%
        Mac
        (1)
        7,416
        6,824
        9
        %
        Wearables, Home and Accessories
        (1)(2)
        7,308
        5,481
        33
        %
        Services
        (3)
        10,875
        9,129
        19
        %
        """
        iphone_current, iphone_prior, iphone_yoy = _extract_table_metric(sample, ["iPhone"])
        wearables_current, wearables_prior, wearables_yoy = _extract_table_metric(sample, ["Wearables, Home and Accessories"])
        self.assertAlmostEqual(iphone_current or 0.0, 51.982, places=3)
        self.assertAlmostEqual(iphone_prior or 0.0, 61.104, places=3)
        self.assertAlmostEqual(iphone_yoy or 0.0, -15.0, places=1)
        self.assertAlmostEqual(wearables_current or 0.0, 7.308, places=3)
        self.assertAlmostEqual(wearables_prior or 0.0, 5.481, places=3)
        self.assertAlmostEqual(wearables_yoy or 0.0, 33.0, places=1)

    @patch("app.services.reports.parse_official_materials")
    @patch("app.services.reports.hydrate_source_materials")
    @patch("app.services.reports.resolve_official_sources")
    def test_enrich_history_with_official_structures_can_repair_metrics(
        self,
        mock_resolve_sources: object,
        mock_hydrate: object,
        mock_parse_materials: object,
    ) -> None:
        mock_resolve_sources.return_value = [{"url": "https://example.com/apple-10q", "date": "2008-02-01"}]
        mock_hydrate.return_value = [{"label": "Apple 10-Q", "kind": "sec_filing", "status": "cached"}]
        mock_parse_materials.return_value = {
            "latest_kpis": {
                "revenue_bn": 9.608,
                "net_income_bn": 1.581,
                "gaap_gross_margin_pct": 34.7,
                "revenue_yoy_pct": 35.0,
                "net_income_yoy_pct": 57.5,
            },
            "current_segments": [
                {"name": "Mac", "value_bn": 3.552},
                {"name": "iPod", "value_bn": 3.997},
            ],
        }
        history = [
            {
                "quarter_label": "2007Q1",
                "fiscal_label": "2007Q1",
                "period_end": "2007-03-31",
                "release_date": "2007-03-31",
                "revenue_bn": 5.264,
                "net_income_bn": 0.77,
                "gross_margin_pct": None,
                "revenue_yoy_pct": 21.0,
                "net_income_yoy_pct": 35.0,
                "net_margin_pct": 14.6,
                "segments": None,
                "geographies": None,
                "structure_basis": None,
                "source_type": "structured_financial_series",
                "source_url": "",
            },
            {
                "quarter_label": "2007Q4",
                "fiscal_label": "2007Q4",
                "period_end": "2007-12-31",
                "release_date": "2007-12-31",
                "revenue_bn": 6.62475,
                "net_income_bn": 1.30325,
                "gross_margin_pct": None,
                "revenue_yoy_pct": 28.1,
                "net_income_yoy_pct": 53.8,
                "net_margin_pct": 19.7,
                "segments": None,
                "geographies": None,
                "structure_basis": None,
                "source_type": "structured_financial_series",
                "source_url": "",
            },
        ]
        enriched = _enrich_history_with_official_structures(get_company("apple"), history)
        enriched = _recompute_history_derivatives(enriched)
        self.assertAlmostEqual(enriched[-1]["revenue_bn"], 9.608, places=3)
        self.assertAlmostEqual(enriched[-1]["net_income_bn"], 1.581, places=3)
        self.assertAlmostEqual(enriched[-1]["gross_margin_pct"], 34.7, places=1)
        self.assertEqual(enriched[-1]["source_type"], "official_release")
        self.assertTrue(enriched[-1]["segments"])

    @patch("app.services.reports.parse_official_materials")
    @patch("app.services.reports.hydrate_source_materials")
    @patch("app.services.reports.resolve_official_sources")
    def test_enrich_history_with_official_structures_does_not_copy_geographies_into_segments(
        self,
        mock_resolve_sources: object,
        mock_hydrate: object,
        mock_parse_materials: object,
    ) -> None:
        mock_resolve_sources.return_value = [{"url": "https://example.com/alphabet-release", "date": "2019-07-25"}]
        mock_hydrate.return_value = [{"label": "Alphabet release", "kind": "official_release", "status": "cached"}]
        mock_parse_materials.return_value = {
            "latest_kpis": {"revenue_bn": 32.657},
            "current_segments": [],
            "current_geographies": [
                {"name": "United States", "value_bn": 14.933},
                {"name": "EMEA", "value_bn": 10.785},
                {"name": "Asia Pacific", "value_bn": 5.09},
                {"name": "Americas Excluding U.S.", "value_bn": 1.849},
            ],
        }
        history = [
            {
                "quarter_label": "2019Q2",
                "fiscal_label": "2019Q2",
                "period_end": "2019-06-30",
                "release_date": "2019-07-25",
                "revenue_bn": 32.657,
                "net_income_bn": 9.947,
                "gross_margin_pct": None,
                "revenue_yoy_pct": 19.0,
                "net_income_yoy_pct": 0.0,
                "net_margin_pct": 30.5,
                "segments": None,
                "geographies": None,
                "structure_basis": None,
                "source_type": "structured_financial_series",
                "source_url": "",
            }
        ]
        enriched = _enrich_history_with_official_structures(get_company("alphabet"), history)
        self.assertEqual(enriched[-1]["structure_basis"], "geography")
        self.assertFalse(enriched[-1]["segments"])
        self.assertTrue(enriched[-1]["geographies"])

    def test_enrich_history_with_partial_official_segments_keeps_more_complete_existing_history(self) -> None:
        company = get_company("nvidia")
        history = [
            {
                "quarter_label": "2023Q1",
                "fiscal_label": "2023Q1",
                "period_end": "2023-04-30",
                "release_date": "2023-05-24",
                "revenue_bn": 7.192,
                "net_income_bn": 2.043,
                "gross_margin_pct": 64.6,
                "revenue_yoy_pct": 19.0,
                "net_income_yoy_pct": 26.0,
                "net_margin_pct": 28.4,
                "segments": [
                    {"name": "Data Center", "value_bn": 4.284, "share_pct": 59.6},
                    {"name": "Gaming", "value_bn": 2.24, "share_pct": 31.1},
                    {"name": "Professional Visualization", "value_bn": 0.295, "share_pct": 4.1},
                    {"name": "Automotive", "value_bn": 0.296, "share_pct": 4.1},
                    {"name": "OEM and Other", "value_bn": 0.077, "share_pct": 1.1},
                ],
                "geographies": [],
                "structure_basis": "segment",
                "source_type": "structured_financial_series",
                "source_url": "",
            }
        ]

        def cached_partial_snapshot(company_id: str, period: str) -> dict[str, object]:
            if company_id == "nvidia" and period == "2023Q1":
                return {
                    "latest_kpis": {},
                    "current_segments": [
                        {"name": "Gaming", "value_bn": 2.24},
                        {"name": "Professional Visualization", "value_bn": 0.295},
                        {"name": "Automotive", "value_bn": 0.296},
                    ],
                    "current_geographies": [
                        {"name": "United States", "value_bn": 4.198},
                        {"name": "International", "value_bn": 2.992},
                    ],
                    "source_url": "https://example.com/nvidia/2023q1",
                    "source_date": "2023-05-24",
                }
            return {
                "latest_kpis": {},
                "current_segments": [],
                "current_geographies": [],
                "source_url": "",
                "source_date": "",
            }

        with patch("app.services.reports._load_historical_official_quarter_cache", side_effect=cached_partial_snapshot):
            with patch("app.services.reports.resolve_official_sources", side_effect=AssertionError("should use cache")):
                with patch("app.services.reports.hydrate_source_materials", side_effect=AssertionError("should use cache")):
                    with patch("app.services.reports.parse_official_materials", side_effect=AssertionError("should use cache")):
                        enriched = _enrich_history_with_official_structures(company, history)

        q1_2023 = enriched[0]
        self.assertEqual(
            [item["name"] for item in q1_2023["segments"]],
            ["Data Center", "Gaming", "Professional Visualization", "Automotive", "OEM and Other"],
        )
        segments = {item["name"]: item for item in q1_2023["segments"]}
        self.assertAlmostEqual(segments["Data Center"]["value_bn"], 4.284, places=3)
        self.assertTrue(q1_2023["geographies"])

    def test_history_cube_requires_full_window(self) -> None:
        with self.assertRaises(ValueError):
            build_historical_quarter_cube("nvidia", "1900Q1", 12)

    def test_history_cube_rejects_sparse_non_contiguous_windows_without_official_source(self) -> None:
        sparse_periods = ["2021Q4", "2022Q4", "2023Q4", "2024Q4", "2025Q4"]
        sparse_series = {
            "revenue": {period: 100 + index * 10 for index, period in enumerate(sparse_periods)},
            "earnings": {period: 10 + index for index, period in enumerate(sparse_periods)},
            "grossMargin": {},
            "revenueGrowth": {},
            "roe": {},
            "equity": {},
            "periodMeta": {},
        }
        with patch(
            "app.services.reports.get_company",
            return_value={**get_company("nvidia"), "official_source": {}},
        ):
            with self.assertRaises(ValueError):
                build_historical_quarter_cube("nvidia", "2025Q4", 5, periods=sparse_periods, series=sparse_series)

    def test_apple_history_cube_remaps_to_natural_quarter(self) -> None:
        cube = build_historical_quarter_cube("apple", "2025Q4", 12)
        self.assertEqual(cube[-1]["quarter_label"], "2025Q4")
        self.assertAlmostEqual(cube[-1]["revenue_bn"], 143.756, places=3)
        self.assertEqual(cube[-1]["fiscal_label"], "Q1 FY2026")

    def test_avgo_history_cube_falls_back_when_segments_missing(self) -> None:
        cube = build_historical_quarter_cube("avgo", "2025Q4", 12)
        self.assertEqual(len(cube), 12)
        self.assertFalse(all(entry.get("segments") for entry in cube))
        self.assertEqual(resolve_structure_dimension("avgo", cube), "management")

    def test_enrich_history_with_official_structures_accepts_adjusted_profit_basis_for_special_item_quarter(self) -> None:
        company = get_company("visa")
        history = [
            {
                "quarter_label": "2017Q1",
                "period_end": "2017-03-31",
                "revenue_bn": 4.477,
                "net_income_bn": 0.43,
                "gross_margin_pct": None,
                "revenue_yoy_pct": 17.3,
                "net_income_yoy_pct": -74.8,
                "segments": [],
                "geographies": [],
                "source_type": "structured_financial_series",
                "source_url": "",
            }
        ]

        cached_snapshot = {
            "latest_kpis": {
                "net_income_bn": 2.1,
                "net_income_yoy_pct": 29.15,
                "gaap_eps": 0.18,
                "non_gaap_eps": 0.86,
            },
            "current_segments": [],
            "current_geographies": [],
            "source_url": "https://example.com/visa/2017q1",
            "source_date": "2017-04-20",
            "profit_basis": "adjusted_special_items",
        }

        with patch("app.services.reports._load_historical_official_quarter_cache", return_value=cached_snapshot):
            with patch("app.services.reports.resolve_official_sources", side_effect=AssertionError("should use cache")):
                with patch("app.services.reports.hydrate_source_materials", side_effect=AssertionError("should use cache")):
                    with patch("app.services.reports.parse_official_materials", side_effect=AssertionError("should use cache")):
                        enriched = _enrich_history_with_official_structures(company, history)

        self.assertAlmostEqual(enriched[0]["net_income_bn"], 2.1, places=2)
        self.assertAlmostEqual(enriched[0]["net_margin_pct"], 2.1 / 4.477 * 100, places=2)
        self.assertAlmostEqual(enriched[0]["net_income_yoy_pct"], 29.15, places=2)

    def test_enrich_history_with_official_structures_rejects_revenue_override_that_creates_implausible_margin(self) -> None:
        company = get_company("visa")
        history = [
            {
                "quarter_label": "2016Q2",
                "period_end": "2016-06-30",
                "revenue_bn": 3.63,
                "net_income_bn": 0.412,
                "gross_margin_pct": None,
                "revenue_yoy_pct": 3.18,
                "net_income_yoy_pct": -75.72,
                "segments": [],
                "geographies": [],
                "source_type": "structured_financial_series",
                "source_url": "",
            }
        ]

        cached_snapshot = {
            "latest_kpis": {
                "revenue_bn": 1.635,
                "net_income_bn": 1.6,
                "net_income_yoy_pct": -11.45,
            },
            "current_segments": [],
            "current_geographies": [],
            "source_url": "https://example.com/visa/2016q2",
            "source_date": "2016-07-21",
            "profit_basis": "adjusted_special_items",
        }

        with patch("app.services.reports._load_historical_official_quarter_cache", return_value=cached_snapshot):
            with patch("app.services.reports.resolve_official_sources", side_effect=AssertionError("should use cache")):
                with patch("app.services.reports.hydrate_source_materials", side_effect=AssertionError("should use cache")):
                    with patch("app.services.reports.parse_official_materials", side_effect=AssertionError("should use cache")):
                        enriched = _enrich_history_with_official_structures(company, history)

        self.assertAlmostEqual(enriched[0]["revenue_bn"], 3.63, places=2)
        self.assertAlmostEqual(enriched[0]["net_income_bn"], 1.6, places=2)
        self.assertLess(enriched[0]["net_margin_pct"], 50.0)

    def test_companyfacts_series_maps_historical_quarter_metrics(self) -> None:
        payload = {
            "facts": {
                "us-gaap": {
                    "RevenueFromContractWithCustomerExcludingAssessedTax": {
                        "units": {
                            "USD": [
                                {
                                    "start": "2016-10-31",
                                    "end": "2017-01-29",
                                    "val": 4_139_000_000,
                                    "accn": "0001730168-18-000084",
                                    "fy": 2018,
                                    "fp": "FY",
                                    "form": "10-K",
                                    "filed": "2018-12-21",
                                    "frame": "CY2016Q4",
                                }
                            ]
                        }
                    },
                    "NetIncomeLoss": {
                        "units": {
                            "USD": [
                                {
                                    "start": "2016-10-31",
                                    "end": "2017-01-29",
                                    "val": 239_000_000,
                                    "accn": "0001730168-18-000084",
                                    "fy": 2018,
                                    "fp": "FY",
                                    "form": "10-K",
                                    "filed": "2018-12-21",
                                    "frame": "CY2016Q4",
                                }
                            ]
                        }
                    },
                    "GrossProfit": {
                        "units": {
                            "USD": [
                                {
                                    "start": "2016-10-31",
                                    "end": "2017-01-29",
                                    "val": 2_001_000_000,
                                    "accn": "0001730168-18-000084",
                                    "fy": 2018,
                                    "fp": "FY",
                                    "form": "10-K",
                                    "filed": "2018-12-21",
                                    "frame": "CY2016Q4",
                                }
                            ]
                        }
                    },
                }
            }
        }
        series_payload = _build_companyfacts_series(payload)
        self.assertEqual(series_payload["periods"], ["2016Q4"])
        self.assertEqual(series_payload["series"]["revenue"]["2016Q4"], 4_139_000_000)
        self.assertEqual(series_payload["series"]["earnings"]["2016Q4"], 239_000_000)
        self.assertAlmostEqual(series_payload["series"]["grossMargin"]["2016Q4"], 48.345, places=3)
        self.assertEqual(series_payload["series"]["periodMeta"]["2016Q4"]["date_key"], "2017-01-29")
        self.assertEqual(series_payload["series"]["periodMeta"]["2016Q4"]["accn"], "0001730168-18-000084")

    def test_avgo_legacy_quarter_report_handles_missing_guidance_values(self) -> None:
        payload = build_report_payload("avgo", "2016Q4", 12, refresh_source_materials=False)
        self.assertEqual(payload["calendar_quarter"], "2016Q4")
        self.assertIn("guidance", payload["visuals"])
        self.assertEqual(payload["guidance"]["mode"], "proxy")
        self.assertEqual(payload["latest_kpis"]["revenue_bn"], 4.139)
        self.assertIn("$4.1B", payload["visuals"]["guidance"])

    def test_merge_parsed_payload_reapplies_theme_minimums(self) -> None:
        merged = _merge_parsed_payload(
            {
                "management_themes": [
                    {"label": "AWS 加速扩张", "score": 88, "note": "AWS 仍是利润质量核心。"},
                    {"label": "北美零售底盘稳固", "score": 80, "note": "北美零售仍是最大收入基础盘。"},
                    {"label": "国际业务仍在修复", "score": 72, "note": "国际业务恢复速度仍需观察。"},
                ],
                "qna_themes": [
                    {"label": "AWS 增长持续性", "score": 80, "note": "云业务需求与利润率仍是电话会焦点。"},
                    {"label": "零售利润率兑现", "score": 74, "note": "零售效率改善仍需验证。"},
                ],
                "risks": [{"label": "高投入压制利润弹性", "score": 66, "note": "高投入仍会拖慢利润释放。"}],
                "catalysts": [{"label": "AWS 继续抬升 mix", "score": 84, "note": "高利润率业务继续改善结构质量。"}],
            },
            {
                "management_themes": [
                    {"label": "AWS 加速扩张", "score": 88, "note": "AWS 仍是利润质量核心。"},
                    {"label": "北美零售底盘稳固", "score": 80, "note": "北美零售仍是最大收入基础盘。"},
                    {"label": "国际业务仍在修复", "score": 72, "note": "国际业务恢复速度仍需观察。"},
                ],
                "qna_themes": [
                    {"label": "AWS 增长持续性", "score": 80, "note": "云业务需求与利润率仍是电话会焦点。"},
                    {"label": "零售利润率兑现", "score": 74, "note": "零售效率改善仍需验证。"},
                    {"label": "延伸关注：AWS 加速扩张", "score": 88, "note": "AWS 仍是利润质量核心。"},
                ],
                "risks": [{"label": "高投入压制利润弹性", "score": 66, "note": "高投入仍会拖慢利润释放。"}],
                "catalysts": [{"label": "AWS 继续抬升 mix", "score": 84, "note": "高利润率业务继续改善结构质量。"}],
            },
        )

        self.assertGreaterEqual(len(merged["qna_themes"]), 3)

    def test_historical_sec_cik_override_applies_for_alphabet_pre_reorg(self) -> None:
        source_config = dict(get_company("alphabet")["official_source"])
        self.assertEqual(_sec_cik_for_calendar_quarter(source_config, "2013Q3"), "0001288776")
        self.assertEqual(_sec_cik_for_calendar_quarter(source_config, "2025Q4"), "0001652044")

    @patch("app.services.official_source_resolver._load_submissions", return_value={"filings": {"recent": {"form": []}}})
    @patch("app.services.official_source_resolver._discover_attachment_url", return_value=("https://example.com/wrapper", ""))
    @patch("app.services.official_source_resolver._select_filing")
    def test_resolve_official_sources_cache_key_respects_source_config(
        self,
        mock_select_filing: object,
        _mock_discover_attachment_url: object,
        _mock_load_submissions: object,
    ) -> None:
        def select_side_effect(
            _submissions: dict[str, object],
            *,
            company_id: str,
            period_end: str,
            forms: list[str],
            release_mode: bool,
            document_hints: tuple[str, ...] = (),
            document_excludes: tuple[str, ...] = (),
            refresh: bool = False,
        ) -> dict[str, object] | None:
            _ = (company_id, period_end, document_hints, document_excludes, refresh)
            if release_mode and "8-K" in forms:
                return {
                    "form": "8-K",
                    "accessionNumber": "000140316117000026",
                    "primaryDocument": "vex991earningsrelease33117.htm",
                    "filingDate": "2017-04-20",
                }
            if not release_mode and "10-Q" in forms:
                return {
                    "form": "10-Q",
                    "accessionNumber": "000140316117000028",
                    "primaryDocument": "v33117form10q.htm",
                    "filingDate": "2017-04-21",
                }
            if not release_mode and forms == ["10-K"]:
                return {
                    "form": "10-K",
                    "accessionNumber": "000140316118000020",
                    "primaryDocument": "0001403161-18-000020.txt",
                    "filingDate": "2018-04-27",
                }
            return None

        mock_select_filing.side_effect = select_side_effect
        source_resolver.RESOLVED_SOURCES_MEMORY_CACHE.clear()

        company = get_company("visa")
        annual_company = dict(company)
        annual_company["official_source"] = {
            **dict(company.get("official_source") or {}),
            "release_forms": [],
            "filing_forms": ["10-K"],
        }

        with patch.dict(os.environ, {"EARNINGS_DIGEST_DISABLE_SOURCE_FETCH": "0"}):
            annual_sources = source_resolver.resolve_official_sources(
                annual_company,
                "2017Q1",
                "2017-03-31",
                [],
                refresh=False,
                prefer_sec_only=True,
            )
            quarter_sources = source_resolver.resolve_official_sources(
                company,
                "2017Q1",
                "2017-03-31",
                [],
                refresh=False,
                prefer_sec_only=True,
            )

        self.assertTrue(str(annual_sources[0]["url"]).endswith("0001403161-18-000020.txt"))
        self.assertTrue(any(str(item["url"]).endswith("v33117form10q.htm") for item in quarter_sources))

    def test_parse_official_materials_walmart_extracts_legacy_net_sales_table(self) -> None:
        release_path = self._write_temp_text(
            "walmart-legacy-release.txt",
            (
                "Net sales for the fourth quarter of fiscal 2013 were $127.1 billion, an increase of 3.9 percent from $122.3 billion in last year's fourth quarter. "
                "Income from continuing operations attributable to Walmart for the fourth quarter was $5.6 billion, up 7.9 percent. "
                "Diluted earnings per share from continuing operations attributable to Walmart for the fourth quarter of fiscal 2013 were $1.67. "
                "Net Sales: (dollars in billions) 2013 2012 Percent Change 2013 2012 Percent Change "
                "Walmart U.S. $ 74.665 $ 72.789 2.6 % $ 274.490 $ 264.186 3.9 % "
                "Walmart International 37.949 35.486 6.9 % 135.201 125.873 7.4 % "
                "Sam's Club 14.490 14.010 3.4 % 56.423 53.795 4.9 % "
                "Total Company $ 127.104 $ 122.285 3.9 % $ 466.114 $ 443.854 5.0 % "
            ),
        )

        parsed = parse_official_materials(
            get_company("walmart"),
            {"fiscal_label": "2012Q4", "coverage_notes": []},
            [
                {"label": "Walmart earnings release", "kind": "official_release", "status": "cached", "text_path": release_path},
            ],
        )

        self.assertEqual(
            [item["name"] for item in parsed["current_segments"]],
            ["Walmart U.S.", "Walmart International", "Sam's Club U.S."],
        )
        self.assertEqual(
            [item["name"] for item in parsed["current_geographies"]],
            ["Walmart U.S.", "Walmart International", "Sam's Club U.S."],
        )
        self.assertAlmostEqual(parsed["current_segments"][0]["value_bn"], 74.665, places=3)

    def test_home_and_company_api_return_top_20_pool(self) -> None:
        home = self.client.get("/")
        self.assertEqual(home.status_code, 200)
        self.assertIn("Earnings Digest Studio", home.text)
        self.assertTrue("美股市值前 20" in home.text or "选择范围" in home.text)
        self.assertIn("强制刷新官方源", home.text)

        companies = self.client.get("/companies")
        self.assertEqual(companies.status_code, 200)
        payload = companies.json()
        self.assertEqual(len(payload), 20)
        tickers = {item["ticker"] for item in payload}
        self.assertIn("TSM", tickers)
        self.assertIn("ASML", tickers)
        self.assertEqual(payload[0]["id"], "nvidia")
        self.assertIn("2025Q4", payload[0]["supported_quarters"])

        quarters = self.client.get("/companies/nvidia/quarters", params={"history_window": 12})
        self.assertEqual(quarters.status_code, 200)
        quarter_payload = quarters.json()
        self.assertIn("2023Q4", quarter_payload["supported_quarters"])
        self.assertEqual(quarter_payload["supported_quarters"][-1], "2025Q4")

    def test_parse_official_materials_prefers_quarterly_sec_filing_when_annual_is_also_present(self) -> None:
        quarterly_path = self._write_temp_text(
            "apple-quarterly-sec-preferred.txt",
            (
                "iPhone $ 55,957 $ 51,982 8 % "
                "Mac 7,160 7,416 (3) % "
                "iPad 5,977 6,729 (11) % "
                "Wearables, Home and Accessories 10,010 7,308 37 % "
                "Services 12,715 10,875 17 % "
                "Total net sales $ 91,819 $ 84,310 9 % "
                "Gross margin 35,314 32,031 "
                "Operating income 23,894 23,346 "
                "Net income $ 22,236 $ 19,965 "
            ),
        )
        annual_path = self._write_temp_text(
            "apple-annual-sec-not-preferred.txt",
            (
                "iPhone $ 142,381 $ 166,699 (15) % "
                "Services 46,291 39,748 16 % "
                "Total net sales $ 260,174 $ 265,595 (2) % "
                "Gross margin 98,392 101,839 "
                "Operating income 63,930 70,898 "
                "Net income $ 55,256 $ 59,531 "
            ),
        )

        parsed = parse_official_materials(
            get_company("apple"),
            {"fiscal_label": "2020Q1", "calendar_quarter": "2019Q3", "coverage_notes": []},
            [
                {"label": "Apple Form 10-K", "kind": "sec_filing", "status": "cached", "text_path": annual_path},
                {"label": "Apple Form 10-Q", "kind": "sec_filing", "status": "cached", "text_path": quarterly_path},
            ],
        )

        self.assertAlmostEqual(parsed["latest_kpis"]["revenue_bn"], 91.819, places=3)
        self.assertAlmostEqual(parsed["latest_kpis"]["net_income_bn"], 22.236, places=3)
        self.assertEqual(parsed["current_segments"][0]["name"], "iPhone")

    def test_parse_official_materials_microsoft_can_fall_back_to_sec_only_for_segments(self) -> None:
        sec_path = self._write_temp_text(
            "microsoft-sec-only-history.txt",
            (
                "Total revenue 32,471 28,918 "
                "Total cost of revenue 11,127 10,234 "
                "Research and development 4,068 3,759 "
                "Sales and marketing 4,776 4,148 "
                "General and administrative 1,323 1,279 "
                "Operating income 13,891 10,258 "
                "Productivity and Business Processes 11,833 10,101 "
                "Intelligent Cloud 11,869 9,379 "
                "More Personal Computing 13,211 12,991 "
                "Revenue, classified by the major geographic areas in which our customers were located, was as follows: "
                "(In millions) Three Months Ended December 31 2019 2018 Six Months Ended December 31 2019 2018 "
                "United States (a) 18,113 16,081 35,221 31,945 "
                "Other countries 18,793 16,390 35,700 31,547 "
            ),
        )
        parsed = parse_official_materials(
            get_company("microsoft"),
            {"fiscal_label": "2020Q2", "calendar_quarter": "2019Q4", "coverage_notes": []},
            [
                {"label": "Microsoft Form 10-Q", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )
        self.assertEqual([item["name"] for item in parsed["current_segments"]], ["More Personal Computing", "Intelligent Cloud", "Productivity and Business Processes"])
        self.assertEqual([item["name"] for item in parsed["current_geographies"]], ["Other countries", "United States"])
        self.assertAlmostEqual(parsed["latest_kpis"]["revenue_bn"], 32.471, places=3)

    def test_parse_official_materials_alphabet_historical_can_fall_back_without_release(self) -> None:
        sec_path = self._write_temp_text(
            "alphabet-historical-sec-only.txt",
            (
                "Total revenues 46,075 55,314 "
                "Google Search & other 31,879 39,545 "
                "YouTube ads 4,038 6,005 "
                "Google Network Members' properties 5,195 6,800 "
                "Google Cloud 2,777 4,047 "
                "Google other 5,449 6,494 "
                "Other Bets revenues 135 198 "
                "United States revenues (GAAP) 21,711 25,895 19 % "
                "EMEA revenues (GAAP) 14,391 17,030 18 % "
                "APAC revenues (GAAP) 6,929 8,157 18 % "
                "Other Americas revenues (GAAP) 3,044 4,232 39 % "
            ),
        )
        parsed = parse_official_materials(
            get_company("alphabet"),
            {"fiscal_label": "2021Q1", "calendar_quarter": "2021Q1", "coverage_notes": []},
            [
                {"label": "Alphabet Form 10-Q", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )
        self.assertTrue(parsed["current_segments"])
        self.assertIn("Google Search & other", [item["name"] for item in parsed["current_segments"]])
        self.assertTrue(parsed["current_geographies"])
        self.assertIn("United States", [item["name"] for item in parsed["current_geographies"]])

    def test_parse_official_materials_alphabet_legacy_business_mix_uses_google_properties_taxonomy(self) -> None:
        release_path = self._write_temp_text(
            "alphabet-legacy-release.txt",
            (
                "Segment revenues and operating results "
                "Three Months Ended September 30, 2017 "
                "Three Months Ended September 30, 2018 "
                "Google properties revenues $19,723 $24,054 "
                "Google Network Members' properties revenues 4,342 4,900 "
                "Google advertising revenues 24,065 28,954 "
                "Google other revenues 3,590 4,640 "
                "Google segment revenues $27,655 $33,594 "
                "Other Bets revenues $117 $146 "
                "United States revenues (GAAP) $15,523 $12,930 16.7% "
                "EMEA revenues (GAAP) $10,961 $9,097 17.0% "
                "APAC revenues (GAAP) $5,426 $4,199 22.6% "
                "Other Americas revenues (GAAP) $1,835 $1,546 15.7% "
                "Total revenues $27,772 $33,740 "
            ),
        )
        parsed = parse_official_materials(
            get_company("alphabet"),
            {"fiscal_label": "2018Q3", "calendar_quarter": "2018Q3", "coverage_notes": [], "latest_kpis": {"revenue_bn": 33.740}},
            [
                {"label": "Alphabet release", "kind": "official_release", "status": "cached", "text_path": release_path},
            ],
        )
        self.assertEqual(
            [item["name"] for item in parsed["current_segments"]],
            ["Google properties", "Google Network", "Google other", "Other Bets"],
        )
        self.assertEqual([item["name"] for item in parsed["current_geographies"][:2]], ["United States", "EMEA"])

    def test_normalize_segment_items_alphabet_legacy_report_keeps_business_and_geography_distinct(self) -> None:
        parsed = {
            "current_segments": [
                {"name": "Google properties", "value_bn": 24.054},
                {"name": "Google Network", "value_bn": 4.900},
                {"name": "Google other", "value_bn": 4.640},
                {"name": "Other Bets", "value_bn": 0.146},
            ],
            "current_geographies": [
                {"name": "United States", "value_bn": 12.930},
                {"name": "EMEA", "value_bn": 9.097},
                {"name": "Asia Pacific", "value_bn": 4.199},
                {"name": "Americas Excluding U.S.", "value_bn": 1.546},
            ],
        }
        normalized_segments = _normalize_segment_items(get_company("alphabet"), parsed["current_segments"])
        self.assertEqual(
            [item["name"] for item in normalized_segments],
            ["Google properties", "Google Network", "Google subscriptions, platforms, and devices", "Other Bets"],
        )
        self.assertEqual(
            [item["name"] for item in parsed["current_geographies"]],
            ["United States", "EMEA", "Asia Pacific", "Americas Excluding U.S."],
        )
        self.assertNotEqual(
            [item["name"] for item in normalized_segments],
            [item["name"] for item in parsed["current_geographies"][:4]],
        )

    @patch("app.services.official_source_resolver._fetch_text")
    def test_discover_default_sitemap_urls_uses_robots_and_common_paths(self, mock_fetch_text: object) -> None:
        def fake_fetch(url: str) -> str:
            if url == "https://abc.xyz/robots.txt":
                return "User-agent: *\nSitemap: https://abc.xyz/sitemap.xml\nSitemap: https://abc.xyz/investor/sitemap-news.xml\n"
            raise RuntimeError("unexpected fetch")

        mock_fetch_text.side_effect = fake_fetch
        urls = _discover_default_sitemap_urls("https://abc.xyz/investor/")
        self.assertIn("https://abc.xyz/sitemap.xml", urls)
        self.assertIn("https://abc.xyz/investor/sitemap.xml", urls)
        self.assertIn("https://abc.xyz/investor/sitemap-news.xml", urls)

    @patch("app.services.official_source_resolver._fetch_text")
    def test_discover_sitemap_sources_expands_nested_sitemap_index(self, mock_fetch_text: object) -> None:
        sitemap_index = """
        <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <sitemap><loc>https://example.com/investor-news.xml</loc></sitemap>
        </sitemapindex>
        """
        sitemap_leaf = """
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url><loc>https://example.com/investor/news/2021/0202/alphabet-announces-fourth-quarter-and-fiscal-year-2020-results/</loc></url>
          <url><loc>https://example.com/investor/events/2021/q4-2020-earnings-call-webcast/</loc></url>
        </urlset>
        """

        def fake_fetch(url: str) -> str:
            if url == "https://example.com/sitemap.xml":
                return sitemap_index
            if url == "https://example.com/investor-news.xml":
                return sitemap_leaf
            raise RuntimeError(f"unexpected fetch: {url}")

        mock_fetch_text.side_effect = fake_fetch
        company = get_company("alphabet")
        discovered = _discover_sitemap_sources(company, "2020Q4", "2020-12-31", "https://example.com/sitemap.xml")
        roles = {str(item.get("role") or "") for item in discovered}
        self.assertIn("earnings_release", roles)
        self.assertIn("earnings_call", roles)

    @patch("app.services.official_source_resolver._fetch_text")
    def test_discover_sitemap_sources_supports_json_link_inventory(self, mock_fetch_text: object) -> None:
        mock_fetch_text.return_value = json.dumps(
            {
                "urls": [
                    {"url": "https://example.com/news/q4-2025-financial-results.html"},
                    {"url": "https://example.com/events/q4-2025-earnings-call-replay/default.aspx"},
                    {"url": "https://example.com/files/q4-2025-earnings-presentation.pdf"},
                ]
            }
        )

        company = {
            "id": "example-json-sitemap",
            "ticker": "EXM",
            "english_name": "Example Co.",
            "official_source": {"fiscal_year_end_month": 12},
        }

        discovered = _discover_sitemap_sources(company, "2025Q4", "2025-12-31", "https://example.com/sitemap.json", refresh=True)
        roles = {str(item.get("role") or ""): str(item.get("url") or "") for item in discovered}

        self.assertEqual(roles["earnings_release"], "https://example.com/news/q4-2025-financial-results.html")
        self.assertEqual(roles["earnings_call"], "https://example.com/events/q4-2025-earnings-call-replay/default.aspx")
        self.assertEqual(roles["earnings_presentation"], "https://example.com/files/q4-2025-earnings-presentation.pdf")

    @patch("app.services.official_source_resolver._fetch_text")
    def test_discover_sitemap_sources_reuses_cached_link_inventory(self, mock_fetch_text: object) -> None:
        source_resolver.SITEMAP_LINKS_MEMORY_CACHE.clear()
        self.addCleanup(source_resolver.SITEMAP_LINKS_MEMORY_CACHE.clear)
        cache_path = self._backup_root / "alphabet-sitemap-cache.json"
        sitemap_index = """
        <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <sitemap><loc>https://example.com/investor-news.xml</loc></sitemap>
        </sitemapindex>
        """
        sitemap_leaf = """
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url><loc>https://example.com/investor/news/2021/0202/alphabet-announces-fourth-quarter-and-fiscal-year-2020-results/</loc></url>
          <url><loc>https://example.com/investor/events/2021/q4-2020-earnings-call-webcast/</loc></url>
        </urlset>
        """

        def fake_fetch(url: str) -> str:
            if url == "https://example.com/sitemap.xml":
                return sitemap_index
            if url == "https://example.com/investor-news.xml":
                return sitemap_leaf
            raise RuntimeError(f"unexpected fetch: {url}")

        mock_fetch_text.side_effect = fake_fetch
        company = get_company("alphabet")
        with patch("app.services.official_source_resolver._sitemap_cache_path", return_value=cache_path):
            first = _discover_sitemap_sources(company, "2020Q4", "2020-12-31", "https://example.com/sitemap.xml")
            self.assertTrue(first)
            self.assertEqual(mock_fetch_text.call_count, 2)
            second = _discover_sitemap_sources(company, "2020Q4", "2020-12-31", "https://example.com/sitemap.xml")

        self.assertEqual(mock_fetch_text.call_count, 2)
        self.assertEqual(first, second)

    def test_quarter_fallback_for_structure_keeps_metric_baseline(self) -> None:
        fallback = _quarter_fallback_for_structure(
            {
                "quarter_label": "2019Q3",
                "fiscal_label": "2019Q3",
                "period_end": "2019-09-30",
                "revenue_bn": 9.302,
                "net_income_bn": 3.171,
                "gross_margin_pct": 47.6,
                "revenue_yoy_pct": 12.6,
                "net_income_yoy_pct": 13.5,
            },
            [{"url": "https://example.com/tsmc-q3"}],
        )
        self.assertEqual(fallback["latest_kpis"]["revenue_bn"], 9.302)
        self.assertEqual(fallback["latest_kpis"]["net_income_bn"], 3.171)
        self.assertEqual(fallback["latest_kpis"]["gaap_gross_margin_pct"], 47.6)

    def test_parse_official_materials_tsmc_can_use_presentation_when_release_is_unusable(self) -> None:
        presentation_path = self._write_temp_text(
            "tsmc-quarterly-presentation.txt",
            (
                "Taiwan Semiconductor Manufacturing Company Limited and Subsidiaries "
                "CONSOLIDATED STATEMENTS OF COMPREHENSIVE INCOME "
                "For the Three Months Ended September 30 2019 2018 "
                "NET REVENUE $ 293,045,439 100 $ 260,347,882 100 "
                "COST OF REVENUE 153,613,278 52 136,967,039 53 "
                "GROSS PROFIT 139,412,434 48 123,366,640 47 "
                "INCOME FROM OPERATIONS 107,887,292 37 95,245,181 37 "
                "NET INCOME 101,102,454 35 89,098,072 34 "
                "Diluted earnings per share $ 3.90 $ 3.44 "
                "Geography Taiwan 23,868,865 21,364,902 11.7% "
                "United States 174,062,231 161,146,948 8.0% "
                "China 57,798,771 40,021,498 44.4% "
                "Japan 15,442,881 15,349,531 0.6% "
                "Others 4,554,210 4,869,651 (6.5)% "
                "Platform Smartphone 144,301,200 117,666,462 22.6% "
                "High Performance Computing 85,300,705 84,945,146 0.4% "
                "Internet of Things 25,790,650 17,015,261 51.6% "
                "Automotive 13,143,575 13,290,261 (1.1)% "
                "Digital Consumer Electronics 14,078,762 14,830,746 (5.1)% "
                "Others 10,430,547 12,599,868 (17.2)% "
            ),
        )
        unusable_release_path = self._write_temp_text(
            "tsmc-unusable-release.txt",
            "TSMC Property NYSE Section 303A corporate governance practices and annual report references only.",
        )

        parsed = parse_official_materials(
            get_company("tsmc"),
            {
                "fiscal_label": "2019Q3",
                "calendar_quarter": "2019Q3",
                "period_end": "2019-09-30",
                "coverage_months": ["2019-07", "2019-08", "2019-09"],
                "latest_kpis": {
                    "revenue_bn": 9.302,
                    "net_income_bn": 3.171,
                    "revenue_yoy_pct": 12.6,
                    "net_income_yoy_pct": 13.5,
                },
            },
            [
                {"label": "TSMC NYSE Section 303A", "kind": "official_release", "status": "cached", "text_path": unusable_release_path},
                {"label": "TSMC earnings presentation", "kind": "presentation", "status": "cached", "text_path": presentation_path},
            ],
        )

        self.assertTrue(parsed["current_segments"])
        self.assertEqual(parsed["current_segments"][0]["name"], "HPC")
        self.assertTrue(parsed["current_geographies"])
        self.assertEqual(parsed["current_geographies"][0]["name"], "United States")
        self.assertAlmostEqual(parsed["latest_kpis"]["gaap_gross_margin_pct"], 47.57, places=1)
        self.assertAlmostEqual(parsed["latest_kpis"]["gaap_eps"], 3.9, places=1)

    def test_parse_official_materials_tsmc_extracts_ending_equity_from_balance_sheet_presentation(self) -> None:
        presentation_path = self._write_temp_text(
            "tsmc-quarterly-presentation-equity.txt",
            (
                "Balance Sheets & Key Indices "
                "Selected Items from Balance Sheets "
                "(In NT$ billions) Amount % Amount % Amount % "
                "Total Assets 7,006.35 100.0% 7,133.29 100.0% 5,982.36 100.0% "
                "Total Liabilities 2,389.72 34.1% 2,531.66 35.5% 2,162.22 36.1% "
                "Total Shareholders' Equity 4,616.63 65.9% 4,601.63 64.5% 3,820.14 63.9% "
            ),
        )

        parsed = parse_official_materials(
            get_company("tsmc"),
            {
                "fiscal_label": "2025Q2",
                "calendar_quarter": "2025Q2",
                "period_end": "2025-06-30",
                "coverage_months": ["2025-04", "2025-05", "2025-06"],
                "latest_kpis": {"revenue_bn": 30.07, "net_income_bn": 12.83},
            },
            [
                {"label": "TSMC earnings presentation", "kind": "presentation", "status": "cached", "text_path": presentation_path},
            ],
        )

        self.assertAlmostEqual(parsed["latest_kpis"]["ending_equity_bn"], 4_616.63, places=2)

    def test_parse_official_materials_tsmc_legacy_application_mix_maps_to_platform_segments(self) -> None:
        presentation_path = self._write_temp_text(
            "tsmc-legacy-application-presentation.txt",
            (
                "4Q18 Revenue by Application "
                "Consumer 5% Industrial/Standard 20% Computer 11% Communication 64% "
                "Communication Computer Consumer Industrial/Standard "
                "2018 Revenue by Application Growth rate by application (YoY) "
                "Consumer 7% 23% Computer 14% Communication 56% "
            ),
        )

        parsed = parse_official_materials(
            get_company("tsmc"),
            {
                "fiscal_label": "2018Q4",
                "calendar_quarter": "2018Q4",
                "period_end": "2018-12-31",
                "coverage_months": ["2018-10", "2018-11", "2018-12"],
                "latest_kpis": {"revenue_bn": 9.4, "net_income_bn": 3.24, "revenue_yoy_pct": 10.7},
            },
            [
                {"label": "TSMC earnings presentation", "kind": "presentation", "status": "cached", "text_path": presentation_path},
            ],
        )

        segment_names = [item["name"] for item in parsed["current_segments"]]
        self.assertEqual(
            segment_names,
            ["HPC", "Smartphone", "Internet of Things", "Automotive", "DCE", "Others"],
        )
        self.assertAlmostEqual(sum(float(item["value_bn"]) for item in parsed["current_segments"]), 9.4, places=1)

    def test_parse_official_materials_jnj_extracts_segment_and_geography_tables_from_supplement(self) -> None:
        release_path = self._write_temp_text(
            "jnj-release.txt",
            (
                "JOHNSON & JOHNSON REPORTS 2023 SECOND-QUARTER RESULTS: "
                "2023 Second-Quarter reported sales growth of 6.3% to $25.5 Billion. "
                "Earnings per share (EPS) of $1.96 increasing 8.9%. "
                "Estimated Reported Sales $98.8B - $99.8B / $99.3B."
            ),
        )
        supplement_path = self._write_temp_text(
            "jnj-supplement.txt",
            (
                "Johnson & Johnson and Subsidiaries\n"
                "Supplementary Sales Data\n"
                "(Unaudited; Dollars in Millions)\n"
                "SECOND QUARTER\n"
                "Percent Change\n"
                "2023\n"
                "2022\n"
                "Total\n"
                "Operations\n"
                "Currency\n"
                "Sales to customers by\n"
                "geographic area\n"
                "U.S.\n$\n13,444\n12,197\n10.2\n%\n10.2\n—\n"
                "Europe\n5,894\n6,085\n(3.1)\n(3.9)\n0.8\n"
                "Western Hemisphere excluding U.S.\n1,713\n1,536\n11.5\n17.7\n(6.2)\n"
                "Asia-Pacific, Africa\n4,479\n4,202\n6.6\n12.5\n(5.9)\n"
                "International\n12,086\n11,823\n2.2\n4.7\n(2.5)\n"
                "Worldwide\n$\n25,530\n24,020\n6.3\n%\n7.5\n(1.2)\n"
                "Note: Percentages have been calculated using actual, non-rounded figures.\n"
                "Johnson & Johnson and Subsidiaries\n"
                "Supplementary Sales Data\n"
                "(Unaudited; Dollars in Millions)\n"
                "SECOND QUARTER\n"
                "Percent Change\n"
                "2023\n"
                "2022\n"
                "Total\n"
                "Operations\n"
                "Currency\n"
                "Sales to customers by\n"
                "segment of business\n"
                "Consumer Health\nU.S.\n$\n1,787\n1,687\n6.0\n%\n6.0\n—\nInternational\n2,224\n2,118\n5.0\n9.0\n(4.0)\n4,011\n3,805\n5.4\n7.7\n(2.3)\n"
                "Pharmaceutical\n(1)\nU.S.\n7,818\n7,159\n9.2\n9.2\n—\nInternational\n5,913\n6,158\n(4.0)\n(2.5)\n(1.5)\n13,731\n13,317\n3.1\n3.8\n(0.7)\n"
                "Pharmaceutical excluding COVID-19 Vaccine\n(1)\nU.S.\n7,818\n7,114\n9.9\n9.9\n—\nInternational\n5,628\n5,659\n(0.5)\n1.5\n(2.0)\n13,446\n12,773\n5.3\n6.2\n(0.9)\n"
                "MedTech\nU.S.\n3,839\n3,351\n14.6\n14.6\n—\nInternational\n3,949\n3,547\n11.3\n14.7\n(3.4)\n7,788\n6,898\n12.9\n14.7\n(1.8)\n"
                "U.S.\n13,444\n12,197\n10.2\n10.2\n—\nInternational\n12,086\n11,823\n2.2\n4.7\n(2.5)\nWorldwide\n25,530\n24,020\n6.3\n7.5\n(1.2)\n"
                "Note: Percentages have been calculated using actual, non-rounded figures.\n"
            ),
        )

        parsed = parse_official_materials(
            get_company("jnj"),
            {"fiscal_label": "2023Q2", "calendar_quarter": "2023Q2", "coverage_notes": []},
            [
                {"label": "Johnson & Johnson earnings release", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "Johnson & Johnson a2023q2exhibit992.htm", "kind": "presentation", "role": "earnings_commentary", "status": "cached", "text_path": supplement_path},
            ],
        )

        self.assertEqual(
            sorted(item["name"] for item in parsed["current_segments"]),
            ["Consumer Health", "MedTech", "Pharmaceutical"],
        )
        self.assertEqual(
            [item["name"] for item in parsed["current_geographies"]],
            ["U.S.", "Europe", "Western Hemisphere excluding U.S.", "Asia-Pacific, Africa"],
        )
        segment_map = {item["name"]: item["value_bn"] for item in parsed["current_segments"]}
        self.assertAlmostEqual(segment_map["Consumer Health"], 4.011, places=3)
        self.assertAlmostEqual(segment_map["Pharmaceutical"], 13.731, places=3)
        self.assertAlmostEqual(segment_map["MedTech"], 7.788, places=3)

    def test_parse_official_materials_jnj_historical_quarter_uses_matching_quarter_supplement_table(self) -> None:
        release_path = self._write_temp_text(
            "jnj-release-q1.txt",
            (
                "JOHNSON & JOHNSON REPORTS 2021 FIRST-QUARTER RESULTS: "
                "reported sales growth of 7.9% to $22.3 Billion. "
                "Earnings per share (EPS) of $2.10 increasing 34.6%."
            ),
        )
        supplement_path = self._write_temp_text(
            "jnj-supplement-q1.txt",
            (
                "Johnson & Johnson and Subsidiaries\n"
                "Supplementary Sales Data\n"
                "(Unaudited; Dollars in Millions)\n"
                "FIRST QUARTER\n"
                "Percent Change\n"
                "2021\n"
                "2020\n"
                "Total\n"
                "Operations\n"
                "Currency\n"
                "Sales to customers by\n"
                "geographic area\n"
                "U.S.\n11,111\n10,699\n3.9\n%\n3.9\n—\n"
                "Europe\n5,414\n4,827\n12.1\n4.7\n7.4\n"
                "Western Hemisphere excluding U.S.\n1,424\n1,502\n(5.1)\n0.0\n(5.1)\n"
                "Asia-Pacific, Africa\n4,372\n3,663\n19.4\n13.7\n5.7\n"
                "International\n11,210\n9,992\n12.2\n7.3\n4.9\n"
                "Worldwide\n22,321\n20,691\n7.9\n5.5\n2.4\n"
                "Note: Percentages have been calculated using actual, non-rounded figures.\n"
                "Johnson & Johnson and Subsidiaries\n"
                "Supplementary Sales Data\n"
                "(Unaudited; Dollars in Millions)\n"
                "FIRST QUARTER\n"
                "Percent Change\n"
                "2021\n"
                "2020\n"
                "Total\n"
                "Operations\n"
                "Currency\n"
                "Sales to customers by\n"
                "segment of business\n"
                "Consumer Health\nU.S.\n$\n1,611\n1,740\n(7.4)\n%\n(7.4)\n—\nInternational\n1,932\n1,885\n2.5\n0.5\n2.0\n3,543\n3,625\n(2.3)\n(3.3)\n1.0\n"
                "Pharmaceutical\nU.S.\n6,446\n6,061\n6.4\n6.4\n—\nInternational\n5,753\n5,073\n13.4\n7.9\n5.5\n12,199\n11,134\n9.6\n7.1\n2.5\n"
                "Medical Devices\nU.S.\n3,054\n2,898\n5.4\n5.4\n—\nInternational\n3,525\n3,034\n16.2\n10.5\n5.7\n6,579\n5,932\n10.9\n8.0\n2.9\n"
                "U.S.\n11,111\n10,699\n3.9\n3.9\n—\nInternational\n11,210\n9,992\n12.2\n7.3\n4.9\nWorldwide\n22,321\n20,691\n7.9\n5.5\n2.4\n"
                "Note: Percentages have been calculated using actual, non-rounded figures.\n"
            ),
        )

        parsed = parse_official_materials(
            get_company("jnj"),
            {"fiscal_label": "2021Q1", "calendar_quarter": "2021Q1", "coverage_notes": []},
            [
                {"label": "Johnson & Johnson earnings release", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "Johnson & Johnson a2021q1exhibit992.htm", "kind": "presentation", "role": "earnings_commentary", "status": "cached", "text_path": supplement_path},
            ],
        )

        segment_map = {item["name"]: item["value_bn"] for item in parsed["current_segments"]}
        geography_map = {item["name"]: item["value_bn"] for item in parsed["current_geographies"]}
        self.assertAlmostEqual(segment_map["Consumer Health"], 3.543, places=3)
        self.assertAlmostEqual(segment_map["Pharmaceutical"], 12.199, places=3)
        self.assertAlmostEqual(segment_map["MedTech"], 6.579, places=3)
        self.assertAlmostEqual(geography_map["U.S."], 11.111, places=3)
        self.assertAlmostEqual(geography_map["Europe"], 5.414, places=3)
        self.assertAlmostEqual(geography_map["Western Hemisphere excluding U.S."], 1.424, places=3)
        self.assertAlmostEqual(geography_map["Asia-Pacific, Africa"], 4.372, places=3)
        self.assertAlmostEqual(parsed["latest_kpis"]["revenue_bn"], 22.3, places=1)

    def test_parse_official_materials_jnj_accepts_single_line_supplement_headers(self) -> None:
        release_path = self._write_temp_text(
            "jnj-release-q4.txt",
            (
                "JOHNSON & JOHNSON REPORTS 2025 FOURTH-QUARTER RESULTS: "
                "sales growth of 9.1% to $24.6 Billion. "
                "Earnings per share (EPS) of $2.10 increasing 11.1%."
            ),
        )
        supplement_path = self._write_temp_text(
            "jnj-supplement-q4.txt",
            (
                "Johnson & Johnson and subsidiaries\n"
                "Supplementary sales data\n"
                "(Unaudited; Dollars in Millions)\n"
                "FOURTH QUARTER\n"
                "Percent Change\n"
                "Sales to customers by geographic area\n"
                "2025\n"
                "2024\n"
                "Total\n"
                "Operations\n"
                "Currency\n"
                "U.S.\n$\n14,195\n13,204\n7.5\n%\n7.5\n—\n"
                "Europe\n5,598\n4,921\n13.8\n5.2\n8.6\n"
                "Western Hemisphere excluding U.S.\n1,271\n1,135\n12.0\n11.0\n1.0\n"
                "Asia-Pacific, Africa\n3,500\n3,260\n7.4\n7.2\n0.2\n"
                "International\n10,369\n9,316\n11.3\n6.6\n4.7\n"
                "Worldwide\n$\n24,564\n22,520\n9.1\n%\n7.1\n2.0\n"
                "Note: Percentages have been calculated using actual, non-rounded figures.\n"
                "Johnson & Johnson and subsidiaries\n"
                "Supplementary sales data\n"
                "(Unaudited; Dollars in Millions)\n"
                "FOURTH QUARTER\n"
                "Percent Change\n"
                "Sales to customers by segment of business\n"
                "2025\n"
                "2024\n"
                "Total\n"
                "Operations\n"
                "Currency\n"
                "Innovative Medicine\nU.S.\n$\n9,689\n8,977\n7.9\n%\n7.9\n—\nInternational\n6,074\n5,355\n13.4\n7.9\n5.5\n15,763\n14,332\n10.0\n7.9\n2.1\n"
                "MedTech\nU.S.\n4,506\n4,227\n6.6\n6.6\n—\nInternational\n4,295\n3,961\n8.5\n4.9\n3.6\n8,801\n8,188\n7.5\n5.8\n1.7\n"
                "Worldwide\n$\n24,564\n22,520\n9.1\n%\n7.1\n2.0\n"
                "Note: Percentages have been calculated using actual, non-rounded figures.\n"
            ),
        )

        parsed = parse_official_materials(
            get_company("jnj"),
            {"fiscal_label": "2025Q4", "calendar_quarter": "2025Q4", "coverage_notes": []},
            [
                {"label": "Johnson & Johnson earnings release", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "Johnson & Johnson a2025q4exhibit992.htm", "kind": "presentation", "role": "earnings_commentary", "status": "cached", "text_path": supplement_path},
            ],
        )

        segment_map = {item["name"]: item["value_bn"] for item in parsed["current_segments"]}
        geography_map = {item["name"]: item["value_bn"] for item in parsed["current_geographies"]}
        self.assertAlmostEqual(segment_map["Pharmaceutical"], 15.763, places=3)
        self.assertAlmostEqual(segment_map["MedTech"], 8.801, places=3)
        self.assertAlmostEqual(geography_map["U.S."], 14.195, places=3)
        self.assertAlmostEqual(geography_map["Europe"], 5.598, places=3)
        self.assertAlmostEqual(geography_map["Western Hemisphere excluding U.S."], 1.271, places=3)
        self.assertAlmostEqual(geography_map["Asia-Pacific, Africa"], 3.5, places=3)

    def test_parse_official_materials_alphabet_historical_prefers_detailed_geographies(self) -> None:
        release_path = self._write_temp_text(
            "alphabet-historical-release.txt",
            (
                "Google Search & other 17,014 14,717 16% "
                "YouTube ads 4,717 4,717 31% "
                "Google Network Members' properties 5,212 5,212 8% "
                "Google Cloud 2,614 2,614 53% "
                "Google other 5,264 5,264 26% "
                "Other Bets revenues 172 172 -9% "
                "EMEA revenues (GAAP) $ 14,099 $ 14,099 "
                "Prior period EMEA revenues (GAAP) $ 12,251 $ 12,565 "
                "EMEA revenue growth (GAAP) 15 % 12 % "
                "APAC revenues (GAAP) $ 7,482 $ 7,482 "
                "Prior period APAC revenues (GAAP) $ 6,031 $ 6,814 "
                "APAC revenue growth (GAAP) 24 % 10 % "
                "Other Americas revenues (GAAP) $ 2,666 $ 2,666 "
                "Prior period Other Americas revenues (GAAP) $ 2,201 $ 2,290 "
                "Other Americas revenue growth (GAAP) 21 % 16 % "
                "United States revenues (GAAP) $ 21,737 $ 21,737 "
                "United States revenue growth (GAAP) 16 % 16 % "
            ),
        )
        sec_path = self._write_temp_text("alphabet-2019-10k.txt", "placeholder annual filing")

        parsed = parse_official_materials(
            get_company("alphabet"),
            {
                "fiscal_label": "2019Q4",
                "calendar_quarter": "2019Q4",
                "period_end": "2019-12-31",
                "coverage_months": ["2019-10", "2019-11", "2019-12"],
                "latest_kpis": {"revenue_bn": 46.075, "revenue_yoy_pct": 17.0},
            },
            [
                {"label": "Alphabet historical release", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "Alphabet Form 10-K", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )

        self.assertEqual(
            [item["name"] for item in parsed["current_geographies"]],
            ["United States", "EMEA", "Asia Pacific", "Americas Excluding U.S."],
        )
        self.assertAlmostEqual(parsed["current_geographies"][1]["value_bn"], 14.099, places=3)

    def test_prefer_richer_geographies_upgrades_coarse_split(self) -> None:
        current = [
            {"name": "United States", "value_bn": 10.0},
            {"name": "International", "value_bn": 30.0},
        ]
        candidate = [
            {"name": "United States", "value_bn": 10.0, "scope": "annual_filing"},
            {"name": "EMEA", "value_bn": 12.0, "scope": "annual_filing"},
            {"name": "Asia Pacific", "value_bn": 11.0, "scope": "annual_filing"},
            {"name": "Americas Excluding U.S.", "value_bn": 7.0, "scope": "annual_filing"},
        ]

        preferred = _prefer_richer_geographies(current, candidate, 40.0)

        self.assertEqual([item["name"] for item in preferred], [item["name"] for item in candidate])


class OfficialSourceResolverUnitTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._backup_root = Path(tempfile.mkdtemp(prefix="official-source-tests-"))
        self._data_backup_path = self._backup_root / "data-backup"
        self.addCleanup(self._restore_data_dir)
        self._previous_source_fetch = os.environ.get("EARNINGS_DIGEST_DISABLE_SOURCE_FETCH")
        os.environ["EARNINGS_DIGEST_DISABLE_SOURCE_FETCH"] = "1"
        self.addCleanup(self._restore_source_fetch_env)
        self._previous_full_coverage = os.environ.get("EARNINGS_DIGEST_REQUIRE_FULL_COVERAGE")
        os.environ["EARNINGS_DIGEST_REQUIRE_FULL_COVERAGE"] = "0"
        self.addCleanup(self._restore_full_coverage_env)
        if DATA_DIR.exists():
            shutil.move(str(DATA_DIR), str(self._data_backup_path))
        init_db()
        self.client = TestClient(app)

    def tearDown(self) -> None:
        shutil.rmtree(self._backup_root, ignore_errors=True)

    def _write_temp_text(self, filename: str, content: str) -> str:
        path = self._backup_root / filename
        path.write_text(content, encoding="utf-8")
        return str(path)

    def _restore_data_dir(self) -> None:
        if DATA_DIR.exists():
            shutil.rmtree(DATA_DIR, ignore_errors=True)
        if self._data_backup_path.exists():
            shutil.move(str(self._data_backup_path), str(DATA_DIR))
        shutil.rmtree(self._backup_root, ignore_errors=True)

    def _restore_source_fetch_env(self) -> None:
        if self._previous_source_fetch is None:
            os.environ.pop("EARNINGS_DIGEST_DISABLE_SOURCE_FETCH", None)
            return
        os.environ["EARNINGS_DIGEST_DISABLE_SOURCE_FETCH"] = self._previous_source_fetch

    def _restore_full_coverage_env(self) -> None:
        if self._previous_full_coverage is None:
            os.environ.pop("EARNINGS_DIGEST_REQUIRE_FULL_COVERAGE", None)
            return
        os.environ["EARNINGS_DIGEST_REQUIRE_FULL_COVERAGE"] = self._previous_full_coverage

    def test_ir_temporal_alignment_rejects_wrong_year_link(self) -> None:
        self.assertFalse(
            _ir_temporal_alignment(
                {
                    "url": "https://investors.broadcom.com/news-releases/news-release-details/broadcom-inc-announce-first-quarter-fiscal-year-2026-financial",
                    "text": "Broadcom Inc. to Announce First Quarter Fiscal Year 2026 Financial Results",
                },
                target_years={"2016", "2017"},
                allowed_quarters={1, 4},
            )
        )

    def test_ir_temporal_alignment_rejects_wrong_hyphenated_quarter_link(self) -> None:
        self.assertFalse(
            _ir_temporal_alignment(
                {
                    "url": "https://investors.broadcom.com/news-releases/news-release-details/broadcom-inc-reports-second-quarter-fiscal-year-2026-results",
                    "text": "Read more",
                },
                target_years={"2026"},
                allowed_quarters={1},
            )
        )

    def test_ir_role_keywords_require_real_call_or_presentation_signal(self) -> None:
        announce_link = {
            "url": "https://investors.broadcom.com/news-releases/news-release-details/broadcom-inc-announce-first-quarter-fiscal-year-2026-financial",
            "text": "Broadcom Inc. to Announce First Quarter Fiscal Year 2026 Financial Results",
        }
        self.assertFalse(_ir_role_keywords_match(announce_link, "earnings_call"))
        self.assertFalse(_ir_role_keywords_match(announce_link, "earnings_presentation"))
        self.assertFalse(_ir_role_keywords_match(announce_link, "earnings_release"))

    def test_ir_role_keywords_reject_generic_events_page_for_presentation(self) -> None:
        generic_link = {
            "url": "https://investors.broadcom.com/company-information/events-presentations",
            "text": "Events and Presentations",
        }
        self.assertFalse(_ir_role_keywords_match(generic_link, "earnings_presentation"))

    def test_ir_role_keywords_reject_upcoming_call_event_page(self) -> None:
        upcoming_call_link = {
            "url": "https://investors.broadcom.com/news-releases/news-release-details/broadcom-inc-will-host-first-quarter-fiscal-year-2026-conference-call",
            "text": "Broadcom Inc. Will Host First Quarter Fiscal Year 2026 Conference Call",
        }
        self.assertFalse(_ir_role_keywords_match(upcoming_call_link, "earnings_call"))

    @patch("app.services.official_source_resolver._fetch_text")
    def test_expand_related_ir_sources_recurses_into_event_page_script_links(self, mock_fetch_text: object) -> None:
        def fake_fetch(url: str) -> str:
            if url == "https://example.com/news/q4-2025-results.html":
                return '<html><body><a href="/events/q4-2025-earnings-call/default.aspx">Conference Call Webcast</a></body></html>'
            if url == "https://example.com/events/q4-2025-earnings-call/default.aspx":
                return '<html><body><script>{"transcriptUrl":"/files/q4-2025-prepared-remarks.pdf"}</script></body></html>'
            if url == "https://example.com/files/q4-2025-prepared-remarks.pdf":
                raise RuntimeError("should not fetch binary transcript")
            raise RuntimeError(f"unexpected fetch: {url}")

        mock_fetch_text.side_effect = fake_fetch
        company = {
            "id": "example-related-links",
            "ticker": "EXM",
            "english_name": "Example Co.",
            "official_source": {"fiscal_year_end_month": 12},
        }

        related = _expand_related_ir_sources(
            [
                {
                    "label": "Example Co. Q4 2025 results",
                    "url": "https://example.com/news/q4-2025-results.html",
                    "kind": "official_release",
                    "role": "earnings_release",
                    "date": "2026-02-04",
                }
            ],
            company=company,
            calendar_terms={"q4", "2025"},
            fiscal_terms={"q4 fy2025"},
            target_years={"2025"},
            allowed_quarters={4},
        )

        call_urls = [str(item.get("url") or "") for item in related if str(item.get("role") or "") == "earnings_call"]
        self.assertIn("https://example.com/events/q4-2025-earnings-call/default.aspx", call_urls)
        self.assertIn("https://example.com/files/q4-2025-prepared-remarks.pdf", call_urls)
        prepared_remarks_item = next(item for item in related if str(item.get("url") or "").endswith("q4-2025-prepared-remarks.pdf"))
        self.assertNotIn("{", str(prepared_remarks_item.get("label") or ""))

    @patch("app.services.official_source_resolver._fetch_json_response")
    @patch("app.services.official_source_resolver._fetch_text")
    def test_expand_related_ir_sources_uses_q4_event_feed_from_events_presentations_page(
        self,
        mock_fetch_text: object,
        mock_fetch_json_response: object,
    ) -> None:
        mock_fetch_text.return_value = (
            '<html><head><script src="https://widgets.q4app.com/widgets/q4.api.1.13.5.min.js"></script></head>'
            '<body><div class="module module-event module-event-latest"></div>'
            '<script>$(".module-event-latest").events({showPast:true});</script></body></html>'
        )
        mock_fetch_json_response.return_value = {
            "GetEventListResult": [
                {
                    "Title": "Example Q4 2025 Earnings Conference Call",
                    "LinkToDetailPage": "/events-presentations/event-details/2025/example-q4-2025/default.aspx",
                    "WebCastLink": "",
                    "Attachments": [
                        {
                            "Title": "Transcript",
                            "Url": "https://cdn.example.com/files/q4-2025-transcript.pdf",
                        }
                    ],
                    "EventPresentation": [
                        {
                            "Title": "Example Q4 2025 Presentation",
                            "DocumentPath": "https://cdn.example.com/files/q4-2025-presentation.pdf",
                        }
                    ],
                    "EventPressRelease": [],
                }
            ]
        }

        company = {
            "id": "example-q4-widget",
            "ticker": "EXM",
            "english_name": "Example Co.",
            "official_source": {"fiscal_year_end_month": 12},
        }
        related = _expand_related_ir_sources(
            [
                {
                    "label": "Example events and presentations",
                    "url": "https://example.com/events-presentations/default.aspx",
                    "kind": "presentation",
                    "role": "earnings_presentation",
                    "date": "",
                }
            ],
            company=company,
            calendar_terms={"q4", "2025"},
            fiscal_terms={"q4 fy2025"},
            target_years={"2025"},
            allowed_quarters={4},
            max_depth=1,
        )

        role_to_urls = {}
        for item in related:
            role_to_urls.setdefault(str(item.get("role") or ""), []).append(str(item.get("url") or ""))

        self.assertIn("https://cdn.example.com/files/q4-2025-transcript.pdf", role_to_urls["earnings_call"])
        transcript_item = next(item for item in related if str(item.get("url") or "").endswith("q4-2025-transcript.pdf"))
        self.assertNotIn("events page", str(transcript_item.get("label") or "").lower())

    def test_material_text_quality_issue_accepts_short_real_release_excerpt(self) -> None:
        issue = official_materials._material_text_quality_issue(
            {
                "kind": "official_release",
                "role": "earnings_release",
            },
            title="Quarterly results",
            text=(
                "Revenue was $10.2 billion, up 12% year over year, while net income was $2.4 billion. "
                "Management raised full-year guidance for operating margin."
            ),
            content_type="text/plain",
            suffix=".txt",
        )
        self.assertEqual(issue, "")

    def test_extract_generic_guidance_supports_currency_prefix_and_plus_minus_percent(self) -> None:
        guidance = _extract_generic_guidance(
            {
                "flat_text": "Revenue is expected to be between US$34.6 billion and US$35.8 billion. "
                "For the following quarter, revenue is expected to be $43.0 billion, plus or minus two%.",
            }
        )
        self.assertEqual(guidance["mode"], "official")
        self.assertAlmostEqual(guidance["revenue_low_bn"], 34.6, places=2)
        self.assertAlmostEqual(guidance["revenue_high_bn"], 35.8, places=2)

        tolerance_guidance = _extract_generic_guidance(
            {
                "flat_text": "Revenue is expected to be $43.0 billion, plus or minus two%.",
            }
        )
        self.assertEqual(tolerance_guidance["mode"], "official")
        self.assertAlmostEqual(tolerance_guidance["revenue_bn"], 43.0, places=2)
        self.assertIn("±2%", tolerance_guidance["commentary"])

    def test_extract_quote_cards_accepts_noted_and_filters_operator(self) -> None:
        cards = _extract_quote_cards(
            {
                "label": "Example transcript",
                "raw_text": (
                    "Operator: \"Please stand by while we assemble the queue for today's earnings call and replay details.\" "
                    "Management noted, \"Demand remained strong across AI infrastructure, enterprise refresh, and software attach, "
                    "and we are seeing that momentum carry into the next quarter with improving supply conditions.\" "
                    "CFO remarked: \"Gross margin expanded meaningfully this quarter because mix improved, costs eased, and "
                    "execution across the manufacturing network was more disciplined than a year ago.\""
                )
            }
        )
        self.assertEqual(len(cards), 2)
        self.assertTrue(all(card["speaker"] != "Operator" for card in cards))
        self.assertIn("Management", cards[0]["speaker"])
        self.assertIn("Gross margin expanded", cards[1]["quote"])

    def test_extract_company_segments_uses_dynamic_segment_order_profiles(self) -> None:
        text = (
            "Advertising services revenue increased 18% to $12.3 billion. "
            "Subscription services revenue increased 10% to $9.1 billion. "
            "Online stores revenue was $55.0 billion. "
            "Third-party seller services revenue rose 11% to $36.5 billion."
        )
        segments = _extract_company_segments(
            "amazon",
            [
                {
                    "label": "Amazon release",
                    "kind": "official_release",
                    "raw_text": text,
                    "flat_text": text,
                }
            ],
        )
        names = [item["name"] for item in segments]
        self.assertIn("Advertising services", names)
        self.assertIn("Subscription services", names)
        self.assertIn("Third-party seller services", names)

    def test_extract_company_segments_discovers_generic_text_tables_with_trillion_units(self) -> None:
        text = (
            "Results by Business Segment\n"
            "KRW trillion 4Q24 3Q25 4Q25 QoQ YoY 2024 2025 YoY\n"
            "Total 75.8 86.1 93.8 9%↑ 24%↑ 300.9 333.6 11%↑\n"
            "MX / NW 25.8 34.1 29.3 14%↓ 13%↑ 117.3 129.5 10%↑\n"
            "VD / DA 14.4 13.9 14.8 6%↑ 2%↑ 56.5 57.3 1%↑\n"
            "DS 30.1 33.1 44.0 33%↑ 46%↑ 111.1 130.1 17%↑\n"
            "Harman 3.9 4.0 4.6 16%↑ 17%↑ 14.3 15.8 11%↑\n"
        )
        segments = _extract_company_segments(
            "generic-co",
            [
                {
                    "label": "Generic issuer presentation",
                    "kind": "official_release",
                    "raw_text": text,
                    "flat_text": _flatten_text(text),
                }
            ],
            revenue_bn=93_800.0,
        )
        segment_map = {item["name"]: item["value_bn"] for item in segments}
        self.assertAlmostEqual(segment_map["DS"], 44_000.0, places=3)
        self.assertAlmostEqual(segment_map["MX / NW"], 29_300.0, places=3)
        self.assertAlmostEqual(segment_map["VD / DA"], 14_800.0, places=3)
        self.assertAlmostEqual(segment_map["Harman"], 4_600.0, places=3)
        self.assertAlmostEqual(sum(float(item["value_bn"]) for item in segments), 92_700.0, places=1)

    def test_extract_company_geographies_reads_text_tables_with_header_units(self) -> None:
        text = (
            "Sales to customers by geographic area\n"
            "USD billion 1Q24 4Q24 1Q25 QoQ YoY 2024 2025 YoY\n"
            "North America 9.1 10.4 11.2 8%↑ 23%↑ 35.0 42.0 20%↑\n"
            "Europe 5.0 5.5 6.2 13%↑ 24%↑ 20.2 23.7 17%↑\n"
            "Asia Pacific 7.6 8.3 9.8 18%↑ 29%↑ 31.4 37.9 21%↑\n"
        )
        geographies = _extract_company_geographies(
            "generic-co",
            [
                {
                    "label": "Generic issuer supplement",
                    "kind": "official_release",
                    "raw_text": text,
                    "flat_text": _flatten_text(text),
                }
            ],
            27.2,
        )
        geography_map = {item["name"]: item["value_bn"] for item in geographies}
        self.assertAlmostEqual(geography_map["Asia Pacific"], 9.8, places=3)
        self.assertAlmostEqual(geography_map["North America"], 11.2, places=3)
        self.assertAlmostEqual(geography_map["Europe"], 6.2, places=3)

    @patch.dict(os.environ, {"EARNINGS_DIGEST_DISABLE_SOURCE_FETCH": ""}, clear=False)
    @patch("app.services.official_materials.httpx.Client")
    def test_fetch_material_marks_javascript_shell_as_error(self, mock_http_client: object) -> None:
        response = Mock()
        response.content = b"<html><head><title>Access Denied</title></head><body>Please enable JavaScript to continue.</body></html>"
        response.headers = {"content-type": "text/html; charset=utf-8"}
        response.raise_for_status.return_value = None
        client = Mock()
        client.get.return_value = response
        mock_http_client.return_value.__enter__.return_value = client
        mock_http_client.return_value.__exit__.return_value = False

        source = {
            "label": "Example release",
            "url": "https://example.com/releases/q4-results",
            "kind": "official_release",
            "role": "earnings_release",
            "date": "2025-12-31",
        }
        cache_root = self._backup_root / "materials-cache"
        cache_root.mkdir(parents=True, exist_ok=True)

        with patch("app.services.official_materials._source_cache_dir", return_value=cache_root):
            material = official_materials._fetch_material("apple", "2025Q4", source, refresh=True)

        self.assertEqual(material["status"], "error")
        self.assertTrue(
            "access denied" in material["error"] or "enable javascript" in material["error"]
        )
        self.assertTrue(Path(material["text_path"]).exists())

    @patch("app.services.official_source_resolver._attachment_rows")
    @patch("app.services.official_source_resolver._fetch_text")
    @patch("app.services.official_source_resolver._directory_attachment_rows")
    def test_discover_attachment_url_keeps_quarterly_results_wrapper_over_generic_exhibit(
        self,
        mock_directory_rows,
        mock_fetch_text,
        mock_attachment_rows,
    ) -> None:
        mock_directory_rows.return_value = []
        mock_fetch_text.return_value = "<html></html>"
        mock_attachment_rows.return_value = [
            {"href": "d660287dex991.htm", "text": "Press release"},
            {"href": "d660287dex992.htm", "text": "Presentation"},
        ]
        wrapper_url = "https://www.sec.gov/Archives/edgar/data/937966/000119312519014363/form6kq4resultsjanuary2320.htm"
        resolved_url, resolved_text = _discover_attachment_url(
            wrapper_url,
            hints=("q4results", "quarterlyresul", "financialresul", "press release"),
            excludes=("litigation", "settle", "nikon", "zeiss", "presentation", "deck", "slides"),
        )
        self.assertEqual(resolved_url, wrapper_url)
        self.assertEqual(resolved_text, "")

    def test_report_cache_freshness_uses_shorter_ttl_for_recent_quarters(self) -> None:
        dependency = self._backup_root / "dep.txt"
        dependency.write_text("dep", encoding="utf-8")
        old_timestamp = time.time() - 45 * 24 * 60 * 60
        os.utime(dependency, (old_timestamp, old_timestamp))
        recent_record = {
            "updated_at": (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat(),
            "payload": {
                "payload_schema_version": REPORT_PAYLOAD_SCHEMA_VERSION,
                "release_date": date.today().isoformat(),
            },
        }
        historical_record = {
            "updated_at": (datetime.now(timezone.utc) - timedelta(days=20)).isoformat(),
            "payload": {
                "payload_schema_version": REPORT_PAYLOAD_SCHEMA_VERSION,
                "release_date": "2020-01-31",
            },
        }
        with patch("app.services.reports.REPORT_CACHE_DEPENDENCIES", [dependency]):
            self.assertFalse(_report_cache_is_fresh(recent_record))
            self.assertTrue(_report_cache_is_fresh(historical_record))

    def test_report_cache_invalidates_old_payload_schema_version(self) -> None:
        dependency = self._backup_root / "dep-version.txt"
        dependency.write_text("dep", encoding="utf-8")
        timestamp = time.time() - 60
        os.utime(dependency, (timestamp, timestamp))
        record = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "payload": {
                "payload_schema_version": REPORT_PAYLOAD_SCHEMA_VERSION - 1,
                "release_date": "2020-01-31",
            },
        }
        with patch("app.services.reports.REPORT_CACHE_DEPENDENCIES", [dependency]):
            self.assertFalse(_report_cache_is_fresh(record))

    def test_income_statement_snapshot_prefers_detailed_business_segments(self) -> None:
        company = get_company("apple")
        fixture = {
            "fiscal_label": "2018Q4",
            "period_end": "2018-12-29",
            "latest_kpis": {
                "revenue_bn": 84.31,
                "gaap_gross_margin_pct": 38.0,
                "operating_income_bn": 23.3,
                "net_income_bn": 20.0,
                "revenue_yoy_pct": -4.5,
            },
            "current_segments": [
                {"name": "iPhone", "value_bn": 51.982, "yoy_pct": -15.0},
                {"name": "Mac", "value_bn": 7.416, "yoy_pct": 9.0},
                {"name": "iPad", "value_bn": 6.729, "yoy_pct": 17.0},
                {"name": "Wearables, Home and Accessories", "value_bn": 7.308, "yoy_pct": 33.0},
                {"name": "Services", "value_bn": 10.875, "yoy_pct": 19.0},
            ],
            "income_statement": {
                "sources": [
                    {"name": "Products", "value_bn": 73.435, "yoy_pct": -7.2, "margin_pct": 34.3},
                    {"name": "Services", "value_bn": 10.875, "yoy_pct": 19.0, "margin_pct": 62.8},
                ],
                "opex_breakdown": [
                    {"name": "Research and development", "value_bn": 3.902, "pct_of_revenue": 4.6, "color": "#E11D48"},
                    {"name": "Selling, general and administrative", "value_bn": 4.783, "pct_of_revenue": 5.7, "color": "#F43F5E"},
                ],
            },
        }
        history = [
            {
                "quarter_label": "2018Q4",
                "revenue_bn": 84.31,
                "gross_margin_pct": 38.0,
                "net_income_bn": 20.0,
            }
        ]

        snapshot = _build_income_statement_snapshot(company, fixture, history)

        self.assertEqual(
            [item["name"] for item in snapshot["sources"]],
            ["iPhone", "Mac", "iPad", "Wearables, Home and Accessories", "Services"],
        )
        self.assertEqual(
            [item["name"] for item in snapshot["official_sources"]],
            ["Products", "Services"],
        )

    def test_build_historical_quarter_cube_expands_sparse_official_source_periods_to_contiguous_window(self) -> None:
        sparse_periods = [
            "2006Q4",
            "2007Q4",
            "2008Q4",
            "2009Q4",
            "2010Q4",
            "2011Q4",
            "2012Q4",
            "2013Q4",
            "2014Q4",
            "2015Q4",
            "2016Q4",
            "2017Q4",
            "2018Q4",
        ]
        empty_series = {
            "revenue": {},
            "earnings": {},
            "grossMargin": {},
            "revenueGrowth": {},
            "roe": {},
            "equity": {},
            "periodMeta": {},
        }
        history = build_historical_quarter_cube(
            "asml",
            "2018Q4",
            12,
            periods=sparse_periods,
            series=empty_series,
        )
        self.assertEqual([row["quarter_label"] for row in history][:3], ["2016Q1", "2016Q2", "2016Q3"])
        self.assertEqual(history[-1]["quarter_label"], "2018Q4")
        self.assertEqual(len(history), 12)

    def test_company_quarters_service_returns_filtered_window(self) -> None:
        payload = company_quarters("nvidia", 16)
        self.assertEqual(payload["history_window"], 16)
        self.assertTrue(payload["supported_quarters"])
        self.assertNotIn("2005Q4", payload["supported_quarters"])

    def test_company_reference_resolver_accepts_name_ticker_and_chinese(self) -> None:
        self.assertEqual(resolve_company_reference("NVIDIA")["id"], "nvidia")
        self.assertEqual(resolve_company_reference("nvda")["id"], "nvidia")
        self.assertEqual(resolve_company_reference("英伟达")["id"], "nvidia")
        self.assertEqual(resolve_company_reference("Amazon")["id"], "amazon")

    def test_normalize_calendar_quarter_input_accepts_common_formats(self) -> None:
        self.assertEqual(normalize_calendar_quarter_input("2025Q4"), "2025Q4")
        self.assertEqual(normalize_calendar_quarter_input("2025 Q4"), "2025Q4")
        self.assertEqual(normalize_calendar_quarter_input("Q4 2025"), "2025Q4")
        self.assertEqual(normalize_calendar_quarter_input("2025年第4季度"), "2025Q4")

    @patch("app.main.export_html_to_pdf")
    def test_skill_report_api_accepts_company_name_and_quarter(self, mock_export_pdf: object) -> None:
        mock_export_pdf.return_value = "/tmp/nvidia-2025q4.pdf"
        response = self.client.post(
            "/skill/reports",
            json={"company": "NVIDIA", "quarter": "Q4 2025", "history_window": 12},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["company_id"], "nvidia")
        self.assertEqual(payload["ticker"], "NVDA")
        self.assertEqual(payload["calendar_quarter"], "2025Q4")
        self.assertIn("/reports/", payload["preview_url"])
        self.assertIn("/reports/", payload["pdf_download_url"])
        self.assertIsNone(payload["pdf_error"])
        self.assertIsInstance(payload["diagnostics"], list)

    def test_skill_report_api_returns_clear_company_resolution_error(self) -> None:
        response = self.client.post(
            "/skill/reports",
            json={"company": "zzzz-unlisted", "quarter": "2025Q4", "history_window": 12},
        )
        self.assertEqual(response.status_code, 404)
        detail = response.json()["detail"]
        self.assertIn("Unknown company reference", detail["message"])
        self.assertEqual(detail["diagnostics"][0]["code"], "company_not_resolved")
        self.assertFalse(detail["diagnostics"][0]["suggestions"])
        self.assertNotIn("Closest matches", detail["message"])

    def test_skill_report_api_returns_clear_quarter_format_error(self) -> None:
        response = self.client.post(
            "/skill/reports",
            json={"company": "NVDA", "quarter": "quarter twenty five four", "history_window": 12},
        )
        self.assertEqual(response.status_code, 400)
        detail = response.json()["detail"]
        self.assertEqual(detail["diagnostics"][0]["code"], "quarter_not_normalized")
        self.assertTrue(detail["diagnostics"][0]["suggestions"])

    @patch("app.main.create_report", side_effect=RuntimeError("cache directory missing"))
    def test_skill_report_api_wraps_unexpected_runtime_error(self, _mock_create_report: object) -> None:
        response = self.client.post(
            "/skill/reports",
            json={"company": "NVDA", "quarter": "2025Q4", "history_window": 12},
        )
        self.assertEqual(response.status_code, 500)
        detail = response.json()["detail"]
        self.assertEqual(detail["diagnostics"][0]["code"], "unexpected_runtime_error")
        self.assertIn("cache directory missing", detail["message"])

    @patch("app.main.create_report_job")
    def test_skill_report_job_api_accepts_company_name_and_quarter(self, mock_create_report_job: object) -> None:
        mock_create_report_job.return_value = {
            "job_id": "job-123",
            "company_id": "nvidia",
            "calendar_quarter": "2025Q4",
            "history_window": 12,
            "status": "queued",
            "progress": 0.02,
            "stage": "queued",
            "message": "queued",
            "report_id": None,
            "error": None,
            "preview_url": None,
            "export_pdf_url": None,
        }
        response = self.client.post(
            "/skill/report-jobs",
            json={"company": "英伟达", "quarter": "2025年第4季度", "history_window": 12},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["job_id"], "job-123")
        self.assertEqual(payload["company_id"], "nvidia")
        self.assertEqual(payload["ticker"], "NVDA")
        self.assertEqual(payload["calendar_quarter"], "2025Q4")
        self.assertEqual(payload["status"], "queued")
        self.assertIsNone(payload["pdf_download_url"])
        self.assertEqual(payload["diagnostics"], [])

    @patch("app.main.update_report_pdf")
    @patch("app.main.export_html_to_pdf")
    @patch("app.main.render_report_html")
    @patch("app.main.get_report")
    @patch("app.main.get_report_job")
    def test_skill_report_job_status_endpoint_exports_pdf_when_completed(
        self,
        mock_get_report_job: object,
        mock_get_report: object,
        mock_render_report_html: object,
        mock_export_pdf: object,
        mock_update_report_pdf: object,
    ) -> None:
        mock_get_report_job.return_value = {
            "job_id": "job-456",
            "company_id": "nvidia",
            "calendar_quarter": "2025Q4",
            "history_window": 12,
            "status": "completed",
            "progress": 1.0,
            "stage": "completed",
            "message": "done",
            "report_id": "report-456",
            "error": None,
            "preview_url": "/reports/report-456/preview",
            "export_pdf_url": "/reports/report-456/export.pdf",
        }
        mock_get_report.return_value = {
            "id": "report-456",
            "company_id": "nvidia",
            "calendar_quarter": "2025Q4",
            "pdf_path": "",
            "payload": {"company_id": "nvidia", "calendar_quarter": "2025Q4"},
        }
        mock_render_report_html.return_value = "<html>report</html>"
        mock_export_pdf.return_value = "/tmp/report-456.pdf"

        response = self.client.get("/skill/report-jobs/job-456")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "completed")
        self.assertEqual(payload["pdf_download_url"], "/reports/report-456/download.pdf")
        self.assertIsNone(payload["pdf_error"])
        self.assertIsInstance(payload["diagnostics"], list)
        mock_update_report_pdf.assert_called_once_with("report-456", "/tmp/report-456.pdf")

    def test_curated_report_api_round_trip(self) -> None:
        response = self.client.post(
            "/reports",
            json={"company_id": "nvidia", "calendar_quarter": "2025Q4", "history_window": 12},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["structure_dimension_used"], "segment")
        self.assertIn("/reports/", payload["preview_url"])
        self.assertEqual(payload["payload"]["historical_cube"][-1]["quarter_label"], "2025Q4")
        self.assertTrue(payload["payload"]["income_statement"]["opex_breakdown"])

        preview = self.client.get(payload["preview_url"])
        self.assertEqual(preview.status_code, 200)
        self.assertIn("近 12 季成长总览", preview.text)
        self.assertIn("财报科目中文直译", preview.text)

    def test_curated_report_preview_uses_dynamic_history_window_title(self) -> None:
        response = self.client.post(
            "/reports",
            json={"company_id": "nvidia", "calendar_quarter": "2025Q4", "history_window": 16},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()

        preview = self.client.get(payload["preview_url"])
        self.assertEqual(preview.status_code, 200)
        self.assertIn("近 16 季成长总览", preview.text)
        self.assertNotIn("近 12 季成长总览", preview.text)

    def test_report_job_api_round_trip(self) -> None:
        response = self.client.post(
            "/report-jobs",
            json={"company_id": "nvidia", "calendar_quarter": "2025Q4", "history_window": 12, "force_refresh": True},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn(payload["status"], {"queued", "running", "completed"})
        self.assertGreater(payload["progress"], 0)
        self.assertTrue(payload["job_id"])

        terminal = payload
        for _ in range(80):
            if terminal["status"] in {"completed", "failed"}:
                break
            time.sleep(0.1)
            terminal_response = self.client.get(f"/report-jobs/{payload['job_id']}")
            self.assertEqual(terminal_response.status_code, 200)
            terminal = terminal_response.json()

        self.assertEqual(terminal["status"], "completed")
        self.assertEqual(terminal["stage"], "completed")
        self.assertGreaterEqual(terminal["progress"], 1.0)
        self.assertIn("/reports/", terminal["preview_url"])

    def test_generic_local_company_report_uses_deep_template(self) -> None:
        response = self.client.post(
            "/reports",
            json={"company_id": "apple", "calendar_quarter": "2025Q4", "history_window": 12, "force_refresh": True},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["structure_dimension_used"], "management")
        self.assertEqual(payload["payload"]["guidance"]["mode"], "official_context")
        self.assertEqual(payload["payload"]["currency_code"], "USD")
        self.assertIn("官方展望语境", payload["payload"]["guidance_title"])
        self.assertGreaterEqual(len(payload["payload"]["current_detail_cards"]), 3)
        self.assertNotEqual(
            payload["payload"]["current_detail_cards"][0]["title"],
            payload["payload"]["current_detail_cards"][1]["title"],
        )
        self.assertEqual(payload["payload"]["fiscal_label"], "Q1 FY2026")
        self.assertTrue(any("净利润" in item and "同比" in item for item in payload["payload"]["takeaways"][:3]))
        self.assertGreaterEqual(len(payload["payload"]["call_quote_cards"]), 2)
        self.assertGreaterEqual(len(payload["payload"]["income_statement"]["annotations"]), 3)
        self.assertTrue(payload["payload"]["source_materials"])
        self.assertEqual(payload["payload"]["source_materials"][0]["status"], "disabled")
        self.assertTrue(any("自动抓取" in item for item in payload["payload"]["coverage_warnings"]))
        self.assertIn("qna", payload["payload"]["narrative_provenance"])

        preview = self.client.get(payload["preview_url"])
        self.assertEqual(preview.status_code, 200)
        self.assertIn("当前季度与官方展望语境", preview.text)
        self.assertIn("营收与开支可视化图", preview.text)
        self.assertIn("财报科目中文直译", preview.text)
        self.assertIn("官方管理层锚点", preview.text)
        self.assertIn("问答主题来源", preview.text)
        self.assertIn("机构视角参考", preview.text)
        self.assertIn("抓取状态", preview.text)

    def test_meta_report_uses_official_numeric_guidance(self) -> None:
        response = self.client.post(
            "/reports",
            json={"company_id": "meta", "calendar_quarter": "2025Q4", "history_window": 12, "force_refresh": True},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["payload"]["guidance"]["mode"], "official")
        self.assertIn("下一季指引", payload["payload"]["guidance_title"])
        self.assertGreaterEqual(len(payload["payload"]["sources"]), 2)

    @patch("app.services.local_data._fetch_remote_company_series")
    def test_generic_remote_company_report_uses_stubbed_financials(self, mock_fetch: object) -> None:
        mock_fetch.return_value = _stub_remote_payload("USD")
        response = self.client.post(
            "/reports",
            json={"company_id": "walmart", "calendar_quarter": "2025Q4", "history_window": 12, "force_refresh": True},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["payload"]["guidance"]["mode"], "proxy")
        self.assertEqual(payload["payload"]["currency_code"], "USD")
        self.assertIn("结构限制说明", payload["payload"]["historical_insights"][3]["body"])
        self.assertTrue(any("净利润" in item and "同比" in item for item in payload["payload"]["takeaways"][:3]))

    @patch("app.services.local_data._fetch_remote_company_series")
    def test_non_usd_remote_company_keeps_reported_currency(self, mock_fetch: object) -> None:
        mock_fetch.return_value = _stub_remote_payload("EUR")
        record = create_report("asml", "2025Q4", 12, None, True)
        payload = record["payload"]
        self.assertEqual(payload["currency_code"], "EUR")
        self.assertEqual(payload["money_symbol"], "EUR ")
        self.assertTrue(any("EUR" in warning for warning in payload["coverage_warnings"]))
        self.assertTrue(payload["historical_summary_cards"][0]["value"].startswith("EUR "))

    def test_manual_transcript_upload_is_reflected_in_report(self) -> None:
        upload = self.client.post(
            "/uploads",
            files={
                "file": (
                    "transcript.txt",
                    (
                        "Operator\n"
                        "Today we discuss AI demand, guidance, gross margin and software platform momentum.\n"
                        "Management noted strong AI order flow and improving supply."
                    ).encode("utf-8"),
                    "text/plain",
                )
            },
        )
        self.assertEqual(upload.status_code, 200)
        upload_id = upload.json()["upload_id"]

        record = create_report("avgo", "2025Q4", 12, upload_id, True)
        self.assertIn("已应用手动上传 transcript", record["payload"]["coverage_warnings"][0])
        self.assertTrue(record["payload"]["transcript_summary"]["topics"])
        self.assertEqual(record["payload"]["narrative_provenance"]["qna"]["status"], "manual_transcript")
        self.assertEqual(record["payload"]["call_panel"]["title"], "手动 transcript 摘要")

    def test_automatic_transcript_summary_prefers_real_transcript_over_event_page(self) -> None:
        event_path = self._write_temp_text(
            "event-page.txt",
            (
                "Q1 2026 Broadcom Earnings Conference Call Event Details "
                "Listen to Webcast Date / Time 03/04/2026 5:00 PM EST Webcast Presentation"
            ),
        )
        transcript_path = self._write_temp_text(
            "real-transcript.txt",
            (
                "Operator Good afternoon and welcome to the earnings conference call. "
                "Hock Tan, President and CEO Thanks everyone for joining us. AI networking demand remained strong and revenue grew meaningfully year over year. "
                "Kirsten Spears, CFO We expect margins to remain resilient and demand visibility to stay solid. "
                "Question-and-Answer Session Analyst Can you talk about order visibility and software momentum? "
                "Hock Tan, President and CEO We continue to see strong customer demand and improving attach across software platforms."
            ),
        )
        summary = _automatic_transcript_summary(
            [
                {"label": "Broadcom webcast event", "kind": "call_summary", "role": "earnings_call", "status": "cached", "text_path": event_path},
                {"label": "Broadcom earnings transcript", "kind": "call_summary", "role": "earnings_call", "status": "cached", "text_path": transcript_path},
            ]
        )
        self.assertIsNotNone(summary)
        self.assertEqual(summary["source_type"], "official_call_material")
        self.assertIn("transcript", summary["filename"].lower())
        self.assertTrue(summary["highlights"])
        self.assertTrue(summary["topics"])

    def test_automatic_transcript_summary_accepts_prepared_remarks_commentary_material(self) -> None:
        prepared_remarks_path = self._write_temp_text(
            "prepared-remarks.txt",
            (
                "Operator Good afternoon and welcome to the earnings conference call. "
                "Prepared Remarks CEO Thanks for joining us today. Revenue growth accelerated and margins improved sequentially. "
                "CFO We continue to see resilient demand and expect operating margin expansion through the back half of the year. "
                "Question-and-Answer Session Analyst Can you talk about transaction trends and pricing? "
                "CEO We are seeing healthy traffic recovery and continued pricing discipline across key markets."
            ),
        )

        summary = _automatic_transcript_summary(
            [
                {
                    "label": "Example prepared remarks",
                    "kind": "presentation",
                    "role": "earnings_commentary",
                    "status": "cached",
                    "text_path": prepared_remarks_path,
                }
            ]
        )

        self.assertIsNotNone(summary)
        self.assertEqual(summary["source_type"], "official_call_material")
        self.assertIn("prepared remarks", summary["filename"].lower())
        self.assertTrue(summary["highlights"])
        self.assertTrue(summary["topics"])

    def test_automatic_transcript_summary_rejects_webcast_registration_shell(self) -> None:
        webcast_shell_path = self._write_temp_text(
            "webcast-shell.txt",
            (
                "Already Registered? Log In Now Not Registered? Email: Complete this form to enter the webcast. "
                "Complete this form to enter the webcast. First Name Last Name Company Email FAQs and System Test."
            ),
        )

        summary = _automatic_transcript_summary(
            [
                {
                    "label": "Example webcast registration",
                    "kind": "call_summary",
                    "role": "earnings_call",
                    "status": "cached",
                    "text_path": webcast_shell_path,
                }
            ]
        )

        self.assertIsNone(summary)

    def test_transcript_qna_topics_backfill_to_full_coverage_minimum(self) -> None:
        enriched = _ensure_minimum_qna_topics(
            [
                {"label": "AI 需求", "score": 84, "note": "来自自动抓取的官方电话会材料关键词聚类。"},
                {"label": "供给与交付", "score": 36, "note": "来自自动抓取的官方电话会材料关键词聚类。"},
            ],
            [{"label": "资本开支节奏", "score": 78, "note": "管理层继续强调 AI 基础设施投入与产能建设节奏。"}],
            [{"label": "费用爬坡", "score": 72, "note": "费用投入爬坡可能压缩短期利润率弹性。"}],
            [{"label": "商业化兑现", "score": 75, "note": "新产品商业化速度决定后续增长兑现。"}],
        )
        self.assertEqual(len(enriched), 3)
        self.assertEqual(enriched[2]["label"], "延伸关注：资本开支节奏")

    def test_call_panel_requires_real_transcript_before_claiming_call_summary(self) -> None:
        record = create_report("avgo", "2025Q4", 12, None, True)
        payload = record["payload"]
        self.assertIsNone(payload["transcript_summary"])
        self.assertEqual(payload["narrative_provenance"]["qna"]["status"], "official_material_inferred")
        self.assertEqual(payload["call_panel"]["title"], "当前无完整电话会实录，展示推断问答主题")
        self.assertIn("当前没有完整 transcript", payload["section_meta"]["management_qna"]["note"])

    def test_structure_transition_chart_normalizes_each_quarter_to_ratio_view(self) -> None:
        svg = render_structure_transition_svg(
            [
                {
                    "quarter_label": "2024Q1",
                    "segments": [
                        {"name": "A", "value_bn": 60.0, "share_pct": 75.0},
                        {"name": "B", "value_bn": 40.0, "share_pct": 50.0},
                    ],
                    "structure_basis": "segment",
                },
                {
                    "quarter_label": "2024Q2",
                    "segments": [
                        {"name": "A", "value_bn": 55.0, "share_pct": 70.0},
                        {"name": "B", "value_bn": 45.0, "share_pct": 55.0},
                    ],
                    "structure_basis": "segment",
                },
            ],
            {"A": "#111827", "B": "#2563EB"},
            "#2563EB",
        )
        self.assertIn("头部业务分部占比：A 60.0% → A 55.0%", svg)

    @patch("app.services.institutional_views._fetch_rss")
    def test_institutional_views_extract_head_firms(self, mock_fetch_rss: object) -> None:
        mock_fetch_rss.return_value = [
            {
                "title": "Goldman Sachs lifts Microsoft stock price target to $600 before earnings - Example",
                "description": "",
                "link": "https://example.com/goldman-msft",
                "published": "Fri, 17 Jan 2026 12:00:00 GMT",
            },
            {
                "title": "UBS downgrades Microsoft while keeping $510 price target - Example",
                "description": "",
                "link": "https://example.com/ubs-msft",
                "published": "Tue, 20 Jan 2026 12:00:00 GMT",
            },
        ]
        with patch.dict(os.environ, {"EARNINGS_DIGEST_DISABLE_SOURCE_FETCH": "0"}):
            items = get_institutional_views(get_company("microsoft"), "2025Q4", "2026-01-28", refresh=True)
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["firm"], "Goldman Sachs")
        self.assertEqual(items[0]["stance"], "positive")
        self.assertEqual(items[1]["firm"], "UBS")
        self.assertEqual(items[1]["stance"], "negative")

    def test_official_parser_extracts_apple_metrics_and_segments(self) -> None:
        release_path = self._write_temp_text(
            "apple-release.txt",
            (
                "Apple reports first quarter results\n"
                "Apple today announced financial results for its fiscal 2026 first quarter ended December 27, 2025. "
                "The Company posted quarterly revenue of $143.8 billion, up 16 percent year over year. "
                "Diluted earnings per share was $2.84, up 19 percent year over year. "
                "“Today, Apple is proud to report a remarkable, record-breaking quarter, with revenue of $143.8 billion, "
                "up 16 percent from a year ago and well above our expectations,” said Tim Cook, Apple’s CEO. "
                "“These exceptionally strong results generated nearly $54 billion in operating cash flow, allowing us to return "
                "almost $32 billion to shareholders,” said Kevan Parekh, Apple’s CFO."
            ),
        )
        sec_path = self._write_temp_text(
            "apple-sec.txt",
            (
                "Products and Services Performance "
                "iPhone $ 85,269 $ 69,138 23 % "
                "Mac 8,386 8,987 (7) % "
                "iPad 8,595 8,088 6 % "
                "Wearables, Home and Accessories 11,493 11,747 (2) % "
                "Services 30,013 26,340 14 % "
                "Total net sales $ 143,756 $ 124,300 16 % "
                "Americas $ 58,877 $ 50,430 17 % "
                "Europe 33,908 30,397 12 % "
                "Greater China 18,513 20,819 (11) % "
                "Japan 8,987 7,767 16 % "
                "Rest of Asia Pacific 23,471 14,887 58 % "
                "Research and development 10,887 8,268 "
                "Selling, general and administrative 7,492 7,175 "
                "Operating income 50,852 42,832 "
                "Net income $ 42,097 $ 36,330 "
                "Gross margin percentage: Products 40.7 % 39.3 % Services 76.5 % 75.0 % Total gross margin percentage 48.2 % 46.9 % "
            ),
        )
        parsed = parse_official_materials(
            get_company("apple"),
            get_quarter_fixture("apple", "2025Q4"),
            [
                {"label": "Apple release", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "Apple 10-Q", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )
        self.assertAlmostEqual(parsed["latest_kpis"]["revenue_bn"], 143.756, places=3)
        self.assertAlmostEqual(parsed["latest_kpis"]["net_income_bn"], 42.097, places=3)
        self.assertAlmostEqual(parsed["latest_kpis"]["gaap_gross_margin_pct"], 48.2, places=1)
        self.assertEqual(parsed["current_segments"][0]["name"], "iPhone")
        self.assertEqual(parsed["current_geographies"][0]["name"], "Americas")
        self.assertEqual(parsed["guidance"]["mode"], "official_context")
        self.assertGreaterEqual(len(parsed["call_quote_cards"]), 2)

    def test_apple_legacy_parser_uses_sec_tables_without_fake_guidance(self) -> None:
        sec_path = self._write_temp_text(
            "apple-legacy-sec.txt",
            (
                "Total Macintosh net sales 3,500 2,800 25 % "
                "iPod 1,900 1,300 46 % "
                "Other music related products and services 210 160 31 % "
                "iPhone and related products and services 120 0 0 % "
                "Peripherals and other hardware 410 320 28 % "
                "Software, service, and other sales 485 400 21 % "
                "Total net sales 6,625 5,180 28 % "
                "Gross margin 2,420 1,730 40 % "
                "Operating income 1,200 800 50 % "
                "Net income 900 580 55 % "
                "Diluted 1.01 0.65 "
                "Americas 2,800 2,100 33 % Europe 1,900 1,550 23 % Japan 700 600 17 % Retail 825 650 27 % "
                "The Company expects to adopt updated accounting guidance on income tax disclosures under Note 4."
            ),
        )
        parsed = parse_official_materials(
            get_company("apple"),
            {"fiscal_label": "2007Q4", "coverage_notes": []},
            [
                {"label": "Apple Form 10-Q", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )
        segment_names = [item["name"] for item in parsed["current_segments"]]
        self.assertIn("iPod", segment_names)
        self.assertIn("Software, service, and other sales", segment_names)
        self.assertFalse(parsed.get("guidance"))
        self.assertAlmostEqual(parsed["latest_kpis"]["revenue_bn"], 6.625, places=3)

    def test_parser_prefers_quarterly_sec_filing_over_annual_when_both_exist(self) -> None:
        quarterly_path = self._write_temp_text(
            "apple-quarterly-sec.txt",
            (
                "iPhone $ 55,957 $ 51,982 8 % "
                "Mac 7,160 7,416 (3) % "
                "iPad 5,977 6,729 (11) % "
                "Wearables, Home and Accessories 10,010 7,308 37 % "
                "Services 12,715 10,875 17 % "
                "Total net sales $ 91,819 $ 84,310 9 % "
                "Gross margin 35,314 32,031 "
                "Operating income 23,894 23,346 "
                "Net income $ 22,236 $ 19,965 "
                "Diluted 4.99 4.18 "
            ),
        )
        annual_path = self._write_temp_text(
            "apple-annual-sec.txt",
            (
                "iPhone $ 142,381 $ 166,699 (15) % "
                "Mac 25,740 25,484 1 % "
                "iPad 21,280 18,805 13 % "
                "Wearables, Home and Accessories 24,482 17,381 41 % "
                "Services 46,291 39,748 16 % "
                "Total net sales $ 260,174 $ 265,595 (2) % "
                "Gross margin 98,392 101,839 "
                "Operating income 63,930 70,898 "
                "Net income $ 55,256 $ 59,531 "
                "Diluted 11.89 12.01 "
            ),
        )

        parsed = parse_official_materials(
            get_company("apple"),
            {"fiscal_label": "2020Q1", "calendar_quarter": "2019Q3", "coverage_notes": []},
            [
                {"label": "Apple Form 10-K", "kind": "sec_filing", "status": "cached", "text_path": annual_path},
                {"label": "Apple Form 10-Q", "kind": "sec_filing", "status": "cached", "text_path": quarterly_path},
            ],
        )

        self.assertAlmostEqual(parsed["latest_kpis"]["revenue_bn"], 91.819, places=3)
        self.assertAlmostEqual(parsed["latest_kpis"]["net_income_bn"], 22.236, places=3)
        self.assertEqual(parsed["current_segments"][0]["name"], "iPhone")
        self.assertNotEqual(parsed["current_segments"][0]["value_bn"], 142.381)

    @patch("app.services.reports.get_institutional_views")
    @patch("app.services.reports.parse_official_materials")
    @patch("app.services.reports.hydrate_source_materials")
    @patch("app.services.reports.resolve_official_sources")
    def test_build_report_payload_sanitizes_outlier_dynamic_parse(
        self,
        mock_resolve_sources: object,
        mock_hydrate: object,
        mock_parse_materials: object,
        mock_views: object,
    ) -> None:
        mock_resolve_sources.return_value = []
        mock_hydrate.return_value = []
        mock_views.return_value = []
        mock_parse_materials.return_value = {
            "latest_kpis": {
                "revenue_bn": 0.022,
                "net_income_bn": 1.581,
                "gaap_eps": 900.0,
                "gaap_gross_margin_pct": 48.2,
                "revenue_yoy_pct": 35.0,
                "net_income_yoy_pct": 57.4,
            },
            "current_segments": [{"name": "Services", "value_bn": 0.241}],
            "coverage_notes": ["dynamic parsed"],
        }

        payload = build_report_payload("apple", "2007Q4", 12, refresh_source_materials=False)

        self.assertAlmostEqual(payload["latest_kpis"]["revenue_bn"], 6.62475, places=4)
        self.assertIsNone(payload["latest_kpis"]["gaap_eps"])
        self.assertFalse(payload["current_segments"])

    @patch("app.services.reports.hydrate_source_materials")
    def test_build_report_payload_prefers_dynamic_official_parse(self, mock_hydrate: object) -> None:
        release_path = self._write_temp_text(
            "microsoft-release.txt",
            (
                "Earnings Release FY26 Q2 "
                "Revenue was $81.3 billion and increased 17% "
                "Operating income was $38.3 billion and increased 21% "
                "Net income on a GAAP basis was $38.5 billion and increased 60%, and on a non-GAAP basis was $30.9 billion and increased 23% "
                "Diluted earnings per share on a GAAP basis was $5.16 and increased 60%, and on a non-GAAP basis was $4.14 and increased 24% "
                "“We are only at the beginning phases of AI diffusion and already Microsoft has built an AI business that is larger than some of our biggest franchises,” said Satya Nadella, chairman and chief executive officer of Microsoft. "
                "“Microsoft Cloud revenue crossed $50 billion this quarter, reflecting the strong demand for our portfolio of services,” said Amy Hood, executive vice president and chief financial officer of Microsoft. "
                "Microsoft Cloud revenue was $51.5 billion and increased 26% "
                "Revenue in Productivity and Business Processes was $34.1 billion and increased 16% "
                "Revenue in Intelligent Cloud was $32.9 billion and increased 29% "
                "Azure and other cloud services revenue increased 39% "
                "Revenue in More Personal Computing was $14.3 billion and decreased 3% "
            ),
        )
        sec_path = self._write_temp_text(
            "microsoft-sec.txt",
            (
                "Revenue: Product $ 16,451 $ 16,219 Service and other 64,822 53,413 Total revenue 81,273 69,632 "
                "Cost of revenue: Product 3,505 3,856 Service and other 22,473 17,943 Total cost of revenue 25,978 21,799 "
                "Gross margin 55,295 47,833 "
                "Research and development 8,504 7,917 "
                "Sales and marketing 6,584 6,440 "
                "General and administrative 1,932 1,823 "
                "Operating income 38,275 31,653 "
                "Revenue, classified by the major geographic areas in which our customers were located, was as follows: "
                "(In millions) Three Months Ended December 31, Six Months Ended December 31, 2025 2024 2025 2024 "
                "United States (a) $ 41,410 $ 35,537 $ 81,487 $ 69,450 "
                "Other countries 39,863 34,095 77,459 65,767 "
                "Total $ 81,273 $ 69,632 $ 158,946 $ 135,217 "
            ),
        )
        mock_hydrate.return_value = [
            {"label": "Microsoft release", "kind": "official_release", "status": "cached", "text_path": release_path},
            {"label": "Microsoft 10-Q", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
        ]

        payload = build_report_payload("microsoft", "2025Q4", 12, None, False)
        self.assertAlmostEqual(payload["latest_kpis"]["net_income_bn"], 38.5, places=3)
        self.assertAlmostEqual(payload["latest_kpis"]["gaap_eps"], 5.16, places=2)
        self.assertEqual(payload["current_segments"][0]["name"], "Productivity and Business Processes")
        self.assertEqual(payload["current_geographies"][0]["name"], "United States")
        self.assertEqual(payload["call_quote_cards"][0]["speaker"], "Satya Nadella")
        self.assertTrue(any("动态解析" in item for item in payload["coverage_warnings"]))

    @patch.dict(os.environ, {"EARNINGS_DIGEST_DISABLE_SOURCE_FETCH": ""}, clear=False)
    @patch("app.services.official_source_resolver._fetch_json")
    @patch("app.services.official_source_resolver._load_submissions")
    def test_resolve_official_sources_discovers_sec_release_attachment(
        self,
        mock_load_submissions: object,
        mock_fetch_json: object,
    ) -> None:
        mock_load_submissions.return_value = {
            "filings": {
                "recent": {
                    "form": ["8-K", "10-Q", "4"],
                    "filingDate": ["2026-03-10", "2026-03-11", "2026-03-01"],
                    "reportDate": ["2026-03-10", "2026-02-28", "2026-03-01"],
                    "accessionNumber": ["0001193125-26-100148", "0001193125-26-101045", "0000000000-00-000000"],
                    "primaryDocument": ["orcl-20260310.htm", "orcl-20260228.htm", "form4.xml"],
                    "primaryDocDescription": ["8-K", "10-Q", "FORM 4"],
                    "items": ["2.02,8.01,9.01", "", ""],
                }
            }
        }
        mock_fetch_json.return_value = {
            "directory": {
                "item": [
                    {"name": "0001193125-26-100148.txt", "type": "text.gif", "size": ""},
                    {"name": "orcl-ex99_1.htm", "type": "text.gif", "size": "12345"},
                ]
            }
        }

        company = get_company("oracle")
        sources = resolve_official_sources(
            company,
            "2025Q4",
            "2026-02-28",
            [
                {"label": "Oracle investor relations", "url": company["ir_url"], "kind": "investor_relations", "date": "2026-02-28"},
                {
                    "label": "Oracle quarterly financials",
                    "url": "https://stockanalysis.com/stocks/orcl/financials/?p=quarterly",
                    "kind": "structured_financials",
                    "date": "2026-02-28",
                },
            ],
            refresh=True,
        )

        self.assertEqual(sources[0]["kind"], "official_release")
        self.assertTrue(sources[0]["url"].endswith("/orcl-ex99_1.htm"))
        self.assertEqual(sources[1]["kind"], "sec_filing")
        self.assertTrue(sources[1]["url"].endswith("/orcl-20260228.htm"))
        self.assertTrue(any(source["kind"] == "structured_financials" for source in sources))
        self.assertFalse(any(source["kind"] == "investor_relations" for source in sources))

    @patch("app.services.official_source_resolver._fetch_json")
    @patch("app.services.official_source_resolver._load_historical_submissions")
    @patch("app.services.official_source_resolver._load_submissions")
    def test_resolve_official_sources_uses_historical_sec_submission_files(
        self,
        mock_load_submissions: object,
        mock_load_historical_submissions: object,
        mock_fetch_json: object,
    ) -> None:
        mock_load_submissions.return_value = {
            "filings": {
                "recent": {
                    "form": ["4"],
                    "filingDate": ["2026-03-01"],
                    "reportDate": ["2026-03-01"],
                    "accessionNumber": ["0000000000-00-000000"],
                    "primaryDocument": ["form4.xml"],
                    "primaryDocDescription": ["FORM 4"],
                    "items": [""],
                },
                "files": [
                    {
                        "name": "CIK0001045810-submissions-001.json",
                        "filingFrom": "1998-03-06",
                        "filingTo": "2020-01-05",
                    }
                ],
            }
        }
        mock_load_historical_submissions.return_value = {
            "form": ["8-K", "10-K"],
            "filingDate": ["2018-02-08", "2018-02-28"],
            "reportDate": ["2018-01-28", "2018-01-28"],
            "accessionNumber": ["0001045810-18-000004", "0001045810-18-000010"],
            "primaryDocument": ["form8-kq4fy18.htm", "nvda-2018x10k.htm"],
            "primaryDocDescription": ["8-K", "10-K"],
            "items": ["2.02,9.01", ""],
        }
        mock_fetch_json.side_effect = [
            {
                "directory": {
                    "item": [
                        {"name": "q4fy18pr.htm", "type": "text.gif", "size": "12345"},
                        {"name": "q4fy18cfocommentary.htm", "type": "text.gif", "size": "12345"},
                    ]
                }
            },
            {
                "directory": {
                    "item": [
                        {"name": "q4fy18pr.htm", "type": "text.gif", "size": "12345"},
                        {"name": "q4fy18cfocommentary.htm", "type": "text.gif", "size": "12345"},
                    ]
                }
            },
        ]

        company = get_company("nvidia")
        with patch.dict(os.environ, {"EARNINGS_DIGEST_DISABLE_SOURCE_FETCH": "0"}):
            sources = resolve_official_sources(company, "2017Q4", "2018-01-28", [], refresh=False)

        release = next(source for source in sources if str(source.get("url") or "").endswith("/q4fy18pr.htm"))
        commentary = next(source for source in sources if str(source.get("url") or "").endswith("/q4fy18cfocommentary.htm"))
        annual = next(source for source in sources if str(source.get("url") or "").endswith("/nvda-2018x10k.htm"))
        self.assertEqual(release["kind"], "official_release")
        self.assertEqual(commentary["kind"], "presentation")
        self.assertEqual(annual["kind"], "sec_filing")

    def test_visa_parser_extracts_quarter_segments_and_annual_geographies(self) -> None:
        release_path = self._write_temp_text(
            "visa-release.txt",
            (
                "Net revenue in the fiscal fourth quarter was $10.7 billion, an increase of 12%, driven by growth in payments volume. "
                "GAAP net income in the fiscal fourth quarter was $5.1 billion or $2.62 per share, a decrease of 4%. "
                "Fiscal fourth quarter service revenue was $4.6 billion, an increase of 10% over the prior year. "
                "Data processing revenue rose 17% over the prior year to $5.4 billion. "
                "International transaction revenue grew 10% over the prior year to $3.8 billion. "
                "Other revenue of $1.2 billion rose 21% over the prior year. "
                "Client incentives were $4.2 billion, up 17% over the prior year. "
                "Payments volume increased 8%. Cross-border volume excluding transactions within Europe increased 11%."
            ),
        )
        sec_path = self._write_temp_text(
            "visa-sec.txt",
            (
                "For the Years Ended September 30 U.S. $ 15,633 $ 14,780 $ 14,138 6 % 5 % "
                "International 24,367 21,146 18,515 15 % 14 % Net revenue $ 40,000 $ 35,926 $ 32,653 "
            ),
        )

        parsed = parse_official_materials(
            get_company("visa"),
            {"fiscal_label": "2025Q4", "coverage_notes": []},
            [
                {"label": "Visa q42025earningsrelease.htm", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "Visa Form 10-K", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )

        self.assertAlmostEqual(parsed["latest_kpis"]["revenue_bn"], 10.7, places=1)
        self.assertAlmostEqual(parsed["latest_kpis"]["net_income_bn"], 5.1, places=1)
        self.assertEqual(parsed["current_segments"][0]["name"], "Service revenue")
        self.assertEqual(parsed["current_segments"][1]["name"], "Data processing revenue")
        self.assertEqual(len(parsed["current_geographies"]), 2)
        self.assertEqual(parsed["current_geographies"][0]["scope"], "quarterly_mapped_from_official_geography")

    def test_parse_official_materials_prefers_adjusted_profit_for_special_item_quarters(self) -> None:
        release_path = self._write_temp_text(
            "visa-historical-special-items-release.txt",
            (
                "Fiscal Second Quarter 2017 Key Highlights: "
                "• GAAP net income of $430 million or $0.18 per share including special items related to the legal entity reorganization of Visa Europe. "
                "• Adjusted net income of $2.1 billion or $0.86 per share excluding special items related to the legal entity reorganization of Visa Europe. "
                "• Net operating revenue of $4.5 billion, an increase of 23%."
            ),
        )
        sec_path = self._write_temp_text(
            "visa-historical-special-items-sec.txt",
            (
                "Net revenues $ 4,477 $ 3,817 17 % "
                "Net income, as reported $ 430 $ 1,707 (75 )% "
                "Diluted earnings per share, as reported $ 0.18 $ 0.71 (75 )% "
                "Net income, as adjusted (2) $ 2,066 $ 1,626 27 % "
                "Diluted earnings per share, as adjusted (2) $ 0.86 $ 0.68 26 %"
            ),
        )

        parsed = parse_official_materials(
            get_company("visa"),
            {"fiscal_label": "2017Q1", "coverage_notes": []},
            [
                {"label": "Visa q22017 earnings release", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "Visa Form 10-Q", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )

        self.assertAlmostEqual(parsed["latest_kpis"]["revenue_bn"], 4.5, places=1)
        self.assertAlmostEqual(parsed["latest_kpis"]["net_income_bn"], 2.1, places=1)
        self.assertAlmostEqual(parsed["latest_kpis"]["gaap_eps"], 0.18, places=2)
        self.assertAlmostEqual(parsed["latest_kpis"]["non_gaap_eps"], 0.86, places=2)
        self.assertEqual(parsed["profit_basis"], "adjusted_special_items")

    def test_micron_parser_extracts_business_unit_revenue(self) -> None:
        release_path = self._write_temp_text(
            "micron-release.txt",
            (
                "Fiscal Q1 2026 highlights Revenue of $13.64 billion versus $11.32 billion for the prior quarter and $8.71 billion for the same period last year "
                "GAAP net income of $5.24 billion, or $4.60 per diluted share "
                "Operating cash flow of $8.41 billion versus $5.73 billion for the prior quarter and $3.24 billion for the same period last year "
                "Quarterly Financial Results Revenue $ 13,643 $ 11,315 $ 8,709 Gross margin 7,646 5,054 3,348 Operating expenses 1,510 1,400 1,174 "
                "Operating income 6,136 3,654 2,174 Net income 5,240 3,201 1,870 Diluted earnings per share 4.60 2.83 1.67 "
                "Quarterly Business Unit Financial Results "
                "Cloud Memory Business Unit Revenue $ 5,284 $ 4,543 $ 2,648 "
                "Core Data Center Business Unit Revenue $ 2,379 $ 1,577 $ 2,292 "
                "Mobile and Client Business Unit Revenue $ 4,255 $ 3,760 $ 2,608 "
                "Automotive and Embedded Business Unit Revenue $ 1,720 $ 1,434 $ 1,158 "
                "Business Outlook Revenue $18.70 billion ± $400 million Gross margin 67.0% ± 1.0%"
            ),
        )
        annual_sec_path = self._write_temp_text(
            "micron-annual-sec.txt",
            (
                "Note 29. Geographic Information Revenue based on the geographic location of our customers' headquarters was as follows: "
                "For the year ended 2025 2024 2023 "
                "U.S. $ 24,113 $ 13,168 $ 7,805 "
                "Taiwan 5,672 4,708 2,697 "
                "Mainland China (excluding Hong Kong) 2,639 3,045 2,181 "
                "Other Asia Pacific 1,913 1,330 752 "
                "Hong Kong 1,138 1,071 340 "
                "Japan 895 840 987 "
                "Europe 625 818 682 "
                "Other 383 131 96 "
            ),
        )

        parsed = parse_official_materials(
            get_company("micron"),
            {"fiscal_label": "2025Q4", "coverage_notes": []},
            [
                {"label": "Micron a2026q1ex991-pressrelease.htm", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "Micron Form 10-K", "kind": "sec_filing", "status": "cached", "text_path": annual_sec_path},
            ],
        )

        self.assertAlmostEqual(parsed["latest_kpis"]["revenue_bn"], 13.643, places=3)
        self.assertAlmostEqual(parsed["latest_kpis"]["net_income_bn"], 5.24, places=2)
        self.assertEqual(
            [item["name"] for item in parsed["current_segments"][:2]],
            ["Cloud Memory Business Unit", "Core Data Center Business Unit"],
        )
        self.assertAlmostEqual(parsed["guidance"]["revenue_bn"], 18.7, places=1)
        self.assertEqual(parsed["current_geographies"][0]["scope"], "quarterly_mapped_from_official_geography")
        self.assertEqual(parsed["current_geographies"][0]["name"], "U.S.")

    def test_nvidia_parser_extracts_annual_geographies(self) -> None:
        release_path = self._write_temp_text(
            "nvidia-release.txt",
            (
                "Revenue 39.3 22.1 13.5 78 % 191 % Operating income 24.0 12.0 7.0 100 % 243 % "
                "Net income 22.1 12.3 6.5 80 % 240 % Gross margin 73.0 % 70.0 % 69.0 % 3.0 pts 4.0 pts "
                "Diluted earnings per share $0.89 $0.49 $0.26 82 % 242 % "
                "Net cash provided by operating activities 24,100 15,200 9,800 7,200 "
                "Free cash flow $23,500 $14,800 $9,200 $6,800 "
                "Fourth-quarter revenue was a record $35.6 billion, up 16% from the previous quarter and up 93% from a year ago, driven by the major platform shifts - accelerated computing and AI "
                "Fourth-quarter Gaming revenue was $2.5 billion, up 10% from a year ago, driven by strong Blackwell demand, and down 5% from the previous quarter "
                "Professional Visualization Fourth-quarter revenue was $0.5 billion, up 6% from the previous quarter and up 18% from a year ago "
                "Fourth-quarter Automotive revenue was $570 million, up 3% from the previous quarter and up 27% from a year ago "
                "Revenue is expected to be $43.0 billion, plus or minus 2% "
                "GAAP and non-GAAP gross margins are expected to be 72.0% and 75.0% "
            ),
        )
        sec_path = self._write_temp_text(
            "nvidia-sec.txt",
            (
                "Revenue by geographic area is based upon the location of the customers' headquarters. "
                "Geographic Revenue based upon Customer Headquarters Location (1): (In millions) "
                "United States $ 149,617 $ 77,482 $ 31,533 "
                "Taiwan (2) 42,345 23,600 14,912 "
                "China (including Hong Kong) 19,677 25,048 12,330 "
                "Other 4,299 4,367 2,147 "
                "Total revenue $ 215,938 $ 130,497 $ 60,922 "
            ),
        )
        parsed = parse_official_materials(
            get_company("nvidia"),
            {"fiscal_label": "2025Q4", "coverage_notes": []},
            [
                {"label": "NVIDIA earnings release", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "NVIDIA Form 10-K", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )
        self.assertEqual(parsed["current_geographies"][0]["scope"], "quarterly_mapped_from_official_geography")
        self.assertEqual(parsed["current_geographies"][0]["name"], "United States")

    def test_micron_parser_extracts_legacy_business_units_from_quarterly_sec_filing(self) -> None:
        release_path = self._write_temp_text(
            "micron-legacy-release.txt",
            (
                "Micron Technology, Inc. reports results for the third quarter of fiscal 2021. "
                "Revenue of $7.42 billion versus $6.24 billion for the prior quarter and $5.44 billion for the same period last year. "
                "GAAP net income of $1.74 billion, or $1.52 per diluted share."
            ),
        )
        sec_path = self._write_temp_text(
            "micron-legacy-sec.txt",
            (
                "Three months ended June 3, 2021 May 28, 2020 Revenue "
                "CNBU $ 3,304 $ 2,218 "
                "MBU 1,999 1,525 "
                "SBU 1,009 1,014 "
                "EBU 1,105 675 "
                "All Other 5 6 "
                "$ 7,422 $ 5,438 "
            ),
        )

        parsed = parse_official_materials(
            get_company("micron"),
            {"fiscal_label": "2021Q2", "coverage_notes": []},
            [
                {"label": "Micron earnings release", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "Micron Form 10-Q", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )

        segments = {item["name"]: item for item in parsed["current_segments"]}
        self.assertAlmostEqual(segments["Compute and Networking Business Unit"]["value_bn"], 3.304, places=3)
        self.assertAlmostEqual(segments["Mobile Business Unit"]["value_bn"], 1.999, places=3)
        self.assertAlmostEqual(segments["Storage Business Unit"]["value_bn"], 1.009, places=3)
        self.assertAlmostEqual(segments["Embedded Business Unit"]["value_bn"], 1.105, places=3)

    def test_nvidia_legacy_parser_extracts_historical_market_platform_segments(self) -> None:
        release_path = self._write_temp_text(
            "nvidia-legacy-release.txt",
            (
                "Record quarterly revenue of $2.91 billion, up 34 percent from a year ago. "
                "GAAP earnings per diluted share for the quarter were a record $1.78, up 80 percent from $0.99 a year earlier. "
                "Revenue for the quarter was a record for gaming and datacenter. "
                "Revenue is expected to be $2.90 billion, plus or minus two percent. "
                "Jensen Huang said, \"Industries around the world are racing to incorporate AI.\""
            ),
        )
        commentary_path = self._write_temp_text(
            "nvidia-legacy-commentary.txt",
            (
                "Revenue $2,911 $2,636 $2,173 Up 10% Up 34% "
                "Gross margin 61.9% 59.5% 60.0% Up 240 bps Up 190 bps "
                "Operating income $1,073 $895 $733 Up 20% Up 46% "
                "Net income $1,118 $838 $655 Up 33% Up 71% "
                "Diluted earnings per share $1.78 $1.33 $0.99 Up 34% Up 80% "
                "Revenue by Market Platform "
                "Gaming $1,739 $1,561 $1,348 Up 11% Up 29% "
                "Professional Visualization 254 239 225 Up 6% Up 13% "
                "Datacenter 606 501 296 Up 21% Up 105% "
                "Automotive 132 144 128 Down 8% Up 3% "
                "OEM and IP 180 191 176 Down 6% Up 2% "
                "Revenue is expected to be $2.90 billion, plus or minus two percent. "
            ),
        )

        parsed = parse_official_materials(
            get_company("nvidia"),
            {"fiscal_label": "2017Q4", "coverage_notes": []},
            [
                {"label": "NVIDIA press release Q4 FY2018", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "NVIDIA CFO commentary Q4 FY2018", "kind": "presentation", "status": "cached", "text_path": commentary_path},
            ],
        )

        segments = {item["name"]: item for item in parsed["current_segments"]}
        self.assertAlmostEqual(parsed["latest_kpis"]["revenue_bn"], 2.911, places=3)
        self.assertAlmostEqual(parsed["latest_kpis"]["net_income_bn"], 1.118, places=3)
        self.assertAlmostEqual(parsed["latest_kpis"]["net_income_yoy_pct"], 71.0, places=1)
        self.assertAlmostEqual(segments["Gaming"]["value_bn"], 1.739, places=3)
        self.assertAlmostEqual(segments["Data Center"]["value_bn"], 0.606, places=3)
        self.assertAlmostEqual(segments["Data Center"]["yoy_pct"], 105.0, places=1)
        self.assertTrue(any("CFO commentary" in note for note in parsed["coverage_notes"]))

    def test_generic_parser_can_use_commentary_role_without_full_release(self) -> None:
        commentary_path = self._write_temp_text(
            "oracle-commentary.txt",
            (
                "Revenue was $14.1 billion, up 8% year over year. "
                "Net income was $3.4 billion, up 12% year over year. "
                "Cloud revenue grew 21% to $5.6 billion. "
                "Software revenue increased 4% to $7.1 billion. "
                "Hardware revenue was $1.0 billion. "
                "Services revenue was $0.4 billion. "
                "Revenue is expected to be between $14.4 billion and $14.8 billion."
            ),
        )

        parsed = parse_official_materials(
            get_company("oracle"),
            {"fiscal_label": "2025Q4", "coverage_notes": []},
            [
                {
                    "label": "Oracle earnings commentary",
                    "kind": "presentation",
                    "role": "earnings_commentary",
                    "status": "cached",
                    "text_path": commentary_path,
                }
            ],
        )

        self.assertAlmostEqual(parsed["latest_kpis"]["revenue_bn"], 14.1, places=1)
        self.assertAlmostEqual(parsed["latest_kpis"]["net_income_bn"], 3.4, places=1)
        self.assertAlmostEqual(parsed["guidance"]["revenue_bn"], 14.6, places=1)
        self.assertEqual(parsed["current_segments"][0]["name"], "Software")
        self.assertEqual(parsed["current_segments"][1]["name"], "Cloud")

    def test_parse_official_materials_adds_annual_geography_fallback(self) -> None:
        current_path = self._write_temp_text(
            "tsmc-current.txt",
            "Form 20-F yes Form 40-F no Current report on Form 6-K.",
        )
        annual_path = self._write_temp_text(
            "tsmc-annual.txt",
            (
                "North America NT$ 205,255 77.0 % NT$ 247,895 78.1 % NT$ 247,832 76.8 % "
                "Asia 40,785 15.3 % 43,167 13.6 % 45,128 14.0 % "
                "Europe 20,525 7.7 % 26,345 8.3 % 29,670 9.2 % "
            ),
        )

        with patch("app.services.official_parsers._load_nearby_annual_materials") as mock_annual:
            mock_annual.return_value = [
                {"label": "TSMC Form 20-F", "kind": "sec_filing", "status": "cached", "text_path": annual_path}
            ]
            parsed = parse_official_materials(
                get_company("tsmc"),
                {"fiscal_label": "2007Q4", "coverage_notes": []},
                [
                    {"label": "TSMC Form 6-K", "kind": "sec_filing", "status": "cached", "text_path": current_path}
                ],
            )

        self.assertEqual(parsed["current_geographies"][0]["name"], "North America")
        self.assertEqual(len(parsed["current_geographies"]), 3)
        self.assertTrue(any("季度化映射" in note or "官方年报口径" in note for note in parsed["coverage_notes"]))

    def test_avgo_parser_extracts_quarterly_geographies(self) -> None:
        release_path = self._write_temp_text(
            "avgo-release.txt",
            (
                "Broadcom reported revenue of $19.3 billion, up 29% from a year ago. "
                "Net income was $5.5 billion, up 15% year over year. "
                "Diluted earnings per share was $1.12."
            ),
        )
        sec_path = self._write_temp_text(
            "avgo-sec.txt",
            (
                "The following tables present revenue disaggregated by type and by region for the periods presented: "
                "Fiscal Quarter Ended February 1, 2026 Americas Asia Pacific Europe, the Middle East and Africa Total (In millions) "
                "Products $ 2,164 $ 10,950 $ 1,016 $ 14,130 "
                "Subscriptions and services 2,917 665 1,599 5,181 "
                "Total $ 5,081 $ 11,615 $ 2,615 $ 19,311 "
                "Fiscal Quarter Ended February 2, 2025 Americas Asia Pacific Europe, the Middle East and Africa Total (In millions) "
                "Products $ 1,935 $ 7,333 $ 875 $ 10,143 "
                "Subscriptions and services 2,697 699 1,377 4,773 "
                "Total $ 4,632 $ 8,032 $ 2,252 $ 14,916 "
            ),
        )
        parsed = parse_official_materials(
            get_company("avgo"),
            {"fiscal_label": "2025Q4", "coverage_notes": []},
            [
                {"label": "Broadcom earnings release", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "Broadcom Form 10-Q", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )
        self.assertEqual([item["name"] for item in parsed["current_geographies"]], ["Asia Pacific", "Americas", "Europe, the Middle East and Africa"])

    def test_avgo_parser_extracts_annual_geographies_from_fiscal_year_ended_table(self) -> None:
        release_path = self._write_temp_text(
            "avgo-annual-release.txt",
            "Broadcom reported revenue of $22.6 billion and net income of $2.7 billion.",
        )
        sec_path = self._write_temp_text(
            "avgo-annual-sec.txt",
            (
                "The following table presents revenue disaggregated by type of revenue and by region: "
                "Fiscal Year Ended November 3, 2019 Americas Asia Pacific Europe, the Middle East and Africa Total (In millions) "
                "Products $ 2,023 $ 14,857 $ 1,237 $ 18,117 "
                "Subscriptions and services (a) 3,126 374 980 4,480 "
                "Total $ 5,149 $ 15,231 $ 2,217 $ 22,597 "
            ),
        )
        parsed = parse_official_materials(
            get_company("avgo"),
            {"fiscal_label": "2019Q4", "coverage_notes": []},
            [
                {"label": "Broadcom earnings release", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "Broadcom Form 10-K", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )
        self.assertEqual(len(parsed["current_geographies"]), 3)
        self.assertEqual(parsed["current_geographies"][0]["name"], "Asia Pacific")

    def test_temporal_sanitizer_strips_implausible_future_year_narrative_for_historical_quarter(self) -> None:
        sanitized = _sanitize_temporal_narrative_facts(
            {
                "driver": "2026 年 Robotaxi 推进会成为核心估值锚点。",
                "guidance": {"mode": "official_context", "commentary": "公司预计 2026 年继续加大投入。"},
                "management_theme_items": [{"label": "Robotaxi 继续推进", "score": 80, "note": "季度更新把 2026 年 Robotaxi 扩张列为重点。"}],
                "quotes": [
                    {
                        "speaker": "Management",
                        "quote": "We expect 2026 to be a breakout year.",
                        "analysis": "这是对 2026 的直接展望。",
                        "source_label": "Source",
                    }
                ],
            },
            {"fiscal_label": "2021Q1"},
        )
        self.assertIsNone(sanitized["driver"])
        self.assertEqual(sanitized["guidance"], {"mode": "official_context"})
        self.assertEqual(sanitized["management_theme_items"], [])
        self.assertEqual(sanitized["quotes"], [])

    def test_temporal_sanitizer_strips_future_years_from_direct_output_lists(self) -> None:
        sanitized = _sanitize_temporal_narrative_facts(
            {
                "headline": "2026 年新业务会成为核心估值锚点。",
                "takeaways": ["2026 年投入会继续上升。", "本季利润率已经改善。"],
                "management_themes": [
                    {"label": "AI 扩张", "score": 82, "note": "管理层把 2026 年扩张列为重点。"},
                    {"label": "利润修复", "score": 74, "note": "本季毛利率已经改善。"},
                ],
                "risks": [
                    {"label": "2026 指引兑现", "score": 65, "note": "未来 2026 年高投入可能继续压制利润。"},
                    {"label": "库存波动", "score": 58, "note": "库存与需求错配仍需观察。"},
                ],
                "catalysts": [
                    {"label": "2026 新平台", "score": 80, "note": "新平台会在 2026 年放量。"},
                    {"label": "当季执行改善", "score": 72, "note": "执行效率本季已有改善。"},
                ],
                "call_quote_cards": [
                    {
                        "speaker": "Management",
                        "quote": "We expect 2026 to be a breakout year.",
                        "analysis": "这是对 2026 的直接展望。",
                        "source_label": "Source",
                    },
                    {
                        "speaker": "Management",
                        "quote": "Gross margin improved this quarter.",
                        "analysis": "这是对当季表现的总结。",
                        "source_label": "Source",
                    },
                ],
                "evidence_cards": [
                    {"title": "2026 plan", "text": "2026 年扩产与费用上行是主线。", "source_label": "Source"},
                    {"title": "Current quarter", "text": "本季毛利率已改善。", "source_label": "Source"},
                ],
            },
            {"fiscal_label": "2021Q1"},
        )

        self.assertIsNone(sanitized["headline"])
        self.assertEqual(sanitized["takeaways"], ["本季利润率已经改善。"])
        self.assertEqual(len(sanitized["management_themes"]), 1)
        self.assertEqual(sanitized["management_themes"][0]["label"], "利润修复")
        self.assertEqual(len(sanitized["risks"]), 1)
        self.assertEqual(sanitized["risks"][0]["label"], "库存波动")
        self.assertEqual(len(sanitized["catalysts"]), 1)
        self.assertEqual(sanitized["catalysts"][0]["label"], "当季执行改善")
        self.assertEqual(len(sanitized["call_quote_cards"]), 1)
        self.assertIn("Gross margin improved", sanitized["call_quote_cards"][0]["quote"])
        self.assertEqual(len(sanitized["evidence_cards"]), 1)
        self.assertEqual(sanitized["evidence_cards"][0]["title"], "Current quarter")

    def test_meta_parser_historical_release_uses_period_correct_narrative(self) -> None:
        release_path = self._write_temp_text(
            "meta-historical-release.txt",
            (
                "Facebook Reports Fourth Quarter and Full Year 2016 Results "
                "\"Our business did well in 2016, but we have a lot of work ahead to help bring people together,\" said Mark Zuckerberg, Facebook founder and CEO. "
                "Revenue: Advertising $ 8,629 $ 5,637 53 % Payments and other fees 180 204 (12 )% Total revenue 8,809 5,841 51 % "
                "Total costs and expenses 4,243 3,281 29 % Income from operations $ 4,566 $ 2,560 78 % "
                "Net income $ 3,568 $ 1,562 128 % Diluted earnings per share (EPS) $ 1.21 $ 0.54 124 % "
                "DAUs were 1.23 billion on average for December 2016, an increase of 18% year-over-year. "
                "MAUs were 1.86 billion as of December 31, 2016, an increase of 17% year-over-year. "
                "Mobile advertising revenue represented approximately 84% of advertising revenue for the fourth quarter of 2016. "
                "Capital expenditures for the full year 2016 were $4.49 billion. "
                "Cash and cash equivalents and marketable securities were $29.45 billion at the end of the fourth quarter of 2016."
            ),
        )
        transcript_path = self._write_temp_text(
            "meta-historical-transcript.txt",
            (
                "Sheryl Sandberg, COO Thanks Mark and hi everyone. "
                "Q4 ad revenue grew 53%. Mobile ad revenue reached $7.2 billion, up 61% year-over-year, and was approximately 84% of total ad revenue. "
                "In Q4, the average price per ad increased 3% and the total number of ad impressions served increased 49%, driven primarily by mobile feed ads on Facebook and Instagram. "
                "Instagram now has over 600 million monthly actives and recently passed 400 million daily actives. "
                "I've said before that I see video as a mega trend and we're going to keep putting video first across our family of apps."
            ),
        )
        parsed = parse_official_materials(
            get_company("meta"),
            {"fiscal_label": "2016Q4", "coverage_notes": []},
            [
                {"label": "Meta historical release", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "Meta historical transcript", "kind": "call_summary", "role": "earnings_call", "status": "cached", "text_path": transcript_path},
            ],
        )
        narrative = "\n".join(
            map(
                str,
                [
                    parsed.get("management_themes"),
                    parsed.get("qna_themes"),
                    parsed.get("risks"),
                    parsed.get("catalysts"),
                    parsed.get("call_quote_cards"),
                ],
            )
        )
        self.assertNotIn("2025", narrative)
        self.assertNotIn("2026", narrative)
        self.assertIn("广告收入保持高增", narrative)
        self.assertIn("Instagram", narrative)

    def test_tsla_parser_historical_release_does_not_leak_modern_robotaxi_language(self) -> None:
        release_path = self._write_temp_text(
            "tsla-historical-release.txt",
            (
                "Tesla Q1 2021 Vehicle Production & Deliveries "
                "In the first quarter, we produced just over 180,000 vehicles and delivered nearly 185,000 vehicles. "
                "We are encouraged by the strong reception of the Model Y in China and are quickly progressing to full production capacity. "
                "The new Model S and Model X have also been exceptionally well received, with the new equipment installed and tested in Q1 and we are in the early stages of ramping production. "
                "Our delivery count should be viewed as slightly conservative, and final numbers could vary by up to 0.5% or more. "
                "Total revenues 5,985 6,036 8,771 10,744 10,389 74% "
                "Total GAAP gross margin 18.9% 19.0% 23.5% 24.1% 20.1% 120 bp "
                "Income from operations -167 327 809 575 594 456% "
                "Net income attributable to common stockholders (GAAP) -408 104 331 270 464 2800% "
                "Net cash provided by operating activities 0 1,424 2,402 3,019 1,641 126% "
                "Free cash flow -895 418 1,395 1,869 293 133% "
                "Total automotive revenues 4,973 5,132 7,611 9,314 9,002 81% "
                "Energy generation and storage revenue 293 370 579 752 494 69% "
                "Services and other revenue 719 534 581 678 893 60% "
                "Research and development 324 279 366 522 666 105% "
                "Selling, general and administrative 666 646 708 761 1,056 59% "
                "Restructuring and other — — — — -101 0% "
            ),
        )
        parsed = parse_official_materials(
            get_company("tsla"),
            {"fiscal_label": "2021Q1", "coverage_notes": []},
            [{"label": "Tesla historical release", "kind": "official_release", "status": "cached", "text_path": release_path}],
        )
        narrative = "\n".join(
            map(
                str,
                [
                    parsed.get("management_themes"),
                    parsed.get("qna_themes"),
                    parsed.get("risks"),
                    parsed.get("catalysts"),
                    parsed.get("call_quote_cards"),
                ],
            )
        )
        self.assertNotIn("2026", narrative)
        self.assertNotIn("Robotaxi", narrative)
        self.assertNotIn("physical AI", narrative)
        self.assertIn("Model Y", narrative)
        self.assertIn("S/X", narrative)

    def test_jpm_parser_extracts_annual_geographies(self) -> None:
        narrative_path = self._write_temp_text(
            "jpm-narrative.txt",
            (
                "NET INCOME OF $14.7 BILLION ( $4.84 PER SHARE ) "
                "Reported revenue of $43.7 billion and managed revenue of $45.2 billion "
            ),
        )
        supplement_path = self._write_temp_text(
            "jpm-supplement.txt",
            (
                "Consumer & Community Banking $ 18,000 $ 17,500 $ 17,000 $ 16,000 $ 15,000 6 "
                "Commercial & Investment Bank $ 17,000 $ 16,200 $ 15,800 $ 15,100 $ 14,900 5 "
                "Asset & Wealth Management $ 5,200 $ 4,900 $ 4,700 $ 4,500 $ 4,300 8 "
            ),
        )
        sec_path = self._write_temp_text(
            "jpm-sec.txt",
            (
                "2023 Europe/Middle East/Africa $ 24,478 $ 14,825 $ 9,653 $ 6,813 $ 641,190 "
                "Asia-Pacific 14,065 8,271 5,794 4,101 343,520 "
                "Latin America/Caribbean 4,215 2,180 2,035 1,561 96,759 "
                "Total international 42,758 25,276 17,482 12,475 1,081,469 "
                "North America (a) 139,689 84,576 55,113 44,573 3,343,431 "
                "2022 Europe/Middle East/Africa $ 22,353 $ 12,843 $ 9,510 $ 6,713 $ 552,407 "
                "Asia-Pacific 11,995 6,922 5,073 3,615 296,430 "
                "Latin America/Caribbean 3,885 1,895 1,990 1,512 73,631 "
                "Total international 38,233 21,660 16,573 11,840 922,468 "
                "North America (a) 139,323 80,815 58,508 46,631 3,080,346 "
            ),
        )
        parsed = parse_official_materials(
            get_company("jpm"),
            {"fiscal_label": "2025Q4", "coverage_notes": []},
            [
                {"label": "JPM narrative", "kind": "official_release", "status": "cached", "text_path": narrative_path},
                {"label": "JPM supplement", "kind": "presentation", "status": "cached", "text_path": supplement_path},
                {"label": "JPM Form 10-K", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )
        self.assertEqual(parsed["current_geographies"][0]["name"], "North America")
        self.assertEqual(parsed["current_geographies"][0]["scope"], "quarterly_mapped_from_official_geography")

    def test_jpm_parser_extracts_geographies_from_quarterly_10q_section(self) -> None:
        narrative_path = self._write_temp_text(
            "jpm-quarterly-narrative.txt",
            (
                "NET INCOME OF $9.7 BILLION ( $3.12 PER SHARE ) "
                "Reported revenue of $32.7 billion and managed revenue of $33.5 billion "
            ),
        )
        supplement_path = self._write_temp_text(
            "jpm-quarterly-supplement.txt",
            (
                "Consumer & Community Banking $ 16,000 $ 15,500 $ 15,100 $ 14,900 $ 14,600 3 "
                "Commercial & Investment Bank $ 11,600 $ 11,100 $ 10,900 $ 10,700 $ 10,300 4 "
                "Asset & Wealth Management $ 3,700 $ 3,500 $ 3,400 $ 3,300 $ 3,100 6 "
            ),
        )
        sec_path = self._write_temp_text(
            "jpm-quarterly-sec.txt",
            (
                "For the nine months ended September 30, (in millions, except where otherwise noted) "
                "2022 2021 Change 2022 2021 Change "
                "Total net revenue (a) "
                "Europe/Middle East/Africa $ 3,653 $ 3,201 14 % $ 12,625 $ 11,045 14 % "
                "Asia-Pacific 2,060 1,973 4 6,068 6,026 1 "
                "Latin America/Caribbean 549 526 4 1,690 1,480 14 "
                "Total international net revenue 6,262 5,700 10 20,383 18,551 10 "
                "North America 5,613 6,696 (16) 16,968 21,664 (22) "
                "Total net revenue $ 11,875 $ 12,396 (4) $ 37,351 $ 40,215 (7) "
                "Loans retained (period-end)"
            ),
        )
        parsed = parse_official_materials(
            get_company("jpm"),
            {"fiscal_label": "2022Q3", "coverage_notes": []},
            [
                {"label": "JPM quarterly narrative", "kind": "official_release", "status": "cached", "text_path": narrative_path},
                {"label": "JPM quarterly supplement", "kind": "presentation", "status": "cached", "text_path": supplement_path},
                {"label": "JPM Form 10-Q", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )
        names = [item["name"] for item in parsed["current_geographies"]]
        self.assertEqual(
            names,
            ["North America", "Europe / Middle East / Africa", "Asia-Pacific", "Latin America / Caribbean"],
        )
        geographies = {item["name"]: item for item in parsed["current_geographies"]}
        self.assertEqual(geographies["North America"]["scope"], "quarterly_mapped_from_official_geography")
        self.assertAlmostEqual(geographies["North America"]["value_bn"], 14.855, places=3)
        self.assertAlmostEqual(geographies["Europe / Middle East / Africa"]["value_bn"], 11.053, places=3)

    def test_finalize_backfills_management_themes_after_qna_expansion(self) -> None:
        parsed = parse_official_materials(
            get_company("jpm"),
            {"fiscal_label": "2011Q4", "coverage_notes": []},
            [
                {"label": "JPM narrative", "kind": "official_release", "status": "cached", "text_path": self._write_temp_text("jpm-min-narrative.txt", "NET INCOME OF $3.7 BILLION ( $0.90 PER SHARE ) Reported revenue of $21.5 billion and managed revenue of $24.3 billion")},
                {"label": "JPM supplement", "kind": "presentation", "status": "cached", "text_path": self._write_temp_text("jpm-min-supplement.txt", "Consumer & Community Banking $ 9,000 $ 8,700 $ 8,500 $ 8,200 $ 8,000 4 Commercial & Investment Bank $ 8,400 $ 8,000 $ 7,800 $ 7,600 $ 7,300 5 Asset & Wealth Management $ 3,000 $ 2,900 $ 2,800 $ 2,700 $ 2,600 6")},
                {"label": "JPM Form 10-K", "kind": "sec_filing", "status": "cached", "text_path": self._write_temp_text("jpm-min-sec.txt", "2011 Europe/Middle East/Africa $ 20,000 $ 19,000 Asia-Pacific 10,000 9,000 Latin America/Caribbean 3,000 2,500 North America (a) 100,000 98,000 2010 Europe/Middle East/Africa $ 18,000 $ 17,000 Asia-Pacific 9,000 8,000 Latin America/Caribbean 2,500 2,200 North America (a) 98,000 96,000")},
            ],
        )
        self.assertGreaterEqual(len(parsed["management_themes"]), 3)

    def test_xom_parser_extracts_annual_us_non_us_geographies(self) -> None:
        release_path = self._write_temp_text(
            "xom-release.txt",
            (
                "Sales and other operating revenue was $323.9 billion. "
                "Net income attributable to ExxonMobil was $33.7 billion. "
                "Diluted earnings per common share was $8.12."
            ),
        )
        sec_path = self._write_temp_text(
            "xom-sec.txt",
            (
                "Year ended December 31, 2023 Revenues and other income Sales and other operating revenue "
                "25,396 13,993 99,073 145,378 7,594 14,615 5,502 12,269 323,820 Income from equity affiliates 19 4,340 139 198 135 544 7 (52) 5,330 "
                "Year ended December 31, 2022 Revenues and other income Sales and other operating revenue "
                "22,929 14,202 101,325 159,531 8,558 14,338 5,790 12,463 339,136 Income from equity affiliates (36) 5,649 140 (109) 166 615 0 (43) 6,382 "
            ),
        )
        parsed = parse_official_materials(
            get_company("xom"),
            {"fiscal_label": "2025Q4", "coverage_notes": []},
            [
                {"label": "ExxonMobil earnings release", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "ExxonMobil Form 10-K", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )
        self.assertEqual(parsed["current_geographies"][0]["scope"], "annual_filing")
        self.assertEqual(parsed["current_geographies"][0]["name"], "Non-U.S.")

    def test_tsmc_parser_extracts_annual_geographies(self) -> None:
        release_path = self._write_temp_text(
            "tsmc-release-annual-geo.txt",
            (
                "TSMC today announced consolidated revenue of NT$1,046.09 billion, net income of NT$505.74 billion, "
                "and diluted earnings per share of NT$19.50 for the fourth quarter ended December 31, 2025. "
                "In US dollars, fourth quarter revenue was $33.73 billion, which increased 25.5% year-over-year and increased 1.9% from the previous quarter. "
                "Gross margin for the quarter was 62.3%, operating margin was 54.0%, and net profit margin was 48.3%. "
                "Revenue is expected to be between US$34.6 billion and US$35.8 billion."
            ),
        )
        sec_path = self._write_temp_text(
            "tsmc-20f.txt",
            (
                "Years Ended December 31 2022 2023 2024 NT$ NT$ NT$ Geography "
                "(In Millions) (In Millions) (In Millions) "
                "Taiwan $ 210,470.8 $ 149,777.4 $ 168,552.3 "
                "United States 1,493,328.8 1,408,841.9 1,920,114.2 "
                "China 245,168.8 267,154.1 310,225.0 "
                "Japan 119,099.3 132,072.0 148,441.2 "
                "Europe, the Middle East and Africa 123,767.1 117,348.2 139,772.8 "
                "Others 72,056.5 86,542.2 101,104.6 "
            ),
        )
        parsed = parse_official_materials(
            get_company("tsmc"),
            {"fiscal_label": "2025Q4", "coverage_notes": []},
            [
                {"label": "TSMC earnings release", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "TSMC Form 20-F", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )
        self.assertGreaterEqual(len(parsed["current_geographies"]), 6)
        self.assertEqual(parsed["current_geographies"][0]["name"], "United States")
        self.assertEqual(parsed["current_geographies"][0]["scope"], "annual_filing")

    def test_asml_parser_extracts_annual_geographies(self) -> None:
        presentation_path = self._write_temp_text(
            "asml-presentation.txt",
            (
                "Q4 2025 Total net sales €9.3 billion Net system sales €7.6 billion Installed Base Management1 sales €1.7 billion "
                "Gross Margin 51.7% Operating margin2 36.0% Net income as a percentage of total net sales 29.4% Earnings per share (basic) €6.85 "
                "Q1 2026 Total net sales between €7.5 billion and €8.0 billion of which Installed Base Management1 sales around €1.9 billion "
                "Gross margin between 50.0% and 53.0%"
            ),
        )
        sec_path = self._write_temp_text(
            "asml-20f.txt",
            (
                "Total net sales and long-lived assets by geographic region were as follows: "
                "Year ended December 31 (€, in millions) 2023 2024 2025 "
                "Total net sales Long-lived assets Total net sales Long-lived assets Total net sales Long-lived assets "
                "Japan 613.6 10.4 1,156.0 16.0 1,420.9 20.9 "
                "South Korea 6,949.2 148.1 6,408.8 241.6 8,159.6 348.7 "
                "Singapore 282.1 5.0 285.0 4.3 608.4 8.4 "
                "Taiwan 8,074.6 354.5 4,354.0 473.8 8,337.9 531.7 "
                "China 7,251.8 48.6 10,195.1 72.7 9,519.7 70.9 "
                "Rest of Asia 3.9 0.2 3.5 0.1 2.7 1.8 "
                "Netherlands 25.1 3,783.6 16.6 4,621.4 4.7 5,342.4 "
                "EMEA 1,206.8 314.5 1,322.1 443.1 524.3 529.6 "
                "United States 3,151.4 1,134.9 4,521.8 1,361.0 4,089.1 1,380.4 "
            ),
        )
        parsed = parse_official_materials(
            get_company("asml"),
            {"fiscal_label": "2025Q4", "coverage_notes": []},
            [
                {"label": "ASML presentation", "kind": "presentation", "status": "cached", "text_path": presentation_path},
                {"label": "ASML Form 20-F", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )
        self.assertGreaterEqual(len(parsed["current_geographies"]), 8)
        names = {item["name"] for item in parsed["current_geographies"]}
        self.assertIn("China", names)
        self.assertIn("United States", names)
        self.assertTrue(all(item.get("scope") == "quarterly_mapped_from_official_geography" for item in parsed["current_geographies"]))

    def test_asml_parser_extracts_legacy_quarter_segments_from_narrative(self) -> None:
        release_path = self._write_temp_text(
            "asml-q42008-release.txt",
            (
                "In Q4 2008, ASML net sales of EUR 494 million included 15 new and 10 used systems, "
                "totaling net system sales of EUR 381 million, and net service and field options sales of EUR 113 million. "
                "Q4 2008 net bookings totaled 13 systems valued at EUR 127 million."
            ),
        )
        parsed = parse_official_materials(
            get_company("asml"),
            {"fiscal_label": "2008Q4", "coverage_notes": []},
            [
                {"label": "ASML quarterly results release", "kind": "official_release", "status": "cached", "text_path": release_path},
            ],
        )
        names = [item["name"] for item in parsed["current_segments"]]
        self.assertEqual(names, ["Net system sales", "Installed Base Management"])
        values = {item["name"]: item["value_bn"] for item in parsed["current_segments"]}
        self.assertAlmostEqual(values["Net system sales"], 0.381, places=3)
        self.assertAlmostEqual(values["Installed Base Management"], 0.113, places=3)

    def test_asml_parser_extracts_legacy_q1_release_table_metrics(self) -> None:
        release_path = self._write_temp_text(
            "asml-q12016-release.txt",
            (
                "ASML today publishes its 2016 first-quarter results. "
                "Q1 net sales of EUR 1.33 billion, gross margin 42.6 percent. "
                "(Figures in millions of euros unless otherwise indicated) "
                "Q4 2015 Q1 2016 Net sales 1,434 1,333 "
                "...of which service and field option sales 553 477 "
                "Gross profit 660 568 Gross margin (%) 46.0 42.6 "
                "Net income 292 198 EPS (basic; in euros) 0.68 0.46"
            ),
        )
        parsed = parse_official_materials(
            get_company("asml"),
            {"fiscal_label": "2016Q1", "coverage_notes": []},
            [
                {"label": "ASML press release", "kind": "official_release", "status": "cached", "text_path": release_path},
            ],
        )
        latest_kpis = parsed["latest_kpis"]
        self.assertAlmostEqual(latest_kpis["revenue_bn"], 1.33, places=2)
        self.assertAlmostEqual(latest_kpis["net_income_bn"], 0.198, places=3)
        self.assertAlmostEqual(latest_kpis["gaap_eps"], 0.46, places=2)
        values = {item["name"]: item["value_bn"] for item in parsed["current_segments"]}
        self.assertAlmostEqual(values["Installed Base Management"], 0.477, places=3)
        self.assertAlmostEqual(values["Net system sales"], 0.853, places=3)

    def test_asml_parser_extracts_legacy_annual_geographies(self) -> None:
        presentation_path = self._write_temp_text(
            "asml-legacy-presentation.txt",
            (
                "Q4 results summary Net sales of € 3,143 million, net systems sales valued at € 2,424 million, "
                "Installed Base Management sales of € 719 million Gross margin of 44.3% Operating margin of 26.0% "
                "Net income as a percentage of net sales of 25.1%"
            ),
        )
        sec_path = self._write_temp_text(
            "asml-legacy-20f.txt",
            (
                "Total net sales and long-lived assets (consisting of property, plant and equipment) by geographic region were as follows: "
                "Year ended December 31 Total net sales Long-lived assets (in millions) EUR EUR "
                "2018 Japan 567.6 8.2 Korea 3,725.1 24.6 Singapore 222.5 1.1 Taiwan 1,989.5 96.5 China 1,842.8 16.2 "
                "Rest of Asia 1.9 0.4 Netherlands 1.2 1,113.8 EMEA 631.7 5.1 United States 1,961.7 323.6 Total 10,944.0 1,589.5 "
                "2017 1 Japan 404.3 3.4 Korea 3,031.4 23.2 Singapore 163.7 0.8 Taiwan 2,096.7 88.1 China 919.5 4.1 "
                "Rest of Asia 3.5 3.0 Netherlands 4.0 1,186.0 EMEA 921.5 5.0 United States 1,418.1 287.2 Total 8,962.7 1,600.8"
            ),
        )
        parsed = parse_official_materials(
            get_company("asml"),
            {"fiscal_label": "2018Q4", "coverage_notes": []},
            [
                {"label": "ASML presentation", "kind": "presentation", "status": "cached", "text_path": presentation_path},
                {"label": "ASML Form 20-F", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )
        self.assertGreaterEqual(len(parsed["current_geographies"]), 8)
        names = {item["name"] for item in parsed["current_geographies"]}
        self.assertIn("China", names)
        self.assertIn("South Korea", names)
        self.assertIn("United States", names)

    def test_tsmc_parser_extracts_platform_structure_from_ocr_text(self) -> None:
        release_path = self._write_temp_text(
            "tsmc-release.txt",
            (
                "TSMC today announced consolidated revenue of NT$1,046.09 billion, net income of NT$505.74 billion, "
                "and diluted earnings per share of NT$19.50 for the fourth quarter ended December 31, 2025. "
                "In US dollars, fourth quarter revenue was $33.73 billion, which increased 25.5% year-over-year and increased 1.9% from the previous quarter. "
                "Gross margin for the quarter was 62.3%, operating margin was 54.0%, and net profit margin was 48.3%. "
                "Revenue is expected to be between US$34.6 billion and US$35.8 billion."
            ),
        )
        presentation_path = self._write_temp_text(
            "tsmc-presentation.txt",
            (
                "=== a4q25presentatione007 ===\n"
                "4Q25 Revenue by Platform\n"
                "IoT\n"
                "5%\n"
                "Automotive DCE\n"
                "5% 1%\n"
                "Others\n"
                "2%\n"
                "Smartphone\n"
                "32%\n"
                "HPC\n"
                "55%\n"
                "Growth Rate by Platform (QoQ)\n"
            ),
        )

        parsed = parse_official_materials(
            get_company("tsmc"),
            {"fiscal_label": "2025Q4", "coverage_notes": []},
            [
                {"label": "TSMC earnings release", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "TSMC earnings presentation", "kind": "presentation", "status": "cached", "text_path": presentation_path},
            ],
        )

        self.assertAlmostEqual(parsed["latest_kpis"]["revenue_bn"], 33.73, places=2)
        self.assertEqual([item["name"] for item in parsed["current_segments"][:3]], ["HPC", "Smartphone", "Internet of Things"])
        self.assertAlmostEqual(parsed["current_segments"][0]["value_bn"], 18.551, places=3)

    def test_tsmc_parser_extracts_q2_platform_shares_from_ocr_block(self) -> None:
        release_path = self._write_temp_text(
            "tsmc-q2-release.txt",
            (
                "TSMC today announced consolidated revenue of NT$933.79 billion, net income of NT$398.27 billion, "
                "and diluted earnings per share of NT$15.36 for the second quarter ended June 30, 2025. "
                "In US dollars, second quarter revenue was $30.07 billion."
            ),
        )
        presentation_path = self._write_temp_text(
            "tsmc-q2-presentation.txt",
            (
                "2Q25 Revenue by Platform Unleash Innovation\n"
                "Growth Rate by Platform (QoQ)\n"
                "loT 5% Automotive 5% DCE\n"
                "1%\n"
                "Others 2% +30%\n"
                "Smartphone 27%\n"
                "HPC\n"
                "60% +14% +14%\n"
                "+7% +6%\n"
                "+0%\n"
                "HPC Smartphone loT Automotive DCE Others\n"
            ),
        )

        parsed = parse_official_materials(
            get_company("tsmc"),
            {"fiscal_label": "2025Q2", "coverage_notes": []},
            [
                {"label": "TSMC earnings release", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "TSMC earnings presentation", "kind": "presentation", "status": "cached", "text_path": presentation_path},
            ],
        )

        segments = {item["name"]: item for item in parsed["current_segments"]}
        self.assertAlmostEqual(segments["HPC"]["value_bn"], 18.042, places=3)
        self.assertAlmostEqual(segments["Smartphone"]["value_bn"], 8.119, places=3)
        self.assertIn("Internet of Things", segments)
        self.assertIn("Automotive", segments)
        self.assertIn("DCE", segments)
        self.assertIn("Others", segments)

    def test_costco_generic_parser_extracts_geographies_from_segment_table(self) -> None:
        release_path = self._write_temp_text(
            "costco-release.txt",
            (
                "Total revenue increased 8% to $67,307, driven by an increase in comparable sales. "
                "Net sales increased 8% to $65,978, driven by an increase in comparable sales. "
                "Membership fee revenue increased 14% to $1,329. "
                "Net income was $2,001, or $4.50 per diluted share."
            ),
        )
        sec_path = self._write_temp_text(
            "costco-sec.txt",
            (
                "The following table provides the revenue, significant expenses, and operating income for the Company's reportable segments: "
                "12 Weeks Ended November 23, 2025 November 24, 2024 United States Total revenue $ 48,569 $ 45,088 "
                "Canada Total revenue $ 9,073 $ 8,404 Other International Total revenue $ 9,665 $ 8,659 Total Total revenue $ 67,307 $ 62,151 "
            ),
        )

        parsed = parse_official_materials(
            get_company("costco"),
            {"fiscal_label": "2025Q4", "coverage_notes": []},
            [
                {"label": "Costco earnings release", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "Costco Form 10-Q", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )

        self.assertEqual([item["name"] for item in parsed["current_geographies"]], ["United States", "Other International", "Canada"])
        self.assertAlmostEqual(parsed["current_geographies"][0]["value_bn"], 48.569, places=3)

    def test_extract_company_geographies_uses_raw_html_sec_table(self) -> None:
        sec_raw_html_path = self._write_temp_text(
            "costco-sec-table.html",
            (
                "<html><body><table>"
                "<tr><th>12 Weeks Ended</th><th>November 23, 2025</th><th>November 24, 2024</th></tr>"
                "<tr><th>(In millions)</th><th></th><th></th></tr>"
                "<tr><td>United States</td><td>$ 48,569</td><td>$ 45,088</td></tr>"
                "<tr><td>Canada</td><td>$ 9,073</td><td>$ 8,404</td></tr>"
                "<tr><td>Other International</td><td>$ 9,665</td><td>$ 8,659</td></tr>"
                "<tr><td>Total</td><td>$ 67,307</td><td>$ 62,151</td></tr>"
                "</table></body></html>"
            ),
        )

        geographies = _extract_company_geographies(
            "costco",
            [
                {
                    "label": "Costco Form 10-Q",
                    "kind": "sec_filing",
                    "status": "cached",
                    "raw_text": "placeholder text without structured region values",
                    "flat_text": "placeholder text without structured region values",
                    "raw_path": sec_raw_html_path,
                },
            ],
            67.307,
        )

        self.assertEqual([item["name"] for item in geographies], ["United States", "Other International", "Canada"])
        self.assertAlmostEqual(geographies[0]["value_bn"], 48.569, places=3)

    def test_extract_company_segments_prefers_raw_html_table_metrics(self) -> None:
        raw_html_path = self._write_temp_text(
            "amazon-segments.html",
            (
                "<html><body><table>"
                "<tr><th>Net sales</th><th>Three months ended September 30, 2025</th><th>Three months ended September 30, 2024</th></tr>"
                "<tr><th>(In millions)</th><th></th><th></th></tr>"
                "<tr><td>Online stores</td><td>61,406</td><td>57,270</td></tr>"
                "<tr><td>Third-party seller services</td><td>38,852</td><td>34,150</td></tr>"
                "<tr><td>Advertising services</td><td>15,318</td><td>14,332</td></tr>"
                "<tr><td>Subscription services</td><td>12,345</td><td>10,500</td></tr>"
                "</table></body></html>"
            ),
        )

        segments = _extract_company_segments(
            "amazon",
            [
                {
                    "label": "Amazon Form 10-Q",
                    "kind": "sec_filing",
                    "status": "cached",
                    "raw_text": "placeholder text without structured segment values",
                    "flat_text": "placeholder text without structured segment values",
                    "raw_path": raw_html_path,
                }
            ],
        )

        segment_map = {item["name"]: item["value_bn"] for item in segments}
        self.assertAlmostEqual(segment_map["Online stores"], 61.406, places=3)
        self.assertAlmostEqual(segment_map["Third-party seller services"], 38.852, places=3)
        self.assertAlmostEqual(segment_map["Advertising services"], 15.318, places=3)
        self.assertAlmostEqual(segment_map["Subscription services"], 12.345, places=3)

    def test_extract_company_segments_prefers_three_month_columns_over_ytd_columns(self) -> None:
        raw_html_path = self._write_temp_text(
            "amazon-segments-ytd.html",
            (
                "<html><body><table>"
                "<tr><th>Nine months ended</th><th>September 30, 2025</th><th>September 30, 2024</th><th>Three months ended</th><th>September 30, 2025</th><th>September 30, 2024</th></tr>"
                "<tr><th>(In millions)</th><th></th><th></th><th></th><th></th></tr>"
                "<tr><td>Online stores</td><td>180,500</td><td>169,000</td><td>61,406</td><td>57,270</td></tr>"
                "<tr><td>Third-party seller services</td><td>113,700</td><td>101,200</td><td>38,852</td><td>34,150</td></tr>"
                "<tr><td>Advertising services</td><td>44,900</td><td>40,800</td><td>15,318</td><td>14,332</td></tr>"
                "<tr><td>Subscription services</td><td>36,200</td><td>31,100</td><td>12,345</td><td>10,500</td></tr>"
                "</table></body></html>"
            ),
        )

        segments = _extract_company_segments(
            "amazon",
            [
                {
                    "label": "Amazon Form 10-Q",
                    "kind": "sec_filing",
                    "status": "cached",
                    "raw_text": "placeholder text without structured segment values",
                    "flat_text": "placeholder text without structured segment values",
                    "raw_path": raw_html_path,
                }
            ],
        )

        segment_map = {item["name"]: item["value_bn"] for item in segments}
        self.assertAlmostEqual(segment_map["Online stores"], 61.406, places=3)
        self.assertAlmostEqual(segment_map["Third-party seller services"], 38.852, places=3)
        self.assertAlmostEqual(segment_map["Advertising services"], 15.318, places=3)
        self.assertAlmostEqual(segment_map["Subscription services"], 12.345, places=3)

    def test_extract_company_segments_prefers_quarterly_profiled_html_table_over_annual_match(self) -> None:
        raw_html_path = self._write_temp_text(
            "mcdonalds-segments-quarter-vs-year.html",
            (
                "<html><body>"
                "<table>"
                "<tr><th>Years Ended December 31,</th><th>2025</th><th>2024</th></tr>"
                "<tr><td>U.S.</td><td>10,487</td><td>10,407</td></tr>"
                "<tr><td>International Operated Markets</td><td>13,410</td><td>12,458</td></tr>"
                "<tr><td>International Developmental Licensed Markets &amp; Corporate</td><td>2,342</td><td>2,630</td></tr>"
                "<tr><td>Total Revenues</td><td>26,885</td><td>25,920</td></tr>"
                "</table>"
                "<table>"
                "<tr><th>Quarters Ended December 31,</th><th>2025</th><th>2024</th></tr>"
                "<tr><td>Total Franchised revenues and Company-owned and operated sales</td><td></td><td></td></tr>"
                "<tr><td>U.S.</td><td>2,696</td><td>2,574</td></tr>"
                "<tr><td>International Operated Markets</td><td>3,538</td><td>3,141</td></tr>"
                "<tr><td>International Developmental Licensed Markets &amp; Corporate</td><td>613</td><td>553</td></tr>"
                "<tr><td>Total Revenues</td><td>7,009</td><td>6,388</td></tr>"
                "</table>"
                "</body></html>"
            ),
        )

        segments = _extract_company_segments(
            "mcdonalds",
            [
                {
                    "label": "McDonald's Exhibit 99.2",
                    "kind": "presentation",
                    "status": "cached",
                    "raw_text": "Quarterly and annual segment tables.",
                    "flat_text": "Quarterly and annual segment tables.",
                    "raw_path": raw_html_path,
                }
            ],
            revenue_bn=7.009,
            target_calendar_quarter="2025Q4",
        )

        segment_map = {item["name"]: item["value_bn"] for item in segments}
        self.assertAlmostEqual(segment_map["U.S."], 2.696, places=3)
        self.assertAlmostEqual(segment_map["International Operated Markets"], 3.538, places=3)
        self.assertAlmostEqual(segment_map["International Developmental Licensed Markets & Corporate"], 0.613, places=3)

    def test_extract_company_segments_collects_single_segment_html_tables_from_headers(self) -> None:
        raw_html_path = self._write_temp_text(
            "starbucks-segment-header-tables.html",
            (
                "<html><body>"
                "<table>"
                "<tr><th>Q1 North America Segment Results</th></tr>"
                "<tr><th>Quarter Ended</th><th>Dec 28, 2025</th><th>Dec 29, 2024</th></tr>"
                "<tr><td>Net revenues</td><td>7,280.5</td><td>7,071.9</td></tr>"
                "<tr><td>Operating Income</td><td>867.0</td><td>1,181.3</td></tr>"
                "</table>"
                "<table>"
                "<tr><th>Q1 International Segment Results</th></tr>"
                "<tr><th>Quarter Ended</th><th>Dec 28, 2025</th><th>Dec 29, 2024</th></tr>"
                "<tr><td>Net revenues</td><td>2,064.9</td><td>1,871.3</td></tr>"
                "<tr><td>Operating Income</td><td>282.7</td><td>237.1</td></tr>"
                "</table>"
                "<table>"
                "<tr><th>Q1 Channel Development Segment Results</th></tr>"
                "<tr><th>Quarter Ended</th><th>Dec 28, 2025</th><th>Dec 29, 2024</th></tr>"
                "<tr><td>Net revenues</td><td>522.7</td><td>436.3</td></tr>"
                "<tr><td>Operating Income</td><td>215.8</td><td>208.0</td></tr>"
                "</table>"
                "</body></html>"
            ),
        )

        segments = _extract_company_segments(
            "starbucks",
            [
                {
                    "label": "Starbucks quarterly segment release",
                    "kind": "official_release",
                    "status": "cached",
                    "raw_text": "Segment result tables.",
                    "flat_text": "Segment result tables.",
                    "raw_path": raw_html_path,
                }
            ],
            revenue_bn=9.868,
            target_calendar_quarter="2025Q4",
        )

        segment_map = {item["name"]: item["value_bn"] for item in segments}
        self.assertAlmostEqual(segment_map["North America"], 7.280, places=3)
        self.assertAlmostEqual(segment_map["International"], 2.065, places=3)
        self.assertAlmostEqual(segment_map["Channel Development"], 0.523, places=3)

    def test_generic_parser_extracts_statement_metrics_from_raw_html_sec_table(self) -> None:
        sec_text_path = self._write_temp_text(
            "oracle-sec-placeholder.txt",
            "Condensed consolidated statements of operations.",
        )
        sec_raw_html_path = self._write_temp_text(
            "oracle-sec-statement.html",
            (
                "<html><body><table>"
                "<tr><th>Three months ended</th><th>May 31, 2025</th><th>May 31, 2024</th></tr>"
                "<tr><th>(In millions)</th><th></th><th></th></tr>"
                "<tr><td>Total revenues</td><td>14,100</td><td>13,050</td></tr>"
                "<tr><td>Total cost of revenues</td><td>4,935</td><td>4,698</td></tr>"
                "<tr><td>Gross profit</td><td>9,165</td><td>8,352</td></tr>"
                "<tr><td>Research and development</td><td>2,115</td><td>2,020</td></tr>"
                "<tr><td>Selling, general and administrative</td><td>3,102</td><td>2,950</td></tr>"
                "<tr><td>Operating income</td><td>3,948</td><td>3,382</td></tr>"
                "<tr><td>Income before taxes</td><td>3,650</td><td>3,121</td></tr>"
                "<tr><td>Net income</td><td>3,400</td><td>3,036</td></tr>"
                "</table></body></html>"
            ),
        )

        parsed = parse_official_materials(
            get_company("oracle"),
            {"fiscal_label": "2025Q4", "coverage_notes": []},
            [
                {
                    "label": "Oracle Form 10-Q",
                    "kind": "sec_filing",
                    "status": "cached",
                    "text_path": sec_text_path,
                    "raw_path": sec_raw_html_path,
                }
            ],
        )

        self.assertAlmostEqual(parsed["latest_kpis"]["revenue_bn"], 14.1, places=3)
        self.assertAlmostEqual(parsed["latest_kpis"]["net_income_bn"], 3.4, places=3)

    def test_generic_parser_prefers_consolidated_statement_over_segment_table(self) -> None:
        sec_text_path = self._write_temp_text(
            "starbucks-sec-placeholder.txt",
            "Condensed consolidated statements of earnings.",
        )
        sec_raw_html_path = self._write_temp_text(
            "starbucks-sec-statement-vs-segment.html",
            (
                "<html><body>"
                "<table>"
                "<tr><th>Q1 Channel Development Segment Results</th></tr>"
                "<tr><th>Quarter Ended</th><th>Dec 28, 2025</th><th>Dec 29, 2024</th></tr>"
                "<tr><td>Net revenues</td><td>522.7</td><td>436.3</td></tr>"
                "<tr><td>Operating Income</td><td>215.8</td><td>208.0</td></tr>"
                "</table>"
                "<table>"
                "<tr><th>Condensed Consolidated Statements of Earnings</th></tr>"
                "<tr><th>Quarter Ended</th><th>Dec 28, 2025</th><th>Dec 29, 2024</th></tr>"
                "<tr><td>Total net revenues</td><td>9,915.1</td><td>9,397.8</td></tr>"
                "<tr><td>Total cost of revenues</td><td>3,470.3</td><td>3,288.9</td></tr>"
                "<tr><td>Gross profit</td><td>6,444.8</td><td>6,108.9</td></tr>"
                "<tr><td>Research and development</td><td>210.0</td><td>198.0</td></tr>"
                "<tr><td>Selling, general and administrative</td><td>640.1</td><td>623.5</td></tr>"
                "<tr><td>Total operating expenses</td><td>9,084.9</td><td>8,322.6</td></tr>"
                "<tr><td>Operating income</td><td>890.8</td><td>1,121.7</td></tr>"
                "<tr><td>Earnings before income taxes</td><td>764.8</td><td>1,022.3</td></tr>"
                "<tr><td>Net earnings attributable to Starbucks</td><td>293.3</td><td>780.8</td></tr>"
                "</table>"
                "</body></html>"
            ),
        )

        parsed = parse_official_materials(
            get_company("starbucks"),
            {"calendar_quarter": "2025Q4", "fiscal_label": "2025Q4", "coverage_notes": []},
            [
                {
                    "label": "Starbucks Form 10-Q",
                    "kind": "sec_filing",
                    "status": "cached",
                    "text_path": sec_text_path,
                    "raw_path": sec_raw_html_path,
                }
            ],
        )

        self.assertAlmostEqual(parsed["latest_kpis"]["revenue_bn"], 9.915, places=3)
        self.assertAlmostEqual(parsed["latest_kpis"]["net_income_bn"], 0.293, places=3)
        self.assertAlmostEqual(parsed["latest_kpis"]["gaap_gross_margin_pct"], 65.0, places=1)
        opex_names = [item["name"] for item in parsed["income_statement"]["opex_breakdown"]]
        self.assertIn("Research and development", opex_names)
        self.assertIn("Selling, general and administrative", opex_names)

    def test_official_material_proxy_summary_builds_call_proxy_without_transcript(self) -> None:
        proxy = _official_material_proxy_summary(
            {
                "call_quote_cards": [
                    {"quote": "Management expects margin recovery to remain paced by labor and mix."}
                ],
                "management_themes": [
                    {"label": "门店效率", "score": 82, "note": "管理层会继续围绕门店效率与交易量修复展开。"}
                ],
                "evidence_cards": [
                    {"detail": "官方 release 强调交易量修复与会员活跃度改善。"}
                ],
            },
            [
                {
                    "label": "Example earnings release",
                    "kind": "official_release",
                    "status": "cached",
                    "text_length": 1200,
                }
            ],
            [
                {"label": "交易量修复", "score": 78, "note": "市场会继续追问交易量修复是否可持续。"}
            ],
        )

        self.assertIsNotNone(proxy)
        self.assertEqual(proxy["source_type"], "official_material_proxy")
        self.assertEqual(proxy["filename"], "Example earnings release")
        self.assertTrue(proxy["highlights"])
        self.assertEqual(proxy["topics"][0]["label"], "交易量修复")

    def test_official_material_proxy_summary_prefers_call_like_supplement_material(self) -> None:
        supplement_path = self._write_temp_text(
            "mcd-supplement.txt",
            (
                "Supplemental Information Revenue grew 7% year over year while operating income expanded as restaurant margins improved. "
                "Management expects comparable sales momentum and disciplined expense control to remain key themes next quarter. "
                "Cash flow remained strong and guidance continues to emphasize franchise health and consumer demand."
            ),
        )

        proxy = _official_material_proxy_summary(
            {
                "call_quote_cards": [],
                "management_themes": [{"label": "利润率修复", "score": 80, "note": "补充材料继续强调利润率改善。"}],
                "evidence_cards": [],
            },
            [
                {
                    "label": "Example earnings release",
                    "kind": "official_release",
                    "role": "earnings_release",
                    "status": "cached",
                    "text_length": 1200,
                },
                {
                    "label": "Example exhibit99.2 supplement",
                    "kind": "call_summary",
                    "role": "earnings_call",
                    "status": "cached",
                    "text_length": 2000,
                    "text_path": supplement_path,
                },
                {
                    "label": "Example Q4 2025 Webcast",
                    "title": "Registration | Example Q4 2025 Earnings Conference Call",
                    "kind": "call_summary",
                    "role": "earnings_call",
                    "status": "cached",
                    "text_length": 420,
                },
            ],
            [{"label": "同店销售", "score": 76, "note": "市场会继续追问同店销售和利润率。"}],
        )

        self.assertIsNotNone(proxy)
        self.assertEqual(proxy["filename"], "Example exhibit99.2 supplement")
        self.assertTrue(proxy["highlights"])
        self.assertTrue(proxy["topics"])

    def test_source_material_warnings_ignores_failed_duplicate_role_when_covered(self) -> None:
        warnings = _source_material_warnings(
            [
                {
                    "kind": "official_release",
                    "role": "earnings_release",
                    "status": "cached",
                    "text_length": 3200,
                },
                {
                    "kind": "official_release",
                    "role": "earnings_release",
                    "status": "error",
                    "text_length": 0,
                },
            ]
        )

        self.assertTrue(any("复用" in item or "抓取" in item for item in warnings))
        self.assertFalse(any("关键源材料" in item for item in warnings))

    def test_generic_parser_prefers_three_month_statement_columns_over_ytd_columns(self) -> None:
        sec_text_path = self._write_temp_text(
            "oracle-sec-ytd-placeholder.txt",
            "Condensed consolidated statements of operations.",
        )
        sec_raw_html_path = self._write_temp_text(
            "oracle-sec-ytd-statement.html",
            (
                "<html><body><table>"
                "<tr><th>Nine months ended</th><th>May 31, 2025</th><th>May 31, 2024</th><th>Three months ended</th><th>May 31, 2025</th><th>May 31, 2024</th></tr>"
                "<tr><th>(In millions)</th><th></th><th></th><th></th><th></th></tr>"
                "<tr><td>Total revenues</td><td>42,100</td><td>38,700</td><td>14,100</td><td>13,050</td></tr>"
                "<tr><td>Total cost of revenues</td><td>14,820</td><td>13,995</td><td>4,935</td><td>4,698</td></tr>"
                "<tr><td>Gross profit</td><td>27,280</td><td>24,705</td><td>9,165</td><td>8,352</td></tr>"
                "<tr><td>Research and development</td><td>6,322</td><td>6,005</td><td>2,115</td><td>2,020</td></tr>"
                "<tr><td>Selling, general and administrative</td><td>9,245</td><td>8,870</td><td>3,102</td><td>2,950</td></tr>"
                "<tr><td>Operating income</td><td>11,713</td><td>9,830</td><td>3,948</td><td>3,382</td></tr>"
                "<tr><td>Income before taxes</td><td>10,820</td><td>9,110</td><td>3,650</td><td>3,121</td></tr>"
                "<tr><td>Net income</td><td>10,102</td><td>8,864</td><td>3,400</td><td>3,036</td></tr>"
                "</table></body></html>"
            ),
        )

        parsed = parse_official_materials(
            get_company("oracle"),
            {"calendar_quarter": "2025Q2", "fiscal_label": "2025Q2"},
            [
                {
                    "label": "Oracle Form 10-Q",
                    "kind": "sec_filing",
                    "status": "cached",
                    "text_path": sec_text_path,
                    "raw_path": sec_raw_html_path,
                }
            ],
        )

        self.assertAlmostEqual(parsed["latest_kpis"]["revenue_bn"], 14.1, places=3)
        self.assertAlmostEqual(parsed["latest_kpis"]["net_income_bn"], 3.4, places=3)
        self.assertAlmostEqual(parsed["latest_kpis"]["gaap_gross_margin_pct"], 65.0, places=1)

    def test_costco_parser_extracts_legacy_release_table_metrics(self) -> None:
        release_path = self._write_temp_text(
            "costco-legacy-release.txt",
            (
                "COSTCO WHOLESALE CORPORATION CONSOLIDATED STATEMENTS OF INCOME "
                "(amounts in millions, except per value and share data) "
                "16 Weeks Ended 17 Weeks Ended 52 Weeks Ended 53 Weeks Ended "
                "REVENUE Net sales $ 43,414 $ 41,357 $ 138,434 $ 126,172 "
                "Membership fees 997 943 3,142 2,853 "
                "Total revenue 44,411 42,300 141,576 129,025 "
                "NET INCOME ATTRIBUTABLE TO COSTCO $ 1,043 $ 919 $ 3,130 $ 2,679 "
            ),
        )

        parsed = parse_official_materials(
            get_company("costco"),
            {"fiscal_label": "2018Q3", "coverage_notes": []},
            [
                {"label": "Costco legacy earnings release", "kind": "official_release", "status": "cached", "text_path": release_path},
            ],
        )

        self.assertAlmostEqual(parsed["latest_kpis"]["revenue_bn"], 44.411, places=3)
        self.assertAlmostEqual(parsed["latest_kpis"]["net_income_bn"], 1.043, places=3)
        segments = {item["name"]: item for item in parsed["current_segments"]}
        self.assertAlmostEqual(segments["Net sales"]["value_bn"], 43.414, places=3)
        self.assertAlmostEqual(segments["Membership fees"]["value_bn"], 0.997, places=3)

    def test_jnj_parser_extracts_table_metrics_from_official_992_supplement(self) -> None:
        release_path = self._write_temp_text(
            "jnj-legacy-992.txt",
            (
                "Exhibit 99.2O\n"
                "Johnson & Johnson and Subsidiaries\n"
                "Condensed Consolidated Statement of Earnings\n"
                "FOURTH QUARTER\n"
                "2010\n"
                "2009\n"
                "Percent Increase (Decrease)\n"
                "Sales to customers\n"
                "$ 15,644\n"
                "100.0\n"
                "$ 16,551\n"
                "100.0\n"
                "(5.5)\n"
                "Net earnings\n"
                "$ 1,942\n"
                "12.4\n"
                "$ 2,206\n"
                "13.3\n"
                "(12.0)\n"
                "Net earnings per share (Diluted)\n"
                "$ 0.70\n"
                "$ 0.79\n"
                "(11.4)\n"
                "Johnson & Johnson and Subsidiaries\n"
                "Supplementary Sales Data\n"
                "FOURTH QUARTER\n"
                "Sales to customers by\n"
                "segment of business\n"
                "Consumer\n"
                "U.S.\n"
                "1,219\n"
                "1,712\n"
                "(28.8)\n"
                "International\n"
                "2,391\n"
                "2,537\n"
                "(5.8)\n"
                "Worldwide\n"
                "3,610\n"
                "4,249\n"
                "(15.0)\n"
                "Pharmaceutical\n"
                "U.S.\n"
                "2,817\n"
                "3,093\n"
                "(8.9)\n"
                "International\n"
                "4,039\n"
                "3,725\n"
                "8.4\n"
                "Worldwide\n"
                "6,856\n"
                "6,818\n"
                "0.6\n"
                "Medical Devices and Diagnostics\n"
                "U.S.\n"
                "1,691\n"
                "1,733\n"
                "(2.4)\n"
                "International\n"
                "3,487\n"
                "3,751\n"
                "(7.0)\n"
                "Worldwide\n"
                "5,178\n"
                "5,484\n"
                "(5.6)\n"
                "Sales to customers by\n"
                "geographic area\n"
                "U.S.\n"
                "5,727\n"
                "6,538\n"
                "(12.4)\n"
                "Europe\n"
                "3,171\n"
                "3,298\n"
                "(3.8)\n"
                "Western Hemisphere excluding U.S.\n"
                "1,143\n"
                "1,065\n"
                "7.3\n"
                "Asia-Pacific, Africa\n"
                "5,603\n"
                "5,650\n"
                "(0.8)\n"
                "Worldwide\n"
                "15,644\n"
                "16,551\n"
                "(5.5)\n"
            ),
        )

        parsed = parse_official_materials(
            get_company("jnj"),
            {"fiscal_label": "2010Q4", "coverage_notes": []},
            [
                {"label": "Johnson & Johnson exhibit99.2 supplementary sales data", "kind": "official_release", "status": "cached", "text_path": release_path},
            ],
        )

        self.assertAlmostEqual(parsed["latest_kpis"]["revenue_bn"], 15.644, places=3)
        self.assertAlmostEqual(parsed["latest_kpis"]["net_income_bn"], 1.942, places=3)
        self.assertAlmostEqual(parsed["latest_kpis"]["gaap_eps"], 0.70, places=2)
        self.assertTrue(
            any("supplementary sales data" in note.lower() for note in parsed["coverage_notes"])
        )

    def test_generic_parser_extracts_oracle_metrics_and_segments(self) -> None:
        release_path = self._write_temp_text(
            "oracle-release.txt",
            (
                "Oracle announces fiscal 2026 third quarter financial results. "
                "Q3 total revenue was $17.2 billion, up 22%. "
                "Cloud revenue was $8.9 billion, up 44%. "
                "Software revenue was $6.1 billion, up 3%. "
                "Hardware revenue was $0.7 billion, up 2%. "
                "Services revenue was $1.4 billion, up 12%. "
                "Operating income was $5.5 billion, up 25%. "
                "Net income was $3.7 billion, up 27%. "
                "Earnings per share was $1.27, up 25%. "
                "“Oracle Cloud Infrastructure demand remains extraordinarily strong,” said Safra Catz."
            ),
        )
        sec_path = self._write_temp_text(
            "oracle-sec.txt",
            (
                "Revenues: Cloud $ 8,914 $ 6,210 Software 6,119 5,926 Hardware 714 703 Services 1,443 1,291 "
                "Total revenues 17,190 14,130 48,173 41,496 "
                "Operating expenses: Cloud and software 4,776 2,882 Hardware 183 197 Services 1,133 1,116 "
                "Sales and marketing 2,052 2,119 Research and development 2,607 2,429 General and administrative 389 390 "
                "Total operating expenses 11,726 9,772 33,700 28,927 Operating income 5,464 4,358 14,473 12,569 "
                "Net income 3,721 2,936 12,783 9,016 Diluted 1.27 1.02 "
            ),
        )

        parsed = parse_official_materials(
            get_company("oracle"),
            {"fiscal_label": "FY2026 Q3", "coverage_notes": []},
            [
                {"label": "Oracle earnings release", "kind": "official_release", "status": "cached", "text_path": release_path},
                {"label": "Oracle Form 10-Q", "kind": "sec_filing", "status": "cached", "text_path": sec_path},
            ],
        )

        self.assertAlmostEqual(parsed["latest_kpis"]["revenue_bn"], 17.2, places=1)
        self.assertAlmostEqual(parsed["latest_kpis"]["net_income_bn"], 3.7, places=1)
        self.assertAlmostEqual(parsed["latest_kpis"]["gaap_eps"], 1.27, places=2)
        self.assertEqual(parsed["current_segments"][0]["name"], "Cloud")
        self.assertEqual(parsed["current_segments"][1]["name"], "Software")
        self.assertGreaterEqual(len(parsed["call_quote_cards"]), 1)
        self.assertTrue(any("动态" in item for item in parsed["coverage_notes"]))

    def test_normalize_segment_items_reorders_generic_company_to_official_sequence(self) -> None:
        company = get_company("oracle")
        unordered = [
            {"name": "Services", "value_bn": 1.4},
            {"name": "Cloud", "value_bn": 8.9},
            {"name": "Hardware", "value_bn": 0.7},
            {"name": "Software", "value_bn": 6.1},
        ]

        normalized = _normalize_segment_items(company, unordered)

        self.assertEqual([item["name"] for item in normalized], ["Cloud", "Software", "Hardware", "Services"])

    def test_normalize_segment_items_falls_back_to_profile_order_when_company_order_missing(self) -> None:
        company = get_company("walmart")
        company["segment_order"] = []
        unordered = [
            {"name": "Sam's Club U.S.", "value_bn": 23.0},
            {"name": "Walmart International", "value_bn": 32.0},
            {"name": "Walmart U.S.", "value_bn": 121.0},
        ]

        normalized = _normalize_segment_items(company, unordered)

        self.assertEqual(
            [item["name"] for item in normalized],
            ["Walmart U.S.", "Walmart International", "Sam's Club U.S."],
        )

    def test_normalize_segment_items_maps_historical_aliases_to_canonical_taxonomy(self) -> None:
        company = get_company("apple")
        unordered = [
            {"name": "Software, service and other sales", "value_bn": 5.0},
            {"name": "iPad and related products and services", "value_bn": 4.0},
            {"name": "Total Macintosh net sales", "value_bn": 3.0},
            {"name": "iPhone and related products and services", "value_bn": 12.0},
        ]

        normalized = _normalize_segment_items(company, unordered)

        self.assertEqual(
            [item["name"] for item in normalized],
            ["iPhone", "Mac", "iPad", "Services"],
        )

    def test_quarter_reference_terms_use_fiscal_quarter_for_non_calendar_year_companies(self) -> None:
        company = get_company("apple")

        calendar_terms, fiscal_terms, target_years, allowed_quarters = _quarter_reference_terms(
            company,
            "2011Q3",
            "2011-09-24",
        )

        self.assertEqual(allowed_quarters, {4})
        self.assertIn("2011", target_years)
        self.assertIn("q4 fy2011", fiscal_terms)
        self.assertNotIn("q3", calendar_terms)

    def test_quarter_reference_terms_include_period_end_year_for_non_calendar_year_companies(self) -> None:
        company = get_company("apple")

        calendar_terms, fiscal_terms, target_years, allowed_quarters = _quarter_reference_terms(
            company,
            "2021Q4",
            "2021-12-25",
        )

        self.assertEqual(allowed_quarters, {1})
        self.assertIn("2021", target_years)
        self.assertIn("2022", target_years)
        self.assertIn("q1 fy2022", fiscal_terms)
        self.assertIn("quarter ended december 2021", fiscal_terms)
        self.assertIn("2021", calendar_terms)
        self.assertIn("2022", calendar_terms)

    def test_quarter_reference_terms_keep_calendar_quarter_for_calendar_year_companies(self) -> None:
        company = get_company("alphabet")

        calendar_terms, fiscal_terms, target_years, allowed_quarters = _quarter_reference_terms(
            company,
            "2025Q4",
            "2025-12-31",
        )

        self.assertIn(4, allowed_quarters)
        self.assertIn("2025", target_years)
        self.assertIn("q4", calendar_terms)
        self.assertIn("q4 fy2025", fiscal_terms)

    def test_apple_legacy_geographies_extracts_quarterly_region_rows(self) -> None:
        sample = (
            "Americas net sales $ 9,218 $ 6,092 51% "
            "Europe net sales 7,256 5,024 44% "
            "Japan net sales 1,433 783 83% "
            "Asia-Pacific net sales 4,987 1,813 175% "
            "Retail net sales 3,847 1,971 95% "
            "Total net sales $ 26,741 $ 15,683 71%"
        )

        extracted = _apple_legacy_geographies(sample)

        self.assertEqual(
            [item["name"] for item in extracted],
            ["Americas", "Europe", "Asia-Pacific", "Retail", "Japan"],
        )
        self.assertAlmostEqual(extracted[0]["value_bn"], 9.218, places=3)
        self.assertAlmostEqual(extracted[1]["yoy_pct"], 44.0, places=1)

    def test_alphabet_statement_svgs_keep_all_six_official_business_groups(self) -> None:
        statement = {
            "company_id": "alphabet",
            "fiscal_label": "Q4 2025",
            "period_end": "2025-12-31",
            "revenue_bn": 113.8,
            "revenue_yoy_pct": 18.0,
            "gross_profit_bn": 68.0,
            "gross_margin_pct": 59.8,
            "cost_of_revenue_bn": 45.8,
            "operating_profit_bn": 35.9,
            "operating_margin_pct": 31.5,
            "operating_expenses_bn": 32.1,
            "net_profit_bn": 34.5,
            "net_margin_pct": 30.3,
            "business_groups": [
                {"name": "Google Search & other", "value_bn": 63.1, "yoy_pct": 17.0, "color": "#2563EB"},
                {"name": "YouTube ads", "value_bn": 11.4, "yoy_pct": 9.0, "color": "#DC2626"},
                {"name": "Google Network", "value_bn": 7.8, "yoy_pct": -3.0, "color": "#F59E0B"},
                {"name": "Google subscriptions, platforms, and devices", "value_bn": 13.6, "yoy_pct": 17.0, "color": "#16A34A"},
                {"name": "Google Cloud", "value_bn": 17.7, "yoy_pct": 48.0, "color": "#60A5FA"},
                {"name": "Other Bets", "value_bn": 0.4, "yoy_pct": -2.0, "color": "#64748B"},
            ],
            "opex_breakdown": [
                {"name": "Research and development", "value_bn": 16.4, "pct_of_revenue": 14.4, "color": "#E11D48"},
                {"name": "Sales and marketing", "value_bn": 8.6, "pct_of_revenue": 7.6, "color": "#FB7185"},
                {"name": "General and administrative", "value_bn": 3.6, "pct_of_revenue": 3.1, "color": "#F97316"},
            ],
            "below_operating_items": [{"name": "Taxes net of other income", "value_bn": 1.5, "color": "#D92D20"}],
        }
        colors = {
            "Google Search & other": "#2563EB",
            "YouTube ads": "#DC2626",
            "Google Network": "#F59E0B",
            "Google subscriptions, platforms, and devices": "#16A34A",
            "Google Cloud": "#60A5FA",
            "Other Bets": "#64748B",
        }

        income_svg = render_income_statement_svg(statement, colors, "#2563EB")
        translation_svg = render_statement_translation_svg(statement, "#2563EB", "#0F172A")

        for expected in ("$7.8B", "$13.6B", "$0.4B"):
            self.assertIn(expected, income_svg)
            self.assertIn(expected, translation_svg)
        self.assertIn("Other Bets", income_svg)

if __name__ == "__main__":
    unittest.main()
