from __future__ import annotations

import os
import shutil
import tempfile
import time
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.config import DATA_DIR
from app.db import init_db
from app.main import app
from app.services.charts import render_income_statement_svg, render_statement_translation_svg
from app.services.local_data import get_company, get_quarter_fixture
from app.services.local_data import _build_companyfacts_series
from app.services.institutional_views import get_institutional_views
from app.services.official_parsers import parse_official_materials
from app.services.official_parsers import _apple_legacy_geographies
from app.services.official_parsers import _extract_table_metric
from app.services.official_parsers import _prefer_richer_geographies
from app.services.official_source_resolver import resolve_official_sources
from app.services.official_source_resolver import _ir_role_keywords_match, _ir_temporal_alignment
from app.services.official_source_resolver import _quarter_reference_terms
from app.services.reports import (
    REPORT_PAYLOAD_SCHEMA_VERSION,
    _backfill_historical_segment_history,
    _build_income_statement_snapshot,
    _enrich_history_with_official_structures,
    _harmonize_historical_structures,
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


class EarningsDigestStudioTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._backup_root = Path(tempfile.mkdtemp(prefix="earnings-digest-tests-"))
        self._data_backup_path = self._backup_root / "data-backup"
        self.addCleanup(self._restore_data_dir)
        self._previous_source_fetch = os.environ.get("EARNINGS_DIGEST_DISABLE_SOURCE_FETCH")
        os.environ["EARNINGS_DIGEST_DISABLE_SOURCE_FETCH"] = "1"
        self.addCleanup(self._restore_source_fetch_env)
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
            shutil.rmtree(DATA_DIR)
        if self._data_backup_path.exists():
            shutil.move(str(self._data_backup_path), str(DATA_DIR))
        shutil.rmtree(self._backup_root, ignore_errors=True)

    def _restore_source_fetch_env(self) -> None:
        if self._previous_source_fetch is None:
            os.environ.pop("EARNINGS_DIGEST_DISABLE_SOURCE_FETCH", None)
            return
        os.environ["EARNINGS_DIGEST_DISABLE_SOURCE_FETCH"] = self._previous_source_fetch

    def test_calendar_quarter_mapping_uses_majority_months(self) -> None:
        self.assertEqual(resolve_calendar_quarter_from_months(["2025-11", "2025-12", "2026-01"]), "2025Q4")

    def test_nvidia_history_cube_has_12_points_and_segments(self) -> None:
        cube = build_historical_quarter_cube("nvidia", "2025Q4", 12)
        self.assertEqual(len(cube), 12)
        self.assertEqual(cube[-1]["quarter_label"], "2025Q4")
        self.assertTrue(cube[-1]["segments"])
        self.assertEqual(resolve_structure_dimension("nvidia", cube), "segment")

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

    def test_history_cube_requires_full_window(self) -> None:
        with self.assertRaises(ValueError):
            build_historical_quarter_cube("nvidia", "2005Q4", 12)

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

    def test_home_and_company_api_return_top_20_pool(self) -> None:
        home = self.client.get("/")
        self.assertEqual(home.status_code, 200)
        self.assertIn("Earnings Digest Studio", home.text)
        self.assertIn("美股市值前 20", home.text)
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

    def tearDown(self) -> None:
        shutil.rmtree(self._backup_root, ignore_errors=True)

    def _write_temp_text(self, filename: str, content: str) -> str:
        path = self._backup_root / filename
        path.write_text(content, encoding="utf-8")
        return str(path)

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

    def test_report_cache_freshness_uses_shorter_ttl_for_recent_quarters(self) -> None:
        dependency = self._backup_root / "dep.txt"
        dependency.write_text("dep", encoding="utf-8")
        old_timestamp = time.time() - 45 * 24 * 60 * 60
        os.utime(dependency, (old_timestamp, old_timestamp))
        recent_record = {
            "updated_at": (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat(),
            "payload": {"release_date": date.today().isoformat()},
        }
        historical_record = {
            "updated_at": (datetime.now(timezone.utc) - timedelta(days=20)).isoformat(),
            "payload": {"release_date": "2020-01-31"},
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

    def test_company_quarters_service_returns_filtered_window(self) -> None:
        payload = company_quarters("nvidia", 16)
        self.assertEqual(payload["history_window"], 16)
        self.assertTrue(payload["supported_quarters"])
        self.assertNotIn("2005Q4", payload["supported_quarters"])

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
        self.assertIn("自动抓取", payload["payload"]["coverage_warnings"][0])

        preview = self.client.get(payload["preview_url"])
        self.assertEqual(preview.status_code, 200)
        self.assertIn("当前季度与官方展望语境", preview.text)
        self.assertIn("营收与开支可视化图", preview.text)
        self.assertIn("财报科目中文直译", preview.text)
        self.assertIn("官方管理层锚点", preview.text)
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
        sources = resolve_official_sources(company, "2017Q4", "2018-01-28", [], refresh=False)

        self.assertEqual(sources[0]["kind"], "official_release")
        self.assertTrue(sources[0]["url"].endswith("/q4fy18pr.htm"))
        self.assertEqual(sources[1]["kind"], "presentation")
        self.assertTrue(sources[1]["url"].endswith("/q4fy18cfocommentary.htm"))
        self.assertEqual(sources[2]["kind"], "sec_filing")
        self.assertTrue(sources[2]["url"].endswith("/nvda-2018x10k.htm"))

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
        self.assertEqual(parsed["current_geographies"][0]["scope"], "annual_filing")

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
        self.assertEqual(parsed["current_geographies"][0]["scope"], "annual_filing")
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
        self.assertEqual(parsed["current_geographies"][0]["scope"], "annual_filing")
        self.assertEqual(parsed["current_geographies"][0]["name"], "United States")

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
        self.assertTrue(any("fallback" in note.lower() for note in parsed["coverage_notes"]))

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
        self.assertEqual(parsed["current_geographies"][0]["scope"], "annual_filing")

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
        self.assertTrue(all(item.get("scope") == "annual_filing" for item in parsed["current_geographies"]))

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
