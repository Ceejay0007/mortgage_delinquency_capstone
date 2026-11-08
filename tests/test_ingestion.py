"""Unit tests for ingestion helpers. Run with: pytest -q"""

from pathlib import Path

from ingestion.freddie_mac_ingestion import classify_member, find_zips
from ingestion.fred_ingestion import series_list

def test_classify_member_new_perf_name():
    assert classify_member("sample_perf_2005.txt") == ("performance", "2005")

def test_classify_member_origination():
    assert classify_member("sample_orig_2008.txt") == ("origination", "2008")


def test_classify_member_performance_in_subfolder():
    assert classify_member("folder/sample_svcg_2020.txt") == ("performance", "2020")


def test_classify_member_ignores_other_files():
    assert classify_member("readme.pdf") is None


def test_find_zips_respects_vintage_range(tmp_path: Path):
    for y in (2004, 2005, 2010, 2023):
        (tmp_path / f"sample_{y}.zip").touch()
    years = [y for y, _ in find_zips(tmp_path, 2005, 2022)]
    assert years == [2005, 2010]


def test_series_list_expands_state_templates():
    cfg = {
        "fred": {
            "national_series": {"UNRATE": "x"},
            "state_series_templates": {"{state}UR": "y"},
            "states": ["TX", "MN"],
        }
    }
    assert series_list(cfg, national_only=False) == ["UNRATE", "TXUR", "MNUR"]
    assert series_list(cfg, national_only=True) == ["UNRATE"]
