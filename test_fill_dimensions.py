"""
test_fill_dimensions.py
-----------------------
Unit tests for fill_dimensions.py core functions.

Run: pytest test_fill_dimensions.py -v
"""

import math
import pytest
from fill_dimensions import parse_dims, normalize_sku, strip_box_suffix, _to_float, KG_TO_LBS


# ── parse_dims ────────────────────────────────────────────────────────────────

class TestParseDims:
    def test_space_x_separator(self):
        assert parse_dims("63.5 x 91.5 x 16.0") == (63.5, 91.5, 16.0)

    def test_unicode_times_separator(self):
        assert parse_dims("63.5×91.5×16.0") == (63.5, 91.5, 16.0)

    def test_no_spaces(self):
        assert parse_dims("108.00x56.70x225.00") == (108.0, 56.7, 225.0)

    def test_multipart_string_takes_first_three_decimals(self):
        # Multi-part label strings: only decimal numbers are extracted
        L, W, H = parse_dims("Panama table: 79.10\nPanama Corner: 207.10")
        assert L == pytest.approx(79.10)
        assert W == pytest.approx(207.10)
        assert H is None

    def test_none_returns_triple_none(self):
        assert parse_dims(None) == (None, None, None)

    def test_nan_returns_triple_none(self):
        assert parse_dims(float("nan")) == (None, None, None)

    def test_all_zeros_returns_triple_none(self):
        assert parse_dims("0 x 0 x 0") == (None, None, None)

    def test_tuple_passthrough(self):
        assert parse_dims((10.0, 20.0, 30.0)) == (10.0, 20.0, 30.0)

    def test_integer_dims_no_decimal(self):
        # Falls back to integer parsing when no decimals present
        L, W, H = parse_dims("63 x 91 x 16")
        assert L == pytest.approx(63.0)
        assert W == pytest.approx(91.0)
        assert H == pytest.approx(16.0)

    def test_two_values_pads_none(self):
        L, W, H = parse_dims("79.10 x 40.20")
        assert L == pytest.approx(79.10)
        assert W == pytest.approx(40.20)
        assert H is None


# ── normalize_sku ─────────────────────────────────────────────────────────────

class TestNormalizeSku:
    def test_sc_prefix_stripped(self):
        result = normalize_sku("SC GALA_3SEATER_8839_CSH")
        assert "sc" not in result.split()
        assert "gala" in result

    def test_in_prefix_stripped(self):
        result = normalize_sku("IN_IC_974_LO_B2")
        assert "in" not in result.split()

    def test_4digit_code_stripped(self):
        result = normalize_sku("SC GALA_3SEATER_8839_CSH")
        assert "8839" not in result

    def test_3digit_code_kept(self):
        # 3-digit numbers are NOT stripped (could be meaningful)
        result = normalize_sku("IN_IC_974_LO")
        assert "974" in result

    def test_box_suffix_stripped_before_normalise(self):
        # B1 and B2 variants produce the same normalised string
        assert normalize_sku("IN_IC_974_LO_B1") == normalize_sku("IN_IC_974_LO_B2")

    def test_pli_prefix_stripped(self):
        result = normalize_sku("PLI_CURAZAO_CHAIR_WHT")
        assert "pli" not in result.split()
        assert "curazao" in result

    def test_output_is_lowercase(self):
        result = normalize_sku("SC GALA_3SEATER_FRAME")
        assert result == result.lower()

    def test_no_prefix_unchanged(self):
        result = normalize_sku("BARCELONA_CHAIR")
        assert "barcelona" in result
        assert "chair" in result


# ── strip_box_suffix ──────────────────────────────────────────────────────────

class TestStripBoxSuffix:
    def test_b1_stripped(self):
        assert strip_box_suffix("IN_IC_974_LO_B1") == "IN_IC_974_LO"

    def test_b3_stripped(self):
        assert strip_box_suffix("IN_IC_974_LO_B3") == "IN_IC_974_LO"

    def test_b10_stripped(self):
        assert strip_box_suffix("SOME_ITEM_B10") == "SOME_ITEM"

    def test_no_suffix_unchanged(self):
        assert strip_box_suffix("SC GALA_3SEATER_FRAME") == "SC GALA_3SEATER_FRAME"

    def test_mid_b_not_stripped(self):
        # Only trailing Bx suffix is stripped, not a Bx in the middle
        assert "B2" in strip_box_suffix("SC_B2_CHAIR_FRAME") or \
               strip_box_suffix("SC_B2_CHAIR_FRAME") == "SC_B2_CHAIR_FRAME"


# ── _to_float ─────────────────────────────────────────────────────────────────

class TestToFloat:
    def test_positive_float(self):
        assert _to_float(14.0) == pytest.approx(14.0)

    def test_zero_returns_none(self):
        assert _to_float(0.0) is None

    def test_string_number(self):
        assert _to_float("14.5") == pytest.approx(14.5)

    def test_none_returns_none(self):
        assert _to_float(None) is None

    def test_nan_returns_none(self):
        assert _to_float(float("nan")) is None

    def test_invalid_string_returns_none(self):
        assert _to_float("n/a") is None

    def test_negative_returns_none(self):
        # Negative dimensions don't make sense
        assert _to_float(-5.0) is None


# ── Weight conversion ─────────────────────────────────────────────────────────

class TestWeightConversion:
    def test_kg_to_lbs_accuracy(self):
        # 14.0 kg → 30.865 lbs
        result = round(14.0 * KG_TO_LBS, 2)
        assert result == pytest.approx(30.86, abs=0.01)

    def test_zero_kg(self):
        assert round(0.0 * KG_TO_LBS, 2) == pytest.approx(0.0)

    def test_100_kg(self):
        assert round(100.0 * KG_TO_LBS, 2) == pytest.approx(220.46, abs=0.01)
