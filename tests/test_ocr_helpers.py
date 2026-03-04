import numpy as np

from ocr import (
    _join_with_conf,
    _resize_max,
    crop_roi,
    is_blank_or_low_density,
    normalize_scale,
)


def test_join_with_conf_filters_low_confidence():
    raw = [
        [[[0, 0], [1, 0], [1, 1], [0, 1]], "TOTAL", 0.95],
        [[[0, 0], [1, 0], [1, 1], [0, 1]], "ruido", 0.10],
        ["invalid"],
    ]
    assert _join_with_conf(raw, min_conf=0.35) == "TOTAL"


def test_resize_max_reduces_width_when_needed():
    img = np.zeros((1000, 3000, 3), dtype=np.uint8)
    out = _resize_max(img, max_w=1500)
    assert out.shape[1] == 1500


def test_is_blank_or_low_density_detects_blank_page():
    blank = np.full((500, 500, 3), 255, dtype=np.uint8)
    assert is_blank_or_low_density(blank)


def test_is_blank_or_low_density_detects_content_page():
    img = np.full((500, 500, 3), 255, dtype=np.uint8)
    img[200:350, 120:380] = 0
    assert not is_blank_or_low_density(img)


def test_crop_roi_reduces_expected_area():
    img = np.zeros((1000, 1000, 3), dtype=np.uint8)
    out = crop_roi(img, top_ratio=0.1, bottom_ratio=0.1, side_ratio=0.05)
    assert out.shape[:2] == (800, 900)


def test_normalize_scale_downsizes_wide_images():
    img = np.zeros((1200, 3200, 3), dtype=np.uint8)
    out = normalize_scale(img, target_width=1600)
    assert out.shape[1] == 1600
    assert out.shape[0] == 600
