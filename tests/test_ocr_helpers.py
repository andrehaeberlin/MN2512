import numpy as np

from ocr import _join_with_conf, _resize_max


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
