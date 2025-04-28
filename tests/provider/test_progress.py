import pytest
from hypermvp.provider.progress import progress_bar, progress_map, format_size

def test_progress_bar_yields_items():
    """progress_bar should yield all items in the iterable, unchanged."""
    items = [1, 2, 3]
    result = list(progress_bar(items, desc="Test"))
    assert result == items

def test_progress_map_sequential():
    """progress_map should apply the function sequentially when max_workers=1."""
    items = [1, 2, 3]
    result = progress_map(lambda x: x + 1, items, desc="Sequential", max_workers=1)
    assert result == [2, 3, 4]

def test_progress_map_parallel():
    """progress_map should apply the function in parallel when max_workers>1."""
    items = [1, 2, 3]
    result = progress_map(lambda x: x * 2, items, desc="Parallel", max_workers=2)
    # Order is not guaranteed in parallel, so sort for comparison
    assert sorted(result) == [2, 4, 6]

def test_format_size():
    """format_size should return human-readable file sizes."""
    assert format_size(500) == "500.00 B"
    assert format_size(2048) == "2.00 KB"
    assert format_size(5 * 1024 * 1024) == "5.00 MB"