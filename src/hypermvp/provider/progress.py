"""
Progress tracking utilities for the provider ETL pipeline.

- Provides a consistent progress bar for all ETL steps.
- Supports both sequential and parallel processing with progress indication.
- Includes a helper to format file sizes for logging.
"""

from typing import List, Iterable, TypeVar, Any, Optional, Callable
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import progress bar settings from provider_etl_config
from .provider_etl_config import PROGRESS_BAR_COLOR, PROGRESS_BAR_DISABLE

T = TypeVar('T')

def progress_bar(
    iterable: Iterable[T],
    desc: str,
    total: Optional[int] = None,
    unit: str = "it"
) -> Iterable[T]:
    """
    Wraps an iterable with a tqdm progress bar using consistent styling.

    Args:
        iterable: Items to iterate over.
        desc: Description text for the progress bar.
        total: Total number of items (optional).
        unit: Unit text for the progress bar.

    Returns:
        tqdm-wrapped iterable.
    """
    return tqdm(
        iterable,
        desc=desc,
        total=total,
        unit=unit,
        colour=PROGRESS_BAR_COLOR,
        disable=PROGRESS_BAR_DISABLE
    )

def progress_map(
    func: Callable[[Any], Any],
    items: List[Any],
    desc: str,
    max_workers: int = 1
) -> List[Any]:
    """
    Apply a function to each item with progress tracking.
    Uses parallel threads if max_workers > 1.

    Args:
        func: Function to apply to each item.
        items: List of items to process.
        desc: Description for the progress bar.
        max_workers: Number of parallel workers.

    Returns:
        List of results.
    """
    results = []
    if max_workers <= 1:
        for item in progress_bar(items, desc=desc):
            results.append(func(item))
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(func, item): item for item in items}
            for future in progress_bar(as_completed(futures), desc=desc, total=len(items)):
                results.append(future.result())
    return results

def format_size(size_bytes: int) -> str:
    """
    Format file size in a human-readable string.

    Args:
        size_bytes: Size in bytes.

    Returns:
        Formatted size string (e.g., "5.2 MB").
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024 or unit == 'TB':
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024