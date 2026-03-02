"""CHIRPS v2.0 Africa monthly rainfall GeoTIFF download.

Downloads gzipped GeoTIFF files from the CHIRPS data archive, decompresses
them, and caches the results locally.

URL pattern::

    https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/
        chirps-v2.0.{year}.{month:02d}.tif.gz
"""

from __future__ import annotations

import gzip
import logging
import shutil
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

CHIRPS_BASE_URL = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/"


def build_chirps_url(year: int, month: int) -> str:
    """Construct the URL for a CHIRPS monthly rainfall GeoTIFF.

    Args:
        year: Data year.
        month: Month number (1-12).

    Returns:
        Full URL to the gzipped GeoTIFF file.
    """
    return f"{CHIRPS_BASE_URL}chirps-v2.0.{year}.{month:02d}.tif.gz"


def download_chirps(url: str, cache_dir: Path) -> Path:
    """Download and decompress a CHIRPS gzipped GeoTIFF.

    If the decompressed file already exists in ``cache_dir``, returns
    immediately (cache hit). Otherwise downloads the ``.tif.gz`` file,
    decompresses it, and removes the compressed copy.

    Args:
        url: Remote URL of the gzipped GeoTIFF.
        cache_dir: Local directory for cached downloads.

    Returns:
        Path to the decompressed GeoTIFF file.
    """
    gz_filename = url.rsplit("/", maxsplit=1)[-1]
    tif_filename = gz_filename.removesuffix(".gz")
    dest = cache_dir / tif_filename

    if dest.exists():
        logger.info("Cache hit: %s", dest.name)
        return dest

    cache_dir.mkdir(parents=True, exist_ok=True)
    gz_dest = cache_dir / gz_filename

    logger.info("Downloading %s", url)
    with httpx.stream("GET", url, follow_redirects=True, timeout=300) as resp:
        resp.raise_for_status()
        with gz_dest.open("wb") as fh:
            for chunk in resp.iter_bytes(chunk_size=1024 * 64):
                fh.write(chunk)

    logger.info("Decompressing %s", gz_dest.name)
    with gzip.open(gz_dest, "rb") as f_in, dest.open("wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

    gz_dest.unlink()
    logger.info("Saved %s (%.1f MB)", dest.name, dest.stat().st_size / 1e6)
    return dest
