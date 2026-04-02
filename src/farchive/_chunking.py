"""Content-defined chunking via FastCDC (pyfastcdc Cython backend).

Optional: requires the ``chunking`` extra (``pyfastcdc``).
If pyfastcdc is not installed, chunking is unavailable but the archive still works.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

try:
    from pyfastcdc import FastCDC

    _CHUNKING_AVAILABLE = True
except ImportError:
    _CHUNKING_AVAILABLE = False


@dataclass(frozen=True, slots=True)
class Chunk:
    """A single content-defined chunk with its SHA-256 digest."""

    offset: int
    length: int
    digest: str  # SHA-256 hex
    data: bytes


def chunk_data(
    raw: bytes,
    *,
    avg_size: int = 256 * 1024,
    min_size: int = 64 * 1024,
    max_size: int = 1024 * 1024,
) -> list[Chunk]:
    """Split *raw* into content-defined chunks using FastCDC 2020.

    Raises ImportError if pyfastcdc is not installed.
    """
    if not _CHUNKING_AVAILABLE:
        raise ImportError(
            "pyfastcdc is required for chunking. "
            "Install with: pip install farchive[chunking]"
        )
    cdc = FastCDC(avg_size=avg_size, min_size=min_size, max_size=max_size)
    chunks: list[Chunk] = []
    for c in cdc.cut_buf(raw):
        chunk_bytes = bytes(c.data)
        digest = hashlib.sha256(chunk_bytes).hexdigest()
        chunks.append(Chunk(offset=c.offset, length=c.length, digest=digest, data=chunk_bytes))
    return chunks
