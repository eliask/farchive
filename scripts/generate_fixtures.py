#!/usr/bin/env python3
"""Generate test fixtures for farchive.

This script creates v1 schema fixtures that test forward-compatibility.
Run from repo root: python scripts/generate_fixtures.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from farchive import Farchive


def generate_v1_smoke(tmp_dir: Path) -> Path:
    """Create v1_smoke.farchive fixture.

    Creates a v1 schema archive with events enabled containing:
    - 5 locators: 4 html, 1 xml, 1 binary (raw)
    - page1 has 2 spans (content changed)
    - alias has same content as page1's latest (dedup)
    """
    db_path = tmp_dir / "v1_smoke.farchive"

    with Farchive(db_path, enable_events=True) as fa:
        # 4 html pages
        fa.store(
            "https://example.com/page1",
            b"<html><body>Hello World</body></html>",
            storage_class="html",
        )
        fa.store(
            "https://example.com/page2",
            b"<html><body>Goodbye World</body></html>",
            storage_class="html",
        )
        fa.store(
            "https://example.com/doc",
            b'<?xml version="1.0"?><doc><item>test</item></doc>',
            storage_class="xml",
        )
        fa.store(
            "https://example.com/alias",
            b"<html><body>Hello World</body></html>",
            storage_class="html",
        )

        # Update page1 with new content (creates second span)
        fa.store(
            "https://example.com/page1",
            b"<html><body>Updated content</body></html>",
            storage_class="html",
        )

        # Tiny raw blob (under 64 byte threshold)
        fa.store("loc/raw", b"tiny", storage_class="binary")

    # Verify
    with Farchive(db_path) as fa:
        assert len(fa.locators()) == 5
        assert len(fa.history("https://example.com/page1")) == 2
        events = fa.events()
        # 6 stores = 6 fa.observe + 6 fa.store = 12 events
        assert len(events) == 12, f"Expected 12 events, got {len(events)}"

    print(f"Created: {db_path}")
    return db_path


def main():
    fixtures_dir = Path(__file__).parent / "tests" / "fixtures"
    fixtures_dir.mkdir(exist_ok=True)

    generate_v1_smoke(fixtures_dir)
    print("Fixtures generated successfully")


if __name__ == "__main__":
    main()
