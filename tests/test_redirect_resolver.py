import configparser
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from lmdbm import Lmdb

from reconcile.datatypes import ParsedRedirect
from reconcile.redirect_resolver import (  # read_file_linearly,
    create_redirects_db,
    process_redirect_line,
)

# Load configuration
config = configparser.ConfigParser()
config.read("setup.cfg")
CONF_SECTION = "reconcile-test" if "pytest" in sys.modules else "reconcile"
# FILES_DIR = config.get(CONF_SECTION, "files_dir")
# REPORTS_DIR = config.get(CONF_SECTION, "reports_dir")
# IA_PHYSICAL_DIRECT_DUMP = config.get(CONF_SECTION, "ia_physical_direct_dump")
OL_ALL_DUMP = config.get(CONF_SECTION, "ol_all_dump")
OL_DUMP_PARSED_PREFIX = config.get(CONF_SECTION, "ol_dump_parse_prefix")

# Constants
WORK_REDIRECT_1 = [
    "/type/redirect",
    "/books/OL001M",
    "3",
    "2010-04-14T02:53:24.620268",
    """{"created": {"type": "/type/datetime", "value": "2008-04-01T03:28:50.625462"}, "covers": [5685889], "last_modified": {"type": "/type/datetime", "value": "2010-04-14T02:53:24.620268"}, "latest_revision": 3, "location": "/books/OL002M", "key": "/books/OL001M", "type": {"key": "/type/redirect"}, "revision": 3}""",  # noqa E501
]

AUTHOR_REDIRECT_1 = [
    "/type/redirect",
    "/authors/OL10219261A",
    "2",
    "2022-03-06T23:01:28.782362",
    '{"key": "/authors/OL10219261A", "type": {"key": "/type/redirect"}, "location": "/authors/OL3894951A", "latest_revision": 2, "revision": 2, "created": {"type": "/type/datetime", "value": "2022-02-14T22:44:21.951940"}, "last_modified": {"type": "/type/datetime", "value": "2022-03-06T23:01:28.782362"}}',  # noqa E501
]


@pytest.fixture(scope="session")
def setup_db(tmp_path_factory) -> Iterator:
    """
    Set up a database to use for the session.
    """
    r = tmp_path_factory.mktemp("data") / "resolver.db"
    m = tmp_path_factory.mktemp("data") / "edition_to_work_mapper.db"
    with Lmdb.open(str(r), "c") as resolve_db, Lmdb.open(str(m), "c") as map_db:
        yield (resolve_db, map_db)


def test_can_insert_and_get_item_from_db(setup_db) -> None:
    resolve_db, _ = setup_db
    resolve_db["Mount Whitney"] = b"Usually crowded"
    assert resolve_db.get("Mount Whitney") == b"Usually crowded"


def test_only_process_edition_and_work_redirects() -> None:
    assert process_redirect_line(WORK_REDIRECT_1) == ParsedRedirect(
        origin_id="OL001M", destination_id="OL002M"
    )
    assert process_redirect_line(AUTHOR_REDIRECT_1) is None


def test_add_and_retrieve_items_from_db(setup_db) -> None:
    """
    Get an edition and a work redirect. And ensure non-redirects don't end up in
    the redirect store.
    """
    # Hypothetical output from write_processed_chunk_lines_to_disk() for redirects.
    f = Path("tests/ol_dump_parsed_redirect_01234.txt")
    f.write_text("OL002M\tOL003M\nOL002W\tOL003W")

    resolve_db, _ = setup_db
    create_redirects_db(resolve_db, OL_DUMP_PARSED_PREFIX)
    assert resolve_db.get("OL002M") == b"OL003M"
    assert resolve_db.get("OL003M") is None
    assert resolve_db.get("OL002W") == b"OL003W"
    f.unlink()
