import configparser
import sys
from collections.abc import Iterator

import pytest
from lmdbm import Lmdb

from reconcile.redirect_resolver import (
    add_redirects_to_db,
    process_redirect_line,
    read_file_linearly,
)

# Load configuration
config = configparser.ConfigParser()
config.read("setup.cfg")
CONF_SECTION = "reconcile-test" if "pytest" in sys.modules else "reconcile"
# FILES_DIR = config.get(CONF_SECTION, "files_dir")
# REPORTS_DIR = config.get(CONF_SECTION, "reports_dir")
# IA_PHYSICAL_DIRECT_DUMP = config.get(CONF_SECTION, "ia_physical_direct_dump")
OL_ALL_DUMP = config.get(CONF_SECTION, "ol_all_dump")

# Constants
WORK_REDIRECT_1 = [
    "/type/redirect",
    "/books/OL001M",
    "3",
    "2010-04-14T02:53:24.620268",
    """{"created": {"type": "/type/datetime", "value": "2008-04-01T03:28:50.625462"}, "covers": [5685889], "last_modified": {"type": "/type/datetime", "value": "2010-04-14T02:53:24.620268"}, "latest_revision": 3, "location": "/books/OL002M", "key": "/books/OL001M", "type": {"key": "/type/redirect"}, "revision": 3}""",  # noqa E501
]


@pytest.fixture(scope="session")
def setup_db(tmp_path_factory) -> Iterator:
    """
    Set up a database to use for the session.
    """
    d = tmp_path_factory.mktemp("data") / "test.db"
    # db = SqliteDict(d)
    with Lmdb.open(str(d), "c") as db:
        yield db


def test_can_insert_and_get_item_from_db(setup_db) -> None:
    db = setup_db
    db[b"Mount Whitney"] = b"Usually crowded"
    # db.commit()
    assert db.get(b"Mount Whitney") == b"Usually crowded"


def test_can_get_edition_redirect_key_and_value() -> None:
    gen = process_redirect_line(WORK_REDIRECT_1)
    assert next(gen) == ("OL001M", "OL002M")


def test_can_read_a_row() -> None:
    # parsed = read_file_linearly(OL_ALL_DUMP, process_redirect_line)
    parsed = read_file_linearly(OL_ALL_DUMP)
    assert next(parsed) == ("OL001M", "OL002M")


def test_add_and_retrieve_items_from_db(setup_db) -> None:
    """
    Get an edition and a work redirect. And ensure non-redirects don't end up in
    the redirect store.
    """
    db = setup_db
    add_redirects_to_db(db, OL_ALL_DUMP)
    assert db.get("OL002M") == b"OL003M"
    assert db.get("OL003M") is None
    assert db.get("OL002W") == b"OL003W"
