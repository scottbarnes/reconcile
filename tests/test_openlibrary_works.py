import configparser
import sys
from collections.abc import Iterator

import pytest
from lmdbm import Lmdb

from reconcile.database import Database
from reconcile.main import Reconciler
from reconcile.openlibrary_works import (
    build_ia_ol_edition_to_ol_work_column,
    copy_db_column,
    create_resolved_edition_work_mapping,
    get_resolved_work_from_edition,
    update_redirected_ids,
)
from reconcile.redirect_resolver import create_redirects_db

# Load configuration
config = configparser.ConfigParser()
config.read("setup.cfg")
CONF_SECTION = "reconcile-test" if "pytest" in sys.modules else "reconcile"
IA_PHYSICAL_DIRECT_DUMP = config.get(CONF_SECTION, "ia_physical_direct_dump")
OL_ALL_DUMP = config.get(CONF_SECTION, "ol_all_dump")


@pytest.fixture()
def setup_db(tmp_path_factory) -> Iterator:
    """
    Setup a database to  use for the session
    """
    r = Reconciler()
    d = tmp_path_factory.mktemp("data")
    sqlite_db = d / "sqlite.db"
    redirectdb = d / "redirect.db"
    mapdb = d / "edition_to_work_map.db"

    # Get database connections
    db = Database(sqlite_db)
    redirect_db: Lmdb = Lmdb.open(str(redirectdb), "c")
    map_db: Lmdb = Lmdb.open(str(mapdb), "c")

    # Do initial database setup and data insertion.
    r.create_db(db)  # For both just r.create_db(db)
    create_redirects_db(redirect_db, OL_ALL_DUMP)

    yield (db, redirect_db, map_db)


@pytest.fixture()
def setup_db_full(tmp_path_factory) -> Iterator:
    """
    A fully setup database.
    """
    r = Reconciler()
    d = tmp_path_factory.mktemp("data")
    sqlite_db = d / "sqlite.db"
    redirectdb = d / "redirect.db"
    mapdb = d / "edition_to_work_map.db"

    # Get database connections
    db = Database(sqlite_db)
    redirect_db: Lmdb = Lmdb.open(str(redirectdb), "c")
    map_db: Lmdb = Lmdb.open(str(mapdb), "c")

    # Do initial database setup and data insertion.
    r.create_db(db)  # For both just r.create_db(db)
    create_redirects_db(redirect_db, OL_ALL_DUMP)
    copy_db_column(db, "ol", "ol_edition_id", "resolved_ol_edition_id")
    copy_db_column(db, "ol", "ol_work_id", "resolved_ol_work_id")
    update_redirected_ids(
        db, "ol", "ol_edition_id", "resolved_ol_edition_id", redirect_db
    )
    update_redirected_ids(db, "ol", "ol_work_id", "resolved_ol_work_id", redirect_db)

    # sql = """SELECT DISTINCT resolved_ol_edition_id, resolved_ol_work_id FROM ol"""
    # editions_and_works = db.query(sql)
    # create_resolved_edition_work_mapping(editions_and_works, map_db)
    create_resolved_edition_work_mapping(db, map_db)

    yield (db, redirect_db, map_db)


def test_copy_column_db_works(setup_db):
    db, _, _ = setup_db
    sql = """
    SELECT ia_ol_edition_id,
           ia_ol_work_id,
           resolved_ia_ol_work_id
    FROM   ia
    WHERE  ia_ol_work_id = 'OL001W'
    """
    # Before
    assert db.query(sql) == [("OL001M", "OL001W", None)]
    copy_db_column(db, "ia", "ia_ol_work_id", "resolved_ia_ol_work_id")
    # After
    assert db.query(sql) == [("OL001M", "OL001W", "OL001W")]


def test_ia_table_is_updated_with_resolved_redirects(setup_db):
    db, redirect_db, _ = setup_db
    sql1 = """
    SELECT ia_ol_edition_id,
       ia_ol_work_id,
       resolved_ia_ol_work_id
    FROM   ia
    WHERE  ia_ol_work_id = 'OL001W'
    """
    sql2 = """
    SELECT ia_ol_edition_id,
       ia_ol_work_id,
       resolved_ia_ol_work_id
    FROM   ia
    WHERE  ia_ol_work_id = 'OL002W'
    """
    copy_db_column(db, "ia", "ia_ol_work_id", "resolved_ia_ol_work_id")
    # Before updating redirects
    assert db.query(sql1) == [("OL001M", "OL001W", "OL001W")]
    # After
    update_redirected_ids(
        db, "ia", "ia_ol_work_id", "resolved_ia_ol_work_id", redirect_db
    )
    assert db.query(sql1) == [("OL001M", "OL001W", "OL003W")]
    assert db.query(sql2) == [("OL002M", "OL002W", "OL003W")]


def test_create_resolved_edition_work_mapping(setup_db_full):
    """ """
    _, _, map_db = setup_db_full
    assert map_db.get("OL003M") == b"OL003W"


def test_get_resolved_work_from_edition(setup_db_full):
    _, redirect_db, map_db = setup_db_full
    edition = "OL001M"
    assert get_resolved_work_from_edition(redirect_db, map_db, edition) == "OL003W"


def test_build_ia_ol_edition_to_ol_work_column(setup_db_full):
    """ """
    # TODO: the docstring needs better explanation elsewhere. Put it in the actual
    db, redirect_db, map_db = setup_db_full
    sql = """
    SELECT resolved_ia_ol_work_from_edition
    FROM   ia
    WHERE  ia_id = "ol_to_ia_to_ol_backlink_diff_editions_same_work"
    """
    build_ia_ol_edition_to_ol_work_column(db, redirect_db, map_db)
    assert db.query(sql) == [("OL003W",)]
