import configparser
import sys
from collections.abc import Iterator

import pytest
from lmdbm import Lmdb

from reconcile.database import Database
from reconcile.main import Reconciler
from reconcile.openlibrary_works import copy_db_column, update_redirected_ids
from reconcile.redirect_resolver import add_redirects_to_db

# Load configuration
config = configparser.ConfigParser()
config.read("setup.cfg")
CONF_SECTION = "reconcile-test" if "pytest" in sys.modules else "reconcile"
# FILES_DIR = config.get(CONF_SECTION, "files_dir")
# REPORTS_DIR = config.get(CONF_SECTION, "reports_dir")
IA_PHYSICAL_DIRECT_DUMP = config.get(CONF_SECTION, "ia_physical_direct_dump")
OL_ALL_DUMP = config.get(CONF_SECTION, "ol_all_dump")


@pytest.fixture(scope="session")
def setup_db(tmp_path_factory) -> Iterator:
    """
    Setup a database to  use for the session
    """
    r = Reconciler()
    d = tmp_path_factory.mktemp("data")
    sqlite_db = d / "sqlite.db"
    dictdb = d / "dict.db"
    # d = tmp_path_factory.mktemp("data") / "sqlite.db"

    # Get database connections
    db = Database(sqlite_db)
    dict_db = Lmdb.open(str(dictdb), "c")

    # Do initial database setup and data insertion.
    r.create_ia_table(db, IA_PHYSICAL_DIRECT_DUMP)  # For both just r.create_db(db)
    add_redirects_to_db(dict_db, OL_ALL_DUMP)

    yield (db, dict_db)


def test_copy_column_db_works(setup_db):
    db, _ = setup_db
    sql = """SELECT ia_ol_edition_id, ia_ol_work_id, resolved_ia_ol_work_id FROM ia WHERE ia_ol_work_id = 'OL001W'"""  # noqa E501
    # Before
    assert db.query(sql) == [("OL001M", "OL001W", None)]
    copy_db_column(db, "ia", "ia_ol_work_id", "resolved_ia_ol_work_id")
    # After
    assert db.query(sql) == [("OL001M", "OL001W", "OL001W")]


def test_ia_table_is_updated_with_resolved_redirects(setup_db):
    db, dict_db = setup_db
    sql1 = """SELECT ia_ol_edition_id, ia_ol_work_id, resolved_ia_ol_work_id FROM ia WHERE ia_ol_work_id = 'OL001W'"""  # noqa E501
    sql2 = """SELECT ia_ol_edition_id, ia_ol_work_id, resolved_ia_ol_work_id FROM ia WHERE ia_ol_work_id = 'OL002W'"""  # noqa E501
    copy_db_column(db, "ia", "ia_ol_work_id", "resolved_ia_ol_work_id")
    # Before updating redirects
    assert db.query(sql1) == [("OL001M", "OL001W", "OL001W")]
    # After
    update_redirected_ids(db, "ia", "ia_ol_work_id", "resolved_ia_ol_work_id", dict_db)
    assert db.query(sql1) == [("OL001M", "OL001W", "OL003W")]
    assert db.query(sql2) == [("OL002M", "OL002W", "OL003W")]
