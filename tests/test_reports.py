import configparser
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from lmdbm import Lmdb

import reconcile.reports as reports
from reconcile.database import Database
from reconcile.main import (
    build_ia_ol_edition_to_ol_work_column,
    copy_db_column,
    create_ia_table,
    create_ol_table,
    create_redirects_db,
    create_resolved_edition_work_mapping,
    update_redirected_ids,
)

# Load configuration
config = configparser.ConfigParser()
config.read("setup.cfg")
CONF_SECTION = "reconcile-test" if "pytest" in sys.modules else "reconcile"
FILES_DIR = config.get(CONF_SECTION, "files_dir")
REPORTS_DIR = config.get(CONF_SECTION, "reports_dir")
IA_PHYSICAL_DIRECT_DUMP = config.get(CONF_SECTION, "ia_physical_direct_dump")
OL_ALL_DUMP = config.get(CONF_SECTION, "ol_all_dump")
OL_DUMP_PARSED_PREFIX = config.get(CONF_SECTION, "ol_dump_parse_prefix")
SQLITE_DB = config.get(CONF_SECTION, "sqlite_db")
REPORT_ERRORS = config.get(CONF_SECTION, "report_errors")
REPORT_BAD_ISBNS = config.get(CONF_SECTION, "report_bad_isbns")
REPORT_OL_IA_BACKLINKS = config.get(CONF_SECTION, "report_ol_ia_backlinks")
REPORT_OL_HAS_OCAID_IA_HAS_NO_OL_EDITION = config.get(
    CONF_SECTION, "report_ol_has_ocaid_ia_has_no_ol_edition"
)
REPORT_OL_HAS_OCAID_IA_HAS_NO_OL_EDITION_JOIN = config.get(
    CONF_SECTION, "report_ol_has_ocaid_ia_has_no_ol_edition_join"
)
REPORT_EDITIONS_WITH_MULTIPLE_WORKS = config.get(
    CONF_SECTION, "report_edition_with_multiple_works"
)
REPORT_IA_LINKS_TO_OL_BUT_OL_EDITION_HAS_NO_OCAID = config.get(
    CONF_SECTION, "report_ia_links_to_ol_but_ol_edition_has_no_ocaid"
)
REPORT_OL_EDITION_HAS_OCAID_BUT_NO_IA_SOURCE_RECORD = config.get(
    CONF_SECTION, "report_ol_edition_has_ocaid_but_no_source_record"
)
REPORT_IA_WITH_SAME_OL_EDITION = config.get(
    CONF_SECTION, "report_get_ia_with_same_ol_edition"
)
REPORT_BROKEN_OL_IA_BACKLINKS_AFTER_EDITION_TO_WORK_RESOLUTION0 = config.get(
    CONF_SECTION, "report_broken_ol_ia_backlinks_after_edition_to_work_resolution0"
)
REPORT_BROKEN_OL_IA_BACKLINKS_AFTER_EDITION_TO_WORK_RESOLUTION1 = config.get(
    CONF_SECTION, "report_broken_ol_ia_backlinks_after_edition_to_work_resolution1"
)


@pytest.fixture(autouse=True)
def cleanup():
    """
    multiprocessing.pool.imap_unordered() needs to serialize its arguments, and passing
    a database connection led to serialization issues. Because each connection to an
    SQLite :memory: database is a unique DB, and because of difficult passing arguments
    through imap_unordered(), this is a way to use environment variables for the
    database, for everythting to use the same database, and for there to be some
    clean-up.
    """
    # Theoretical setup.

    # Tests run here.
    yield

    # Cleanup
    # db_file = Path(SQLITE_DB)
    # if db_file.is_file():
    #     db_file.unlink()

    error_file = Path(REPORT_ERRORS)
    if error_file.is_file():
        error_file.unlink()

    path = Path(OL_DUMP_PARSED_PREFIX)
    files = Path(FILES_DIR).glob(f"{path.stem}*{path.suffix}")
    for file in files:
        file.unlink()


@pytest.fixture(scope="session")
def setup_db(tmp_path_factory) -> Iterator:
    """
    Setup a database to  use for the session
    """
    d = tmp_path_factory.mktemp("data")
    sqlite_db = d / "sqlite.db"
    redirectdb = d / "redirect.db"
    mapdb = d / "edition_to_work_map.db"

    # Get database connections
    db = Database(sqlite_db)
    redirect_db: Lmdb = Lmdb.open(str(redirectdb), "c")
    map_db: Lmdb = Lmdb.open(str(mapdb), "c")

    # Do initial database setup and data insertion.
    create_ia_table(db)
    create_ol_table(db)

    create_redirects_db(redirect_db, OL_DUMP_PARSED_PREFIX)

    print("Copying tables to save time when resolving the redirects.")
    copy_db_column(db, "ia", "ia_ol_work_id", "resolved_ia_ol_work_id")
    copy_db_column(db, "ol", "ol_work_id", "resolved_ol_work_id")
    copy_db_column(db, "ol", "ol_edition_id", "resolved_ol_edition_id")
    db.commit()

    print("Resolving the redirects so there are consistent ID references.")
    update_redirected_ids(
        db, "ia", "ia_ol_work_id", "resolved_ia_ol_work_id", redirect_db
    )
    update_redirected_ids(db, "ol", "ol_work_id", "resolved_ol_work_id", redirect_db)
    db.commit()

    print("Creating the edition -> work mapping")
    # TODO: needs status bar
    create_resolved_edition_work_mapping(db, map_db)

    print("Building the edition-> work table in ia")
    # TODO: needs status bar
    build_ia_ol_edition_to_ol_work_column(db, redirect_db, map_db)

    yield (db)


def test_query_ol_id_differences(setup_db):
    """
    Verify that broken backlinks from an Open Library edition to an Internet
    Archive record and back are properly detected.
    """
    db = setup_db
    reports.query_ol_id_differences(db, REPORT_OL_IA_BACKLINKS)
    file = Path(REPORT_OL_IA_BACKLINKS)
    assert file.is_file() is True
    assert (
        file.read_text()
        == "jesusdoctrineofa0000heye\tOL1000000M\tOL000000W\tOL1003296M\tOL000000W\t\nenvironmentalhea00moel_0\tOL1000001M\tOL000001W\tOL1003612M\tOL000001W\t\nbacklink_diff_editions_same_work\tOL001M\tOL001W\tOL003M\tOL003W\tOL003W\nbacklink_diff_editions_diff_work\tOL006M\tOL006W\tOL004M\tOL007W\t\nbacklink_diff_editions_diff_work_no_work_redirect\tOL008M\tOL008W\tOL009M\tOL008W\t\n"  # noqa E501
    )


def test_get_records_where_ol_has_ocaid_but_ia_has_no_ol_edition(setup_db):
    """
    Verify that records where an Open Library edition has an Internet
    Archive OCAID but for that Internet Archive record there is no Open
    Library edition.
    """
    db = setup_db
    reports.get_ol_has_ocaid_but_ia_has_no_ol_edition(
        db, REPORT_OL_HAS_OCAID_IA_HAS_NO_OL_EDITION
    )
    file = Path(REPORT_OL_HAS_OCAID_IA_HAS_NO_OL_EDITION)
    assert file.is_file() is True
    assert file.read_text() == "jewishchristiand0000boys\tOL1001295M\n"


def test_get_editions_with_multiple_works(setup_db) -> None:
    """
    Verify records with multiple works are located and written to disk.
    """
    db = setup_db
    reports.get_editions_with_multiple_works(db, REPORT_EDITIONS_WITH_MULTIPLE_WORKS)
    file = Path(REPORT_EDITIONS_WITH_MULTIPLE_WORKS)
    assert file.is_file() is True
    assert file.read_text() == "OL1002158M\n"


def test_get_ol_has_ocaid_but_ia_has_no_ol_edition_union(setup_db) -> None:
    """
    Same as the other ol -> ocaid -> missing link query, but with a union.
    """
    db = setup_db
    reports.get_ol_has_ocaid_but_ia_has_no_ol_edition_join(
        db, REPORT_OL_HAS_OCAID_IA_HAS_NO_OL_EDITION_JOIN
    )
    file = Path(REPORT_OL_HAS_OCAID_IA_HAS_NO_OL_EDITION_JOIN)
    assert file.is_file() is True
    assert file.read_text() == "jewishchristiand0000boys\tOL1001295M\n"


def test_get_ia_links_to_ol_but_ol_edition_has_no_ocaid(setup_db) -> None:
    """
    Verify records where Internet Archive links to an Open Library Edition, but Open
    Library doesn't link back from that Edition, are written to a file.
    """
    # TODO: Is there any way to do this where this can catch one way links on either
    # side? E.g. IA links to an OL Edition of a work, and that specific Edition doesn't
    # link back, but another Edition of the Work links back to a *different* OCAID, and
    # yet IA doesn't link back from that other OCAID.
    # Maybe some way to check: if an IA OCID links to an OL Edition, then for all
    # Editions of the Work, does any edition link to an OCAID?
    db = setup_db
    reports.get_ia_links_to_ol_but_ol_edition_has_no_ocaid(
        db, REPORT_IA_LINKS_TO_OL_BUT_OL_EDITION_HAS_NO_OCAID
    )
    file = Path(REPORT_IA_LINKS_TO_OL_BUT_OL_EDITION_HAS_NO_OCAID)
    assert file.is_file() is True
    assert file.read_text() == "climbersguidetot00rope\tOL5214872M\n"


def test_get_ol_edition_has_ocaid_but_no_ia_source_record(setup_db) -> None:
    """
    Verify the script finds and records Open Library Editions with an OCAID record
    that do not have an 'ia:<ocaid>' record.
    """
    db = setup_db
    reports.get_ol_edition_has_ocaid_but_no_ia_source_record(
        db, REPORT_OL_EDITION_HAS_OCAID_BUT_NO_IA_SOURCE_RECORD
    )
    file = Path(REPORT_OL_EDITION_HAS_OCAID_BUT_NO_IA_SOURCE_RECORD)
    assert file.is_file() is True
    assert file.read_text() == "guidetojohnmuirt0000star\tOL5756837M\n"


def test_get_ia_with_same_ol_edition(setup_db) -> None:
    """
    Verify that Archive.org items with the same Open Library edition are reported.
    """
    db = setup_db
    reports.get_ia_with_same_ol_edition_id(db, REPORT_IA_WITH_SAME_OL_EDITION)
    file = Path(REPORT_IA_WITH_SAME_OL_EDITION)
    assert file.is_file() is True
    assert file.read_text() == "blobbook\tOL0000001M\ndifferentbook\tOL0000001M\n"


def test_get_broken_ol_ia_backlinks_after_edition_to_work_resolution0(
    setup_db,
) -> None:
    db = setup_db
    reports.get_broken_ol_ia_backlinks_after_edition_to_work_resolution0(
        db, REPORT_BROKEN_OL_IA_BACKLINKS_AFTER_EDITION_TO_WORK_RESOLUTION0
    )
    file = Path(REPORT_BROKEN_OL_IA_BACKLINKS_AFTER_EDITION_TO_WORK_RESOLUTION0)
    assert file.is_file() is True
    assert "backlink_diff_editions_diff_work" in file.read_text()
    assert "backlink_diff_editions_same_work" not in file.read_text()


def test_get_broken_ol_ia_backlinks_after_edition_to_work_resolution1(
    setup_db,
) -> None:
    db = setup_db
    reports.get_broken_ol_ia_backlinks_after_edition_to_work_resolution1(
        db, REPORT_BROKEN_OL_IA_BACKLINKS_AFTER_EDITION_TO_WORK_RESOLUTION1
    )
    file = Path(REPORT_BROKEN_OL_IA_BACKLINKS_AFTER_EDITION_TO_WORK_RESOLUTION1)
    assert file.is_file() is True
    assert "backlink_diff_editions_diff_work_no_work_redirect" in file.read_text()
    assert "backlink_diff_editions_diff_work" in file.read_text()
    assert "navigazionidiiac0000ramu" not in file.read_text()


# def test_all_reports(setup_db) -> None:
#     """This just cleans up and verifies reports.all_reports() is facially working."""
#     db = setup_db
#     # Ensure no old reports remain.
#     reports = Path(FILES_DIR).glob("report_*")
#     for report in reports:
#         report.unlink()

#     all_reports(db)
#     report_count = 0  # noqa SIM113
#     reports = Path(FILES_DIR).glob("report_*")
#     for report in reports:
#         report.unlink()
#         report_count += 1

#     assert report_count == 7
