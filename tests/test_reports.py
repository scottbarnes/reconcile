import configparser
import sys
from pathlib import Path

import pytest
from database import Database
from main import create_ia_table, create_ol_table

import reports

# Load configuration
config = configparser.ConfigParser()
config.read("setup.cfg")
CONF_SECTION = "reconcile-test" if "pytest" in sys.modules else "reconcile"
FILES_DIR = config.get(CONF_SECTION, "files_dir")
REPORTS_DIR = config.get(CONF_SECTION, "reports_dir")
IA_PHYSICAL_DIRECT_DUMP = config.get(CONF_SECTION, "ia_physical_direct_dump")
OL_EDITIONS_DUMP = config.get(CONF_SECTION, "ol_editions_dump")
OL_EDITIONS_DUMP_PARSED = config.get(CONF_SECTION, "ol_editions_dump_parsed")
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
    db_file = Path(SQLITE_DB)
    if db_file.is_file():
        db_file.unlink()

    error_file = Path(REPORT_ERRORS)
    if error_file.is_file():
        error_file.unlink()

    path = Path(OL_EDITIONS_DUMP_PARSED)
    files = Path(FILES_DIR).glob(f"{path.stem}*{path.suffix}")
    for file in files:
        file.unlink()


@pytest.fixture()
def setup_db():
    """
    Setup the database table, populate Internet Archive data, and yield a
    Database instance to use.
    """
    db = Database(SQLITE_DB)
    create_ia_table(db, IA_PHYSICAL_DIRECT_DUMP)
    # Specify a size to test chunking.
    create_ol_table(db, OL_EDITIONS_DUMP, size=10_000)
    yield db  # See the Database class


def test_query_ol_id_differences(setup_db: Database):
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
        == "jesusdoctrineofa0000heye\tOL1000000M\tOL000000W\tOL1003296M\t\t\nenvironmentalhea00moel_0\tOL1000001M\tOL0000001W\tOL1003612M\t\t\nol_to_ia_to_ol_backlink_diff_editions_same_work\tOL001M\tOL001W\tOL003M\t\t\n"  # noqa E501
    )


def test_get_records_where_ol_has_ocaid_but_ia_has_no_ol_edition(setup_db: Database):
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


def test_get_editions_with_multiple_works(setup_db: Database) -> None:
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


def test_get_ia_links_to_ol_but_ol_edition_has_no_ocaid(setup_db: Database) -> None:
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


def test_get_ol_edition_has_ocaid_but_no_ia_source_record(setup_db: Database) -> None:
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


def test_get_ia_with_same_ol_edition(setup_db: Database) -> None:
    """
    Verify that Archive.org items with the same Open Library edition are reported.
    """
    db = setup_db
    reports.get_ia_with_same_ol_edition_id(db, REPORT_IA_WITH_SAME_OL_EDITION)
    file = Path(REPORT_IA_WITH_SAME_OL_EDITION)
    assert file.is_file() is True
    assert file.read_text() == "blobbook\tOL0000001M\ndifferentbook\tOL0000001M\n"


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
