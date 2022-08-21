import configparser
import sys

# import csv
from pathlib import Path

import pytest

from reconcile import __version__
from reconcile.database import Database
from reconcile.main import Reconciler
from reconcile.openlibrary_editions import (
    make_chunk_ranges,
    process_line,
    read_and_convert_chunk,
    write_chunk_to_disk,
)
from reconcile.utils import bufcount, path_check

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

reconciler = Reconciler()

###########
# NOTE: Changing the Open Library seed data may require updates to the chunking tests.
# If changing seed data, try to do so in the latter half of the file.
###########


def test_version():
    assert __version__ == "0.1.0"


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
    reconciler.create_ia_table(db, IA_PHYSICAL_DIRECT_DUMP)
    # Specify a size to test chunking.
    reconciler.create_ol_table(db, OL_EDITIONS_DUMP, size=10_000)
    yield db  # See the Database class


#####################################
# Reading, parsing and inserting data
#####################################


def test_create_db_inserts_data() -> None:
    """Get an item from the ia and ol tables."""
    db = Database(SQLITE_DB)
    reconciler.create_db(db)
    db.execute(
        """SELECT ia_id, ia_ol_edition_id FROM ia WHERE ia_ol_edition_id =
        'OL1426680M'"""
    )
    assert db.fetchall() == [("goldenass0000apul_k5d0", "OL1426680M")]

    db.execute("""SELECT * FROM ol WHERE ol_edition_id = 'OL1002158M'""")
    assert db.fetchall() == [
        ("OL1002158M", "OL1883432W", "organizinggenius0000benn", 1, 1)
    ]


def test_process_line() -> None:
    """Process a row from the Open Library editions dump."""

    # Edition has multiple works, ocaid, and ia source_record.
    multi_works_source_rec = [
        "type/edition",
        "/books/OL1002158M",
        "11",
        "2021-02-12T23:39:01.417876",
        r"""{"publishers": ["Addison-Wesley"], "identifiers": {"librarything": ["286951"], "goodreads": ["894978"]}, "subtitle": "the secrets of creative collaboration", "ia_box_id": ["IA150601"], "isbn_10": ["0201570513"], "covers": [3858623], "ia_loaded_id": ["organizinggenius00benn"], "lc_classifications": ["HD58.9 .B45 1997"], "key": "/books/OL1002158M", "authors": [{"key": "/authors/OL225457A"}], "publish_places": ["Reading, Mass"], "contributions": ["Biederman, Patricia Ward."], "pagination": "xvi, 239 p. ;", "source_records": ["marc:marc_records_scriblio_net/part25.dat:199740929:947", "marc:marc_cca/b10621386.out:27805251:1544", "ia:organizinggenius00benn", "marc:marc_loc_2016/BooksAll.2016.part25.utf8:105728045:947", "ia:organizinggenius0000benn"], "title": "Organizing genius", "dewey_decimal_class": ["158.7"], "notes": {"type": "/type/text", "value": "Includes bibliographical references (p. 219-229) and index.\n\"None of us is as smart as all of us.\""}, "number_of_pages": 239, "languages": [{"key": "/languages/eng"}], "lccn": ["96041454"], "subjects": ["Organizational effectiveness -- Case studies", "Strategic alliances (Business) -- Case studies", "Creative thinking -- Case studies", "Creative ability in business -- Case studies"], "publish_date": "1997", "publish_country": "mau", "by_statement": "Warren Bennis, Patricia Ward Biederman.", "works": [{"key": "/works/OL1883432W"}, {"key": "/works/OL0000000W"}], "type": {"key": "/type/edition"}, "ocaid": "organizinggenius0000benn", "latest_revision": 11, "revision": 11, "created": {"type": "/type/datetime", "value": "2008-04-01T03:28:50.625462"}, "last_modified": {"type": "/type/datetime", "value": "2021-02-12T23:39:01.417876"}}""",  # noqa E501
    ]
    # No ocaid, no multiple works, no ia source_record.
    noocaid_nomulti_no_ia = [
        "/type/edition",
        "/books/OL10000149M",
        "2",
        "2010-03-11T23:51:36.723486",
        r"""{"publishers": ["Stationery Office Books"], "key": "/books/OL10000149M", "created": {"type": "/type/datetime", "value": "2008-04-30T09:38:13.731961"}, "number_of_pages": 87, "isbn_13": ["9780107805548"], "physical_format": "Hardcover", "isbn_10": ["0107805545"], "publish_date": "December 31, 1994", "last_modified": {"type": "/type/datetime", "value": "2010-03-11T23:51:36.723486"}, "authors": [{"key": "/authors/OL46053A"}], "title": "40house of Lords Official Report", "latest_revision": 2, "works": [{"key": "/works/OL14903292W"}], "type": {"key": "/type/edition"}, "revision": 2}""",  # noqa E501
    ]
    # No work
    no_work = [
        "/type/edition",
        "/books/OL10000149M",
        "2",
        "2010-03-11T23:51:36.723486",
        r"""{"publishers": ["Stationery Office Books"], "key": "/books/OL10000149M", "created": {"type": "/type/datetime", "value": "2008-04-30T09:38:13.731961"}, "number_of_pages": 87, "isbn_13": ["9780107805548"], "physical_format": "Hardcover", "isbn_10": ["0107805545"], "publish_date": "December 31, 1994", "last_modified": {"type": "/type/datetime", "value": "2010-03-11T23:51:36.723486"}, "authors": [{"key": "/authors/OL46053A"}], "title": "40house of Lords Official Report", "latest_revision": 2, "type": {"key": "/type/edition"}, "revision": 2}""",  # noqa E501
    ]

    assert process_line(multi_works_source_rec) == (
        "OL1002158M",
        "OL1883432W",
        "organizinggenius0000benn",
        1,
        1,
    )
    assert process_line(noocaid_nomulti_no_ia) == (
        "OL10000149M",
        "OL14903292W",
        None,
        0,
        0,
    )
    assert process_line(no_work) == ("OL10000149M", None, None, 0, 0)


def test_make_chunk_ranges() -> None:
    """Make sure chunk ranges create properly."""
    assert make_chunk_ranges(OL_EDITIONS_DUMP, 10_000) == [
        (0, 10884, "./tests/seed_ol_dump_editions.txt"),
        (10884, 31768, "./tests/seed_ol_dump_editions.txt"),
    ]


def test_read_and_covert_chunk() -> None:
    """Read the first entry from chunk."""
    chunk = (0, 10884, "./tests/seed_ol_dump_editions.txt")
    gen = read_and_convert_chunk(chunk)
    assert next(gen) == ("OL10000149M", "OL14903292W", None, 0, 0)  # type: ignore
    for _ in gen:
        pass


def test_write_chunk_to_disk() -> None:
    """Write a chunk to disk."""
    # Delete any existing written chunks.
    path = Path(OL_EDITIONS_DUMP_PARSED)
    files = Path(FILES_DIR).glob(f"{path.stem}*{path.suffix}")
    for file in files:
        file.unlink()

    chunk = (0, 10884, "./tests/seed_ol_dump_editions.txt")
    write_chunk_to_disk(chunk, OL_EDITIONS_DUMP_PARSED)

    # The written files have random hex strings, so use globbing to get the filenames
    # to search the chunk. Note: the search term must be contained with what would be
    # within the first chunk, as this is just writing one chunk. Something too far
    # down the unparsed file won't be in the first chunk.
    path = Path(OL_EDITIONS_DUMP_PARSED)
    files = Path(FILES_DIR).glob(f"{path.stem}*{path.suffix}")

    def find_edition(files):
        for file in files:
            print(f"Searching: {file}")
            if (
                "OL1002158M\tOL1883432W\torganizinggenius0000benn\t1\t1"
                in file.read_text()
            ):
                return True
        return False

    assert find_edition(files) is True


###########
# Utilities
###########


def test_bufcount() -> None:
    """Count the number of lines in a file."""
    f = Path("peaks.txt")
    if f.exists():
        raise Exception("peaks.txt exists.")
    f.write_text("Olancha\nPeak\n")
    assert bufcount("peaks.txt") == 2
    f.unlink()


def test_bufcount_fails_without_file() -> None:
    """Verify bufcount() fails without a file."""
    with pytest.raises(SystemExit):
        bufcount("MountBrewer.txt")


def test_path_check() -> None:
    """Verify the path creation helper utility works."""
    path = Path("Sierra_Peaks_Section")
    if path.exists():
        raise Exception("'Sierra' directory exists.")
    path_check("Sierra_Peaks_Section")
    assert path.is_dir() is True
    path.rmdir()


def test_create_ia_table_exits_if_db_exists(setup_db: Database) -> None:
    with pytest.raises(SystemExit):
        db = setup_db
        reconciler.create_ia_table(db)


def test_create_ol_table_exits_if_db_exists(setup_db: Database) -> None:
    with pytest.raises(SystemExit):
        db = setup_db
        reconciler.create_ol_table(db)


#########
# Reports
#########


def test_query_ol_id_differences(setup_db: Database):
    """
    Verify that broken backlinks from an Open Library edition to an Internet
    Archive record and back are properly detected.
    """
    db = setup_db
    reconciler.query_ol_id_differences(db, REPORT_OL_IA_BACKLINKS)
    file = Path(REPORT_OL_IA_BACKLINKS)
    assert file.is_file() is True
    assert (
        file.read_text()
        == "jesusdoctrineofa0000heye\tOL1000000M\tOL000000W\tOL1003296M\nenvironmentalhea00moel_0\tOL1000001M\tOL0000001W\tOL1003612M\n"  # noqa E501
    )


def test_get_records_where_ol_has_ocaid_but_ia_has_no_ol_edition(setup_db: Database):
    """
    Verify that records where an Open Library edition has an Internet
    Archive OCAID but for that Internet Archive record there is no Open
    Library edition.
    """
    db = setup_db
    reconciler.get_ol_has_ocaid_but_ia_has_no_ol_edition(
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
    reconciler.get_editions_with_multiple_works(db, REPORT_EDITIONS_WITH_MULTIPLE_WORKS)
    file = Path(REPORT_EDITIONS_WITH_MULTIPLE_WORKS)
    assert file.is_file() is True
    assert file.read_text() == "OL1002158M\n"


def test_get_ol_has_ocaid_but_ia_has_no_ol_edition_union(setup_db) -> None:
    """
    Same as the other ol -> ocaid -> missing link query, but with a union.
    """
    db = setup_db
    reconciler.get_ol_has_ocaid_but_ia_has_no_ol_edition_join(
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
    reconciler.get_ia_links_to_ol_but_ol_edition_has_no_ocaid(
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
    reconciler.get_ol_edition_has_ocaid_but_no_ia_source_record(
        db, REPORT_OL_EDITION_HAS_OCAID_BUT_NO_IA_SOURCE_RECORD
    )
    file = Path(REPORT_OL_EDITION_HAS_OCAID_BUT_NO_IA_SOURCE_RECORD)
    assert file.is_file() is True
    assert file.read_text() == "guidetojohnmuirt0000star\tOL5756837M\n"


def test_all_reports(setup_db) -> None:
    """This just cleans up and verifies reconciler.all_reports() is facially working."""
    db = setup_db
    # Ensure no old reports remain.
    reports = Path(FILES_DIR).glob("report_*")
    for report in reports:
        report.unlink()

    reconciler.all_reports(db)
    report_count = 0
    reports = Path(FILES_DIR).glob("report_*")
    for report in reports:
        report.unlink()
        report_count += 1

    assert report_count == 6


##################
# Some debug tests
##################

# def test_print_out_db(setup_db: Database):
#     db = setup_db
#     print("Printing IA table")
#     sql = "SELECT * FROM ia"
#     result = db.query(sql)
#     for row in result:
#         print(row)
#     assert True is True

#     print("\nPrinting OL table")
#     sql = "SELECT * FROM ol"
#     result = db.query(sql)
#     for row in result:
#         print(row)
#     assert True is True
