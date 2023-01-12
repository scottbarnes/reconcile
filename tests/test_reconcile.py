import configparser
import sys

# import csv
from pathlib import Path

import pytest

from reconcile import __version__
from reconcile.database import Database
from reconcile.datatypes import ParsedEdition
from reconcile.main import create_ia_jsonl_table, create_ia_table, create_ol_table
from reconcile.openlibrary_editions import (
    insert_ol_cover_data_into_cover_db,
    process_edition_line,
)
from reconcile.utils import (
    bufcount,
    get_bad_isbn_10s,
    get_bad_isbn_13s,
    path_check,
    record_errors,
)

# Load configuration
config = configparser.ConfigParser()
config.read("setup.cfg")
CONF_SECTION = "reconcile-test" if "pytest" in sys.modules else "reconcile"
FILES_DIR = config.get(CONF_SECTION, "files_dir")
REPORTS_DIR = config.get(CONF_SECTION, "reports_dir")
IA_PHYSICAL_DIRECT_DUMP = config.get(CONF_SECTION, "ia_physical_direct_dump")
IA_INLIBRARY_JSONL_DUMP = config.get(CONF_SECTION, "ia_inlibrary_jsonl_dump")
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

    path = Path(OL_DUMP_PARSED_PREFIX)
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
    create_ia_jsonl_table(db, IA_INLIBRARY_JSONL_DUMP)
    # Specify a size to test chunking.
    create_ol_table(db, OL_ALL_DUMP, size=15_000)  # Size must be identical everywhere.
    yield db  # See the Database class


#####################################
# Reading, parsing and inserting data
#####################################


def test_create_db_inserts_data() -> None:
    """
    Get an item from the ia and ol tables. The data is seeded in from
    seed_ol_dump_all.txt.
    """
    db = Database(SQLITE_DB)
    create_ia_table(db)
    create_ol_table(db)
    db.execute(
        """SELECT ia_id, ia_ol_edition_id FROM ia WHERE ia_ol_edition_id =
        'OL1426680M'"""
    )
    assert db.fetchall() == [("goldenass0000apul_k5d0", "OL1426680M")]

    db.execute("""SELECT * FROM ol WHERE ol_edition_id = 'OL1002158M'""")
    assert db.fetchall() == [
        (
            "OL1002158M",
            "OL1883432W",
            "organizinggenius0000benn",
            "9780201570519",
            1,
            1,
            1,
            "",
            "",
        )
    ]


def test_insert_ol_cover_data_into_cover_db() -> None:
    """
    Ensure the cover DB gets created and populated properly. The
    data is seeded from seed_ol_dump_all.txt
    """
    db = Database(":memory:")
    create_ia_table(db)  # Just for the side effect of creating the processed files.
    create_ol_table(db)  # Just for the side effect of creating the processed files.
    insert_ol_cover_data_into_cover_db(db=db)

    # 9781933060224 is preset multiple times in seed_ol_dump_all.txt,
    # so this tests the unique constraint on the primary key.
    db.execute(
        """SELECT isbn_13, cover_exists FROM EditionCoverData WHERE
              isbn_13 = '9781933060224'"""
    )
    assert db.fetchall() == [("9781933060224", 1)]

    # 9780465052998 has no cover, so it shouldn't be here.
    db.execute(
        """SELECT isbn_13, cover_exists FROM EditionCoverData WHERE
              isbn_13 = '9780465052998'"""
    )
    assert db.fetchall() == []


def test_get_items_from_ia_jsonl_table(setup_db) -> None:
    db = setup_db
    line1 = (
        1,
        "links_to_ol_edition_but_ol_does_not_link_to_it",
        "OL010M",
        1,
        0,
        "9781933060224",
        '{"identifier": "links_to_ol_edition_but_ol_does_not_link_to_it", "isbn": ["1933060220", "9781933060224"], "openlibrary_work": "OL010W", "openlibrary_edition": "OL010M"}\n',  # noqa E501
    )
    line2 = (
        2,
        "links_both_ways",
        "OL011M",
        1,
        0,
        "9781451675504",
        '{"identifier": "links_both_ways", "isbn": ["9781451675504", "145167550X"], "openlibrary_work": "OL011W", "openlibrary_edition": "OL011M"}\n',  # noqa E501
    )
    line3 = (
        3,
        "links_to_ol_edition_but_ol_does_not_link_to_it_two_isbn_13",
        "OL012M",
        0,
        1,
        "9781933060224",
        '{"identifier": "links_to_ol_edition_but_ol_does_not_link_to_it_two_isbn_13", "isbn": ["1933060220", "9781933060224", "9781566199094"], "openlibrary_work": "OL012W", "openlibrary_edition": "OL012M"}\n',  # noqa E501
    )

    db.execute("""SELECT * FROM ia_jsonl""")
    assert db.fetchall()[:3] == [line1, line2, line3]


def test_process_line() -> None:
    """Process a row from the Open Library editions dump."""

    # Edition has multiple works, ocaid, and ia source_record, and two ISBN 10s
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

    assert process_edition_line(multi_works_source_rec) == (
        ParsedEdition(
            edition_id="OL1002158M",
            work_id="OL1883432W",
            ocaid="organizinggenius0000benn",
            isbn_13="9780201570519",
            has_multiple_works=1,
            has_ia_source_record=1,
            has_cover=1,
            isbn_13s="9780201570519",
        )
    )
    assert process_edition_line(noocaid_nomulti_no_ia) == (
        ParsedEdition(
            edition_id="OL10000149M",
            work_id="OL14903292W",
            ocaid=None,
            isbn_13="9780107805548",
            has_multiple_works=0,
            has_ia_source_record=0,
            has_cover=0,
            isbn_13s="9780107805548",
        )
    )
    assert process_edition_line(no_work) == ParsedEdition(
        edition_id="OL10000149M",
        work_id=None,
        ocaid=None,
        isbn_13="9780107805548",
        has_multiple_works=0,
        has_ia_source_record=0,
        has_cover=0,
        isbn_13s="9780107805548",
    )


# Checking and logging bad ISBNs
def test_process_line_and_validate_isbn() -> None:
    p = Path(REPORT_BAD_ISBNS)
    if p.is_file():
        p.unlink()

    edition = [
        "/type/edition",
        "/books/OL10000149M",
        "2",
        "2010-03-11T23:51:36.723486",
        r"""{"isbn_13": ["9780107805548", "XYZ", ""], "isbn_10": ["0107805545", "X111111111"]}""",  # noqa E501
    ]
    process_edition_line(edition)
    assert "XYZ" in p.read_text()
    assert "X111111111" in p.read_text()


# TODO: Consider refactoring how ISBNs are handled throughout.
# Test editions that have multiple ISBN 13s, once all ISBNs are converted from 10.
# This is tested independently because the ISBNs are in a set, and normally only
# one is popped off as the functionality was just taking one ISBN 13. Rather than
# rewriting the functionality there, just test separately here.
def test_process_line_with_multiple_isbn13s() -> None:
    multi_isbn_13_source = [
        "type/edition",
        "/books/OL1002158M",
        "11",
        "2021-02-12T23:39:01.417876",
        r"""{"publishers": ["Addison-Wesley"], "identifiers": {"librarything": ["286951"], "goodreads": ["894978"]}, "subtitle": "the secrets of creative collaboration", "ia_box_id": ["IA150601"], "isbn_10": ["0201570513", "145167550X"], "isbn_13": ["1234567890123"], "covers": [3858623], "ia_loaded_id": ["organizinggenius00benn"], "lc_classifications": ["HD58.9 .B45 1997"], "key": "/books/OL1002158M", "authors": [{"key": "/authors/OL225457A"}], "publish_places": ["Reading, Mass"], "contributions": ["Biederman, Patricia Ward."], "pagination": "xvi, 239 p. ;", "source_records": ["marc:marc_records_scriblio_net/part25.dat:199740929:947", "marc:marc_cca/b10621386.out:27805251:1544", "ia:organizinggenius00benn", "marc:marc_loc_2016/BooksAll.2016.part25.utf8:105728045:947", "ia:organizinggenius0000benn"], "title": "Organizing genius", "dewey_decimal_class": ["158.7"], "notes": {"type": "/type/text", "value": "Includes bibliographical references (p. 219-229) and index.\n\"None of us is as smart as all of us.\""}, "number_of_pages": 239, "languages": [{"key": "/languages/eng"}], "lccn": ["96041454"], "subjects": ["Organizational effectiveness -- Case studies", "Strategic alliances (Business) -- Case studies", "Creative thinking -- Case studies", "Creative ability in business -- Case studies"], "publish_date": "1997", "publish_country": "mau", "by_statement": "Warren Bennis, Patricia Ward Biederman.", "works": [{"key": "/works/OL1883432W"}, {"key": "/works/OL0000000W"}], "type": {"key": "/type/edition"}, "ocaid": "organizinggenius0000benn", "latest_revision": 11, "revision": 11, "created": {"type": "/type/datetime", "value": "2008-04-01T03:28:50.625462"}, "last_modified": {"type": "/type/datetime", "value": "2021-02-12T23:39:01.417876"}}""",  # noqa E501
    ]
    edition = process_edition_line(multi_isbn_13_source)

    # The order of the ISBN 13s isn't stable, so just check that each one is there.
    assert "1234567890123" in edition.isbn_13s
    assert "9780201570519" in edition.isbn_13s
    assert "9781451675504" in edition.isbn_13s


################
# Data scrubbing
################


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


def test_get_bad_isbn_10s() -> None:
    """Verify bad ISBN 10s are found and accuraterly reported."""
    isbn_10s = ["", "a", "083693133X", "1111111111", "0836931335", "9780735211308"]
    assert (
        get_bad_isbn_10s(isbn_10s=isbn_10s).sort()
        == ["a", "083693133X", "9780735211308"].sort()
    )


def test_check_and_report_bad_isbns_13() -> None:
    """Verify bad ISBN 13s are found and accuraterly reported."""
    isbn_13s = ["a", "0836931335", "1111111111111", "9780735211308"]
    assert (
        get_bad_isbn_13s(isbn_13s=isbn_13s).sort()
        == ["a", "0836931335", "1111111111111"].sort()
    )


def test_record_errors() -> None:
    """Verify errors are written to disk"""
    filename = "record_error_test.txt"
    p = Path(filename)
    record_errors("some test error", filename)
    assert "some test error" in p.read_text()
    p.unlink()


def test_create_ia_table_exits_if_db_exists(setup_db: Database) -> None:
    with pytest.raises(SystemExit):
        db = setup_db
        create_ia_table(db)


def test_create_ol_table_exits_if_db_exists(setup_db: Database) -> None:
    with pytest.raises(SystemExit):
        db = setup_db
        create_ol_table(db)


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
