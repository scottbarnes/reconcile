import csv
from pathlib import Path

import pytest

from reconcile import __version__
from reconcile.database import Database
from reconcile.main import Reconciler

SQLITE_DB = ":memory:"
# SQLITE_DB = "./tests/ol_test1.db"
IA_PHYSICAL_DIRECT_DUMP = "./tests/seed_ia_physical_direct.tsv"
OL_EDITIONS_DUMP = "./tests/seed_ol_dump_editions.txt"
OL_EDITIONS_DUMP_PARSED = "./tests/ol_dump_editions_parsed.tsv"
REPORT_OL_IA_BACKLINKS = "./tests/report_ol_ia_backlinks.tsv"
REPORT_OL_HAS_OCAID_IA_HAS_NO_OL_EDITION = (
    "./tests/report_ol_has_ocaid_ia_has_no_ol_edition.tsv"
)
REPORT_OL_HAS_OCAID_IA_HAS_NO_OL_EDITION_JOIN = (
    "./tests/report_ol_has_ocaid_ia_has_no_ol_edition_join.tsv"
)
REPORT_EDITIONS_WITH_MULTIPLE_WORKS = "./tests/report_editions_with_multiple_works.tsv"
REPORT_IA_LINKS_TO_OL_BUT_OL_EDITION_HAS_NO_OCAID = (
    "./tests/report_ia_links_to_ol_but_ol_edition_has_no_ocaid.tsv"
)

reconciler = Reconciler()


def test_version():
    assert __version__ == "0.1.0"


@pytest.fixture()
def setup_db():
    """
    Setup the database table, populate Internet Archive data, and yield a
    Database instance to use.
    """
    db = Database(SQLITE_DB)
    reconciler.create_ia_table(db, IA_PHYSICAL_DIRECT_DUMP)
    reconciler.create_ol_table(db, OL_EDITIONS_DUMP_PARSED)
    yield db  # See the Database class
    db.close()


# @pytest.mark.usefixtures("setup_db")
def test_get_an_ia_db_item(setup_db: Database):
    """
    Get an Internet Archive DB item to make sure inserting from our seed
    data works.
    """
    db = setup_db
    db.execute(
        """SELECT ia_id, ia_ol_edition_id FROM ia WHERE ia_ol_edition_id =
        'OL1426680M'"""
    )
    assert db.fetchall() == [("goldenass0000apul_k5d0", "OL1426680M")]


def test_get_an_ol_db_item(setup_db: Database):
    """
    Get an Internet Archive DB item to make sure inserting from our seed
    data works.
    """
    db = setup_db
    db.execute("""SELECT * FROM ol WHERE ol_edition_id = 'OL1002158M'""")
    assert db.fetchall() == [
        ("OL1002158M", "OL1883432W", "organizinggenius0000benn", 1)
    ]


def test_parse_ol_dump():
    """
    Parse the Open Library editions dump insert an item, and get an item to
    make sure it works.
    """
    reconciler.parse_ol_dump_and_write_ids(OL_EDITIONS_DUMP, OL_EDITIONS_DUMP_PARSED)
    output = []
    with open(OL_EDITIONS_DUMP_PARSED) as file:
        reader = csv.reader(file, delimiter="\t")
        output = [row for row in reader]

        assert len(output) == 13
        assert ["OL1002158M", "OL1883432W", "organizinggenius0000benn", "1"] in output
        assert ["OL10000149M"] not in output


# @pytest.mark.usefixtures("setup_db")
@pytest.fixture
def test_insert_ol_data(setup_db: Database):
    """
    Insert Open Library data into the database on the basis of the Internet
    Archive data that's already in there and get an item to make sure it's
    there.
    """
    db = setup_db
    reconciler.insert_ol_data_from_tsv(db, OL_EDITIONS_DUMP_PARSED)
    db.execute(
        """SELECT ia_id, ia_ol_edition_id, ol_edition_id FROM ia
        WHERE ia_id = 'goldenass0000apul_k5d0'"""
    )
    assert db.fetchall() == [("goldenass0000apul_k5d0", "OL1426680M", "OL1426680M")]


def test_query_ol_id_differences(setup_db, test_insert_ol_data):
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
        == "jesusdoctrineofa0000heye\tOL1000000M\tOL000000W\tOL1003296M\t\nenvironmentalhea00moel_0\tOL1000001M\tOL0000001W\tOL1003612M\t\n"  # noqa E501
    )


def test_get_records_where_ol_has_ocaid_but_ia_has_no_ol_edition(
    setup_db, test_insert_ol_data
):
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


def test_get_editions_with_multiple_works(
    setup_db: Database, test_insert_ol_data
) -> None:
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

    # TODO: Remove debug info.
    # sql = "SELECT * FROM ia WHERE ia_ol_edition_id IS NOT ol_edition_id"
    # return self.query(sql)
    print("Printing IA table")
    sql = "SELECT * FROM ia"
    result = db.query(sql)
    for row in result:
        print(row)
    assert True is True

    print("\nPrinting OL table")
    sql = "SELECT * FROM ol"
    result = db.query(sql)
    for row in result:
        print(row)
    assert True is True
