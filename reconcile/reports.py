# from main import process_result
import configparser
import sys
from typing import Any

from database import Database
from utils import query_output_writer

# Load configuration
config = configparser.ConfigParser()
config.read("setup.cfg")
CONF_SECTION = "reconcile-test" if "pytest" in sys.modules else "reconcile"
REPORTS_DIR = config.get(CONF_SECTION, "reports_dir")

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
REPORT_IA_LINKS_TO_OL_BUT_OL_EDITION_HAS_NO_OCAID_JSONL = config.get(
    CONF_SECTION, "report_ia_links_to_ol_but_ol_edition_has_no_ocaid_jsonl"
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


def process_result(result: list[Any], out_file: str, message: str) -> None:
    """
    Template to reduce repetition in processing the query results.

    :param list result: query output
    :param str out_file: filename into which to write the query output
    :param str message: description of the query result
    """
    count = len(result)
    dedupe_count = len(set(result))
    query_output_writer(result, out_file)
    print(f"{message}: {count:,}")
    print(f"De-duplicated count: {dedupe_count:,}")
    print(f"Results written to {out_file}")


def query_ol_id_differences(
    db: Database, out_file: str = REPORT_OL_IA_BACKLINKS
) -> None:
    """
    Query the database to find archive.org item links to Open Library editions
    that do not themselves link back to the original archive.org item.

    :param Database db: an instance of the database.py class.
    :param str out_file: path to the report output.
    """
    # TODO: There seem to be serious problems with this. Could it be
    # because some entries on both sides will have many editions, and they
    # all link semi-randomly to each other, such that they're all links to
    # the same work but not the same edition? Does this matter? How to
    # filter these results out from the others?

    # Get the results, count them, and write the results to a TSV.
    message = "Total (ostensibly) broken back-links to Open Library"
    result = db.get_ol_ia_id_differences()
    process_result(result, out_file, message)


def get_ol_has_ocaid_but_ia_has_no_ol_edition(
    db: Database, out_file: str = REPORT_OL_HAS_OCAID_IA_HAS_NO_OL_EDITION
) -> None:
    """
    Get rows where Open Library has an Internet Archive OCAID, but for that
    Internet Archive record there is no Open Library edition.

    :param Database db: an instance of the database.py class.
    :param str out_file: path to the report output.
    """
    # Get the results, count them, and write the results to a TSV.
    message = "Total Internet Archive records where an Open Library Edition has an OCAID but Internet Archive has no Open Library Edition"  # noqa E501
    result = db.get_ocaid_where_ol_edition_has_ocaid_and_ia_has_no_ol_edition()
    process_result(result, out_file, message)


def get_ol_has_ocaid_but_ia_has_no_ol_edition_join(
    db: Database,
    out_file: str = REPORT_OL_HAS_OCAID_IA_HAS_NO_OL_EDITION_JOIN,
) -> None:
    """
    Get rows where Open Library has an Internet Archive OCAID, but for that
    Internet Archive record there is no Open Library edition, except this time using
    a database join rather than the Open Library values inserted into the Internet
    Archive table.

    :param Database db: an instance of the database.py class.
    :param str out_file: path to the report output.
    """
    # Get the results, count them, and write the results to a TSV.
    message = "Total Internet Archive records where an Open Library Edition has an OCAID but Internet Archive has no Open Library Edition"  # noqa E501
    result = db.get_ocaid_where_ol_edition_has_ocaid_and_ia_has_no_ol_edition_join()
    process_result(result, out_file, message)


def get_editions_with_multiple_works(
    db: Database, out_file: str = REPORT_EDITIONS_WITH_MULTIPLE_WORKS
) -> None:
    """
    Get rows where on Open Library Edition contains multiple Works.

    :param Database: an instance of the database.py class.
    :param str out_file: path to the report output.
    """
    message = (
        "Total Open Library Editions with more than on associated work"  # noqa E501
    )
    result = db.get_editions_with_multiple_works()
    process_result(result, out_file, message)


def get_ia_links_to_ol_but_ol_edition_has_no_ocaid(
    db: Database,
    out_file: str = REPORT_IA_LINKS_TO_OL_BUT_OL_EDITION_HAS_NO_OCAID,
) -> None:
    """
    Get Internet Archive OCAIDs and corresponding Open Library Edition IDs where
    Internet Archive links to an Open Library Edition, but the Edition has no OCAID.

    :param Database: an instance of the database.py class.
    :param str out_file: path to the report output.
    """
    message = "Total Internet Archive items that link to an Open Library Edition, and that Edition does not have an OCAID"  # noqa E501
    result = db.get_ia_links_to_ol_but_ol_edition_has_no_ocaid()
    process_result(result, out_file, message)


def get_ia_links_to_ol_but_ol_edition_has_no_ocaid_jsonl(
    db: Database,
    out_file: str = REPORT_IA_LINKS_TO_OL_BUT_OL_EDITION_HAS_NO_OCAID_JSONL,
) -> None:
    """
    Get Internet Archive OCAIDs and corresponding Open Library Edition IDs where
    Internet Archive links to an Open Library Edition, but the Edition has no OCAID.

    :param Database: an instance of the database.py class.
    :param str out_file: path to the report output.
    """
    message = "Total Internet Archive items that link to an Open Library Edition, and that Edition does not have an OCAID (per the JSONL dump, ensuring the IA item has only one ISBN 13)"  # noqa E501
    result = db.get_ia_links_to_ol_but_ol_edition_has_no_ocaid_jsonl()
    process_result(result, out_file, message)


def get_ol_edition_has_ocaid_but_no_ia_source_record(
    db: Database,
    out_file: str = REPORT_OL_EDITION_HAS_OCAID_BUT_NO_IA_SOURCE_RECORD,
) -> None:
    """
    Get Open Library Editions where the row has on OCAID but no 'ia:<ocaid>' value
    within

    :param Database: an instance of the database.py class.
    :param str out_file: path to the report output.
    """
    message = "Total Open Library Editions that have an OCAID but have no Internet Archive entry in their source_records"  # noqa E501
    result = db.get_ol_edition_has_ocaid_but_no_ia_source_record()
    process_result(result, out_file, message)


def get_ia_with_same_ol_edition_id(
    db: Database, out_file: str = REPORT_IA_WITH_SAME_OL_EDITION
) -> None:
    """
    Get (Internet Archive OCAID, Open Library Edition ID) pairings where the Open
    Library edition ID is associated with more than one Internet Archive OCAID.

    NOTE: Many of these duplicates are because the Internet Archive dump includes
    the same OCAID with many different ISBNs, and in doing so it links, usually, to
    the same Open Library edition ID.
    """
    message = (
        "Total Archive.org items with the same Open Library edition ID"  # noqa E501
    )
    result = db.get_ia_id_with_same_ol_edition_id()
    process_result(result, out_file, message)


def get_broken_ol_ia_backlinks_after_edition_to_work_resolution0(
    db: Database,
    out_file: str = REPORT_BROKEN_OL_IA_BACKLINKS_AFTER_EDITION_TO_WORK_RESOLUTION0,
) -> None:
    message = "Broken backlinks without many false positive"
    result = db.get_broken_ol_ia_backlinks_after_edition_to_work_resolution0()
    process_result(result, out_file, message)


def get_broken_ol_ia_backlinks_after_edition_to_work_resolution1(
    db: Database,
    out_file: str = REPORT_BROKEN_OL_IA_BACKLINKS_AFTER_EDITION_TO_WORK_RESOLUTION1,
) -> None:
    message = "Broken backlinks many false positives. (Merge candidates?)"
    result = db.get_broken_ol_ia_backlinks_after_edition_to_work_resolution1()
    process_result(result, out_file, message)
