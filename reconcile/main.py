import csv  # TODO: Decide between CSV and JSON for everything?
import os
import sqlite3
import sys
from collections.abc import Iterator
from typing import Any

import fire
import orjson
from database import Database
from fetch import get_and_extract_data
from utils import nuller, query_output_writer

# Read the file paths from environment variables first, otherwise try to use the
# filenames from the download scripts.
# TODO: Should these go in a settings file? Address when looking at pathlib.
files_dir = os.getcwd() + "/files"
pathExists = os.path.exists(files_dir)
if not pathExists:
    os.makedirs(files_dir)

IA_PHYSICAL_DIRECT_DUMP = os.environ.get(
    "IA_PHYSICAL_DIRECT_DUMP", f"{files_dir}/ia_physical_direct_latest.tsv"
)
OL_EDITIONS_DUMP = os.environ.get(
    "OL_EDITIONS_DUMP", f"{files_dir}/ol_dump_editions_latest.txt"
)
OL_EDITIONS_DUMP_PARSED = os.environ.get(
    "OL_EDITIONS_DUMP_PARSED", f"{files_dir}/ol_dump_editions_latest_parsed.tsv"
)
SQLITE_DB = os.environ.get("SQLITE_DB", f"{files_dir}/ol_test.db")
# Reports
REPORT_OL_IA_BACKLINKS = os.environ.get(
    "REPORT_OL_IA_BACKLINKS", f"{files_dir}/report_ol_ia_backlinks.tsv"
)
REPORT_OL_HAS_OCAID_IA_HAS_NO_OL_EDITION = os.environ.get(
    "REPORT_OL_HAS_OCAID_IA_HAS_NO_OL_EDITION",
    f"{files_dir}/report_ol_has_ocaid_ia_has_no_ol_edition.tsv",
)
REPORT_OL_HAS_OCAID_IA_HAS_NO_OL_EDITION_JOIN = os.environ.get(
    "REPORT_OL_HAS_OCAID_IA_HAS_NO_OL_EDITION_JOIN",
    f"{files_dir}/report_ol_has_ocaid_ia_has_no_ol_edition_join.tsv",
)
REPORT_EDITIONS_WITH_MULTIPLE_WORKS = os.environ.get(
    "REPORT_EDITIONS_WITH_MULTIPLE_WORKS",
    f"{files_dir}/report_edition_with_multiple_works.tsv",
)
REPORT_IA_LINKS_TO_OL_BUT_OL_EDITION_HAS_NO_OCAID = os.environ.get(
    "REPORT_IA_LINKS_TO_OL_BUT_OL_EDITION_HAS_NO_OCAID",
    f"{files_dir}/report_ia_links_to_ol_but_ol_edition_has_no_ocaid.tsv",
)
REPORT_OL_EDITION_HAS_OCAID_BUT_NO_IA_SOURCE_RECORD = os.environ.get(
    "REPORT_OL_EDITION_HAS_OCAID_BUT_NO_IA_SOURCE_RECORD",
    f"{files_dir}/report_ol_edition_has_ocaid_but_no_source_record.tsv",
)

# Custom types
# IA_JSON = dict[str, Union[str, list[str], list[dict[str, str]], dict[str, str]]]


class Reconciler:
    """Class object for working witht the various IA <-> OL data
    reconciliation functions."""

    def create_ia_table(
        self, db: Database, ia_dump: str = IA_PHYSICAL_DIRECT_DUMP
    ) -> None:
        """
        Populate the DB with IA data from the [date]_inlibrary_direct.tsv
        dump, available https://archive.org/download/ia-abc-historical-data.
        This data is all entered first and therefore doesn't rely on anything
        else being in there.

        :param Database db: an instance of the database.py class.
        :param str ia_dump: path to ia_physical_direct.tsv.
        """
        # TODO: Find out if it's usual to open and close the session from
        # within a function such as this, or to pass the session as an argument
        # to the function, and handle opening/commiting/closing outside the
        # function.

        # Create the ia table.
        try:
            db.execute(
                "CREATE TABLE ia (ia_id TEXT, ia_ol_edition_id TEXT, \
                ia_ol_work_id TEXT, ol_edition_id TEXT, ol_aid TEXT)"
            )
            # Indexing ia_id massively speeds up adding OL records.
            db.execute("CREATE INDEX ia_idx ON ia(ia_id)")
        except sqlite3.OperationalError as err:
            print(f"SQLite error: {err}")
            sys.exit(1)

        # Populate the DB with IA physical direct dump data.
        with open(ia_dump, newline="") as file:
            reader = csv.reader(file, delimiter="\t")
            for row in reader:
                # TODO: Is this 'better' than try/except?
                if len(row) < 4:
                    continue

                # Get the IDs, though some are empty strings.
                # Format: id<tab>ia_id<tab>ia_ol_edition<tab>ia_ol_work
                ia_id, ia_ol_edition_id, ia_ol_work_id = row[1], row[2], row[3]

                # TODO: Is there any point to setting values to Null rather
                # than None in the DB?
                db.execute(
                    "INSERT INTO ia VALUES (?, ?, ?, ?, ?)",
                    (
                        nuller(ia_id),
                        nuller(ia_ol_edition_id),
                        nuller(ia_ol_work_id),
                        None,
                        None,
                    ),
                )
        db.commit()

    def create_ol_table(
        self, db: Database, ol_dump_parsed: str = OL_EDITIONS_DUMP_PARSED
    ) -> None:
        """
        Create Open Library table and populate it.

        :param Database db: an instance of the database.py class
        :param str ol_dump_parsed: path to the parsed Open Library Editions dump
        """
        # Create the OL table.
        try:
            db.execute(
                "CREATE TABLE ol (ol_edition_id TEXT, ol_work_id TEXT, \
                ol_ocaid TEXT, has_multiple_works INTEGER, \
                has_ia_source_record INTEGER)"
            )
            # Indexing massively increases performance. TODO: Index other keys?
            db.execute("CREATE INDEX ol_idx ON ol(ol_edition_id)")
        except sqlite3.OperationalError as err:
            print(f"SQLite error: {err}")
            sys.exit(1)

        # Populate the DB with IA physical direct dump data.
        with open(ol_dump_parsed, newline="") as file:
            reader = csv.reader(file, delimiter="\t")
            for row in reader:
                # TODO: Is this 'better' than try/except?
                if len(row) < 4:
                    continue

                # Get the IDs, though some are empty strings.
                # Format: id<tab>ia_id<tab>ia_ol_edition<tab>ia_ol_work
                (
                    ol_edition_id,
                    ol_work_id,
                    ol_ocaid,
                    has_multiple_works,
                    has_ia_source_record,
                ) = (
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                )

                # TODO: Is there any point to setting values to Null rather
                # than None in the DB?
                db.execute(
                    "INSERT INTO ol VALUES (?, ?, ?, ?, ?)",
                    (
                        nuller(ol_edition_id),
                        nuller(ol_work_id),
                        nuller(ol_ocaid),
                        int(has_multiple_works),
                        int(has_ia_source_record),
                    ),
                )
        db.commit()

    def parse_ol_dump_and_write_ids(
        self, in_file: str = OL_EDITIONS_DUMP, out_file: str = OL_EDITIONS_DUMP_PARSED
    ) -> None:
        """
        Parse an Open Library editions dump from
        https://openlibrary.org/developers/dumps and write the output to a .tsv in
        the format:
        ol_edition_id\tol_work_id\tol_ocaid\thas_multiple_works\thas_ia_source_record

        :param str in_file: path to the unparsed Open Library editions dump.
        :param str out_file: path where the parsed Open Library editions dump will go.
        """
        # Fix for: _csv.Error: field larger than field limit (131072)
        csv.field_size_limit(sys.maxsize)
        # For each row in the Open Library editions dump, get the edition ID
        # and the associated JSON, and from the JSON extract the ocaid record
        # and the work ID.
        with open(out_file, "w") as outfile, open(in_file) as infile:
            writer = csv.writer(outfile, delimiter="\t")
            reader = csv.reader(infile, delimiter="\t")

            for row in reader:
                # Skip if no JSON.
                if len(row) < 5:
                    continue
                # Annotate some variables to make this a bit cleaner. Maybe
                d: dict[str, Any]
                ol_edition_id: str
                ol_ocaid: str
                ol_work_id: list[dict[str, str]] = []
                has_multiple_works: int = 0  # No boolean in SQLite
                has_ia_source_record: int = 0

                # Parse the JSON
                d = orjson.loads(row[4])
                # TODO: Check how these empty strings are being stored in the database
                # (e.g. None, "", Null, or what)
                ol_ocaid = d.get("ocaid", "")
                ol_edition_id = d.get("key", "").split("/")[-1]

                if work_id := d.get("works"):
                    ol_work_id = work_id[0].get("key").split("/")[-1]
                    has_multiple_works = int(len(work_id) > 1)

                if (source_records := d.get("source_records")) and isinstance(
                    source_records, list
                ):
                    # Check if each record has an "ia:" in it. If any does, return True
                    # and convert to 1 for SQLite, and 0 otherwise.
                    has_ia_source_record = int(
                        any(
                            [
                                "ia" in record
                                for record in source_records
                                if record is not None
                            ]
                        )
                    )

                writer.writerow(
                    [
                        ol_edition_id,
                        ol_work_id,
                        ol_ocaid,
                        has_multiple_works,
                        has_ia_source_record,
                    ]
                )

    def create_db(self, db: Database) -> None:
        """
        Create the tables and insert the data. NOTE: You must parse the data first.

        :param Database db: an instance of the database.py class.
        """
        print("Creating (and inserting) the Internet Archive table")
        self.create_ia_table(db)
        print("Creating (and inserting) the Open Library table")
        self.create_ol_table(db)
        print("Updating the Internet Archive table with Open Library data")
        self.insert_ol_data_from_tsv(db)

    def insert_ol_data_from_tsv(
        self, db: Database, parsed_ol_data: str = OL_EDITIONS_DUMP_PARSED
    ) -> None:
        """
        Add the OL data from parse_ol_dump_and_write_ids() by collating it with
        the already inserted IA data. Specifically, for each row in the parsed
        OL .tsv, find that row's ia_id within the database, and then within the
        database update that database row to include the ol_id as noted within
        the OL tsv that we're reading.

        :param Database db: an instance of the database.py class.
        :param str parsed_ol_data: path to the parsed Open Library Editions file.
        """
        # TODO: Try an IA table and an OL table and see how fast this is doing
        # queries with an inner join. Answer: the queries are slower with the join.

        def collection() -> Iterator[tuple[str, str]]:
            with open(parsed_ol_data) as file:
                reader = csv.reader(file, delimiter="\t")
                for row in reader:
                    ol_id, ol_ocaid = row[0], row[2]

                    if ol_id and ol_ocaid:
                        yield (ol_id, ol_ocaid)

        db.executemany("UPDATE ia SET ol_edition_id = ? WHERE ia_id = ?", collection())
        db.commit()

    def process_result(self, result: list[Any], out_file: str, message: str) -> None:
        """
        Template to reduce repetition in processing the query results.

        :param list result: self.query output
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
        self, db: Database, out_file: str = REPORT_OL_IA_BACKLINKS
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
        self.process_result(result, out_file, message)

    def get_ol_has_ocaid_but_ia_has_no_ol_edition(
        self, db: Database, out_file: str = REPORT_OL_HAS_OCAID_IA_HAS_NO_OL_EDITION
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
        self.process_result(result, out_file, message)

    def get_ol_has_ocaid_but_ia_has_no_ol_edition_join(
        self,
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
        message = "Total Internet Archive records where an Open Library Edition has an OCAID but Internet Archive has no Open Library Edition (JOINS)"  # noqa E501
        result = db.get_ocaid_where_ol_edition_has_ocaid_and_ia_has_no_ol_edition_join()
        self.process_result(result, out_file, message)

    def get_editions_with_multiple_works(
        self, db: Database, out_file: str = REPORT_EDITIONS_WITH_MULTIPLE_WORKS
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
        self.process_result(result, out_file, message)

    def get_ia_links_to_ol_but_ol_edition_has_no_ocaid(
        self,
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
        self.process_result(result, out_file, message)

    def get_ol_edition_has_ocaid_but_no_ia_source_record(
        self,
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
        self.process_result(result, out_file, message)

    def all_reports(self, db: Database) -> None:
        """
        Just run all the reports because these commands are way too long to type.

        :param Database db: an instance of the database.py class.
        """
        reconciler.query_ol_id_differences(db)
        print("\n")
        reconciler.get_editions_with_multiple_works(db)
        print("\n")
        reconciler.get_ol_has_ocaid_but_ia_has_no_ol_edition(db)
        print("\n")
        reconciler.get_ol_edition_has_ocaid_but_no_ia_source_record(db)
        print("\nThe next queries use joins and are slower.\n")
        reconciler.get_ol_has_ocaid_but_ia_has_no_ol_edition_join(db)
        print("\n")
        reconciler.get_ia_links_to_ol_but_ol_edition_has_no_ocaid(db)


if __name__ == "__main__":
    reconciler = Reconciler()
    db = Database(SQLITE_DB)
    # Some functions to work around passing arguments to Fire.
    # TODO: Do this the right way, because this is so ugly/embarrassing.

    def create_db():
        """
        Create the tables and insert the data. NOTE: You must parse the data first.
        """
        reconciler.create_db(db)

    def all_reports():
        """
        Just run all the reports because these commands are way too long to type.
        """
        reconciler.all_reports(db)

    fire.Fire(
        {
            "fetch-data": get_and_extract_data,
            "parse-data": reconciler.parse_ol_dump_and_write_ids,
            "create-db": create_db,
            "all-reports": all_reports,
        }
    )
