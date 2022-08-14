import csv  # TODO: Decide between CSV and JSON for everything?
import os
import sqlite3
import sys
from collections.abc import Iterator
from typing import Any, Union

import fire
import orjson
from database import Database
from fetch import get_and_extract_data
from utils import nuller, query_output_writer

# from collections.abc import


# Read the file paths from environment variables first, otherwise try to use the
# filenames from the download scripts.
# TODO: Should these go in a settings file? Address when looking at pathlib.
files_dir = os.getcwd() + "/files"
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

# Custom type
IA_JSON = dict[str, Union[str, list[str], list[dict[str, str]], dict[str, str]]]


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
        self, db: Database, ia_dump: str = OL_EDITIONS_DUMP_PARSED
    ) -> None:
        """
        Create Open Library table and populate it.
        """
        # Create the OL table.
        try:
            db.execute(
                "CREATE TABLE ol (ol_edition_id TEXT, ol_work_id TEXT, \
                ol_ocaid TEXT, has_multiple_works INTEGER)"
            )
            # Indexing massively increases performance. TODO: Index other keys?
            db.execute("CREATE INDEX ol_idx ON ol(ol_edition_id)")
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
                ol_edition_id, ol_work_id, ol_ocaid, has_multiple_works = (
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                )

                # TODO: Is there any point to setting values to Null rather
                # than None in the DB?
                db.execute(
                    "INSERT INTO ol VALUES (?, ?, ?, ?)",
                    (
                        nuller(ol_edition_id),
                        nuller(ol_work_id),
                        nuller(ol_ocaid),
                        int(has_multiple_works),
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
        ol_edition_id\tol_work_id\tol_ocaid\thas_multiple_works
        """
        # Fix for: _csv.Error: field larger than field limit (131072)
        csv.field_size_limit(sys.maxsize)
        # For each row in the Open Library editions dump, get the edition ID
        # and the associated JSON, and from the JSON extract the ocaid record
        # and the work ID.
        with open(out_file, "w") as outfile, open(in_file) as infile:
            writer = csv.writer(outfile, delimiter="\t")
            reader = csv.reader(infile, delimiter="\t")

            # has_multiple_works = []
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

                # Parse the JSON
                d = orjson.loads(row[4])
                # TODO: Check how these empty strings are being stored in the database
                # (e.g. None, "", Null, or what)
                ol_ocaid = d.get("ocaid", "")
                ol_edition_id = d.get("key", "").split("/")[-1]

                if work_id := d.get("works"):
                    ol_work_id = work_id[0].get("key").split("/")[-1]
                    has_multiple_works = int(len(work_id) > 1)

                writer.writerow(
                    [ol_edition_id, ol_work_id, ol_ocaid, has_multiple_works]
                )

    def insert_ol_data_from_tsv(
        self, db: Database, parsed_ol_data: str = OL_EDITIONS_DUMP_PARSED
    ) -> None:
        """
        Add the OL data from parse_ol_dump_and_write_ids() by collating it with
        the already inserted IA data. Specifically, for each row in the parsed
        OL .tsv, find that row's ia_id within the database, and then within the
        database update that database row to include the ol_id as noted within
        the OL tsv that we're reading.
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

    def query_ol_id_differences(
        self, db: Database, out_file: str = REPORT_OL_IA_BACKLINKS
    ) -> None:
        """
        Query the database to find archive.org item links to Open Library editions
        that do not themselves link back to the original archive.org item.
        """
        # TODO: There seem to be serious problems with this. Could it be
        # because some entries on both sides will have many editions, and they
        # all link semi-randomly to each other, such that they're all links to
        # the same work but not the same edition? Does this matter? How to
        # filter these results out from the others?

        # Get the results, count them, and write the results to a TSV.
        result = db.get_ol_ia_id_differences()
        count = len(result)
        dedupe_count = len(set(result))
        query_output_writer(result, out_file)

        print(f"Total (ostensibly) broken back-links: {count:,}")
        print(f"De-duplicated count: {dedupe_count:,}")
        print(f"Results written to {out_file}")

    def get_ol_has_ocaid_but_ia_has_no_ol_edition(
        self, db: Database, out_file: str = REPORT_OL_HAS_OCAID_IA_HAS_NO_OL_EDITION
    ) -> None:
        """
        Get rows where Open Library has an Internet Archive OCAID, but for that
        Internet Archive record there is no Open Library edition.
        """
        # Get the results, count them, and write the results to a TSV.
        result = db.get_ocaid_where_ol_edition_has_ocaid_and_ia_has_no_ol_edition()
        count = len(result)
        dedupe_count = len(set(result))
        query_output_writer(result, out_file)

        print(
            f"Total Internet Archive records where an Open Library Edition has an OCAID but Internet Archive has no Open Library Edition: {count:,}"  # noqa E501
        )
        print(f"De-duplicated count: {dedupe_count:,}")
        print(f"Results written to {out_file}")

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
        """
        # Get the results, count them, and write the results to a TSV.
        result = db.get_ocaid_where_ol_edition_has_ocaid_and_ia_has_no_ol_edition_join()
        count = len(result)
        dedupe_count = len(set(result))
        query_output_writer(result, out_file)

        print(
            f"Total Internet Archive records where an Open Library Edition has an OCAID but Internet Archive has no Open Library Edition (JOINS): {count:,}"  # noqa E501
        )
        print(f"De-duplicated count (JOINS): {dedupe_count:,}")
        print(f"Results written to {out_file}")

    def get_editions_with_multiple_works(
        self, db: Database, out_file: str = REPORT_EDITIONS_WITH_MULTIPLE_WORKS
    ) -> None:
        result = db.get_editions_with_multiple_works()
        count = len(result)
        dedupe_count = len(set(result))
        query_output_writer(result, out_file)

        print(
            f"Total Open Library Editions with more than on associated work: {count:,}"  # noqa E501
        )
        print(f"De-duplicated count: {dedupe_count:,}")
        print(f"Results written to {out_file}")

    def get_ia_links_to_ol_but_ol_edition_has_no_ocaid(
        self,
        db: Database,
        out_file: str = REPORT_IA_LINKS_TO_OL_BUT_OL_EDITION_HAS_NO_OCAID,
    ) -> None:
        result = db.get_ia_links_to_ol_but_ol_edition_has_no_ocaid()
        count = len(result)
        dedupe_count = len(set(result))
        query_output_writer(result, out_file)

        print(
            f"Total Internet Archive items that link to an Open Library Edition, and that Edition does not have an OCAID: {count:,}"  # noqa E501
        )
        print(f"De-duplicated count: {dedupe_count:,}")
        print(f"Results written to {out_file}")


if __name__ == "__main__":
    reconciler = Reconciler()
    db = Database(SQLITE_DB)
    # Some functions to work around passing arguments to Fire.
    # TODO: Do this the right way, because this is so ugly/embarrassing.

    def create_db():
        """
        Create the tables and insert the data. NOTE: You must parse the data first.
        """
        print("Creating (and inserting) the Internet Archive table")
        reconciler.create_ia_table(db)
        print("Creating (and inserting) the Open Library table")
        reconciler.create_ol_table(db)
        print("Updating the Internet Archive table with Open Library data")
        reconciler.insert_ol_data_from_tsv(db)

    # def iodft():
    #     reconciler.insert_ol_data_from_tsv(db)

    # def qod():
    #     reconciler.query_ol_id_differences(db)

    # def grwohobihnoe():
    #     reconciler.get_records_where_ol_has_ocaid_but_ia_has_no_ol_edition(db)

    def all_reports():
        """
        Just run all the reports because these commands are way too long to type.
        """
        reconciler.query_ol_id_differences(db)
        print("\n")
        reconciler.get_editions_with_multiple_works(db)
        print("\n")
        reconciler.get_ol_has_ocaid_but_ia_has_no_ol_edition(db)
        print("\nThe next queries use joins and are slower.\n")
        reconciler.get_ol_has_ocaid_but_ia_has_no_ol_edition_join(db)
        print("\n")
        reconciler.get_ia_links_to_ol_but_ol_edition_has_no_ocaid(db)

    fire.Fire(
        {
            "fetch-data": get_and_extract_data,
            "parse-data": reconciler.parse_ol_dump_and_write_ids,
            "create-db": create_db,
            "all-reports": all_reports,
            # "insert-ol-data-json": reconciler.insert_ol_data_from_json,
            # "insert-ol-data": iodft,
            # "query-ol-diff": qod,
            # "query-ia-diff": grwohobihnoe,
        }
    )
