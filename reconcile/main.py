import configparser
import csv
import multiprocessing as mp
import sqlite3
import sys
from pathlib import Path
from typing import Any

import fire
from database import Database
from fetch import get_and_extract_data
from lmdbm import Lmdb
from openlibrary_editions import (
    insert_ol_data_in_ol_table,
    make_chunk_ranges,
    pre_create_ol_table_file_cleanup,
    update_ia_editions_from_parsed_tsvs,
    write_chunk_to_disk,
)
from openlibrary_works import (
    build_ia_ol_edition_to_ol_work_column,
    copy_db_column,
    create_resolved_edition_work_mapping,
    update_redirected_ids,
)
from redirect_resolver import create_redirects_db
from tqdm import tqdm
from utils import bufcount, nuller, path_check, query_output_writer

# Load configuration
config = configparser.ConfigParser()
config.read("setup.cfg")
CONF_SECTION = "reconcile-test" if "pytest" in sys.modules else "reconcile"
FILES_DIR = config.get(CONF_SECTION, "files_dir")
REPORTS_DIR = config.get(CONF_SECTION, "reports_dir")
IA_PHYSICAL_DIRECT_DUMP = config.get(CONF_SECTION, "ia_physical_direct_dump")
OL_EDITIONS_DUMP = config.get(CONF_SECTION, "ol_editions_dump")
OL_EDITIONS_DUMP_PARSED = config.get(CONF_SECTION, "ol_editions_dump_parsed")
OL_ALL_DUMP = config.get(CONF_SECTION, "ol_all_dump")
SQLITE_DB = config.get(CONF_SECTION, "sqlite_db")
REDIRECT_DB = config.get(CONF_SECTION, "redirect_db")
MAPPING_DB = config.get(CONF_SECTION, "mapping_db")
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
REPORT_IA_WITH_SAME_OL_EDITION = config.get(
    CONF_SECTION, "report_get_ia_with_same_ol_edition"
)


class Reconciler:
    """Class object for working with the various IA <-> OL data
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
        # to the function, and handle opening/committing/closing outside the
        # function.

        # Create the ia table.
        try:
            # db.execute("PRAGMA synchronous = OFF")  # Speed up.
            # db.execute("PRAGMA journal_mode = MEMORY")
            db.execute(
                "CREATE TABLE ia (ia_id TEXT, ia_ol_edition_id TEXT, \
                 ia_ol_work_id TEXT, ol_edition_id TEXT, \
                 resolved_ia_ol_work_id TEXT, resolved_ia_ol_work_from_edition TEXT)"
            )
        except sqlite3.OperationalError as err:
            print(f"SQLite error: {err}")
            print(f"You may need to delete {SQLITE_DB}.")
            sys.exit(1)

        # Populate the DB with IA physical direct dump data.
        path = Path(ia_dump)
        if not path.is_file():
            print(f"Cannot find {ia_dump}.")
            print("Either `fetch-data` or check `ia_physical_direct_dump` in setup.cfg")
            sys.exit(1)
        total = bufcount(ia_dump)
        print("Inserting the Internet Archive records.")
        with open(ia_dump, newline="") as file, tqdm(total=total) as pbar:
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
                    "INSERT INTO ia VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        nuller(ia_id),
                        nuller(ia_ol_edition_id),
                        nuller(ia_ol_work_id),
                        None,
                        None,
                        None,
                    ),
                )
                pbar.update(1)
        # Indexing ia_id massively speeds up adding OL records.
        # But doing it first slows inserts.
        db.execute("CREATE INDEX idx_ia_id ON ia(ia_id)")
        db.execute("CREATE INDEX idx_ia_ol_work ON ia(ia_ol_work_id)")
        db.commit()
        # db.close()

    def create_ol_table(
        self,
        db: Database,
        filename: str = OL_EDITIONS_DUMP,
        size: int = 256 * 512 * 512,
    ):
        """
        Parse the (uncompressed) Open Library editions dump named {filename} and insert
        it into {db}.
        Dumps available at: https://openlibrary.org/developers/dumps

        Because uncompressed dump is so large, the script parallel processes the files
        in chunks of {size} bytes. For each chunk, it's read from the disk, parsed, and
        added to the database.

        :param Database db: the database connection to use.
        :param str filename: path to the unparsed Open Library editions dump.
        """
        # Ensure the input file exists.
        in_path = Path(OL_EDITIONS_DUMP)
        if not in_path.is_file():
            print(f"Cannot find {OL_EDITIONS_DUMP}.")
            print("Either `fetch-data` or check `ol_editions_dump` in setup.cfg")
            sys.exit(1)

        # Clean up output from prior runs.
        pre_create_ol_table_file_cleanup()

        # Get the chunks on which to operate.
        chunks = make_chunk_ranges(filename, size)
        # Set the number of parallel processes to cpu_count - 1.
        num_parallel = mp.cpu_count() - 1

        # Create the ol table.
        try:
            db.execute(
                "CREATE TABLE ol (ol_edition_id TEXT, ol_work_id TEXT, \
                 ol_ocaid TEXT, has_multiple_works INTEGER, \
                 has_ia_source_record INTEGER, resolved_ol_edition_id TEXT, \
                 resolved_ol_work_id TEXT)"
            )
        except sqlite3.OperationalError as err:
            print(f"SQLite error: {err}")
            print(f"You may need to delete {SQLITE_DB}.")
            sys.exit(1)

        # For each Open Library chunk, read, process, and write it.
        print("Processing Open Library editions dump and writing to disk.")
        print("Note: this progress bar is a little lumpy because of multiprocessing.")
        total = len(chunks)
        with mp.Pool(num_parallel) as pool, tqdm(total=total) as pbar:
            result = pool.imap_unordered(write_chunk_to_disk, chunks)
            for _ in result:
                pbar.update(1)

        # For each Open Library chunk, read it, process it, and INSERT it into the ol
        # table.
        print("Inserting the Open Library editions data.")
        insert_ol_data_in_ol_table(db)

        # Create the index after INSERT for performance gain.
        db.execute("CREATE INDEX idx_ol_edition ON ol(ol_edition_id)")
        db.execute("CREATE INDEX idx_ol_work ON ol(ol_work_id)")

        # For each Open Library chunk, read it, process it, and UPDATE the ia table.
        print("Now updating the Internet Archive table with Open Library data.")
        print("This shouldn't take as long.")
        update_ia_editions_from_parsed_tsvs(db)

        db.commit()

    def create_db(self, db: Database) -> None:
        """
        Create the tables and insert the data. NOTE: You must fetch the data first.

        :param Database db: an instance of the database.py class.
        """
        self.create_ia_table(db)
        self.create_ol_table(db)

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
        message = "Total Internet Archive records where an Open Library Edition has an OCAID but Internet Archive has no Open Library Edition"  # noqa E501
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

    def get_ia_with_same_ol_edition_id(
        self, db: Database, out_file: str = REPORT_IA_WITH_SAME_OL_EDITION
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
        self.process_result(result, out_file, message)

    def all_reports(self, db: Database) -> None:
        """
        Just run all the reports because these commands are way too long to type.

        :param Database db: an instance of the database.py class.
        """
        try:
            self.query_ol_id_differences(db)
            print("\n")
            self.get_editions_with_multiple_works(db)
            print("\n")
            self.get_ol_has_ocaid_but_ia_has_no_ol_edition(db)
            print("\n")
            self.get_ol_edition_has_ocaid_but_no_ia_source_record(db)
            print("\nThe next queries use joins and are slower.\n")
            self.get_ol_has_ocaid_but_ia_has_no_ol_edition_join(db)
            print("\n")
            self.get_ia_links_to_ol_but_ol_edition_has_no_ocaid(db)
            print("\n")
            self.get_ia_with_same_ol_edition_id(db)
        except sqlite3.OperationalError as err:
            print(f"SQLite error: {err}")
            if "no such table" in err.args[0]:
                print("Perhaps you need to run with `create-db` first.")
            sys.exit(1)


if __name__ == "__main__":
    # Create necessary paths
    paths = [FILES_DIR, REPORTS_DIR]
    [path_check(d) for d in paths]

    reconciler = Reconciler()
    db = Database(SQLITE_DB)
    redirect_db: Lmdb = Lmdb.open(REDIRECT_DB, "c")
    map_db: Lmdb = Lmdb.open(MAPPING_DB, "c")

    # Some functions to work around passing arguments to Fire.
    # TODO: Do this the right way, because this is so ugly/embarrassing.

    def create_db():
        """
        Parse the data, create the tables, and insert the data. NOTE: you must fetch the
        data first.
        """
        reconciler.create_db(db)

    def all_reports():
        """
        Just run all the reports because typing them individually too long.
        """
        reconciler.all_reports(db)

    def resolve_redirects():
        """
        Compile the redirects. Must refactor the command line portion. This is so
        embarrassing.
        """
        p = Path(REDIRECT_DB)
        if p.is_file():
            print(f"Error: {REDIRECT_DB} already exists.")
            sys.exit(1)

        print("Creating a key-value store for the redirects.")
        create_redirects_db(redirect_db, OL_ALL_DUMP)

        # Update the ia and ol db tables.
        print("Copying tables to save time when resolving the redirects.")
        copy_db_column(db, "ia", "ia_ol_work_id", "resolved_ia_ol_work_id")
        copy_db_column(db, "ol", "ol_work_id", "resolved_ol_work_id")
        copy_db_column(db, "ol", "ol_edition_id", "resolved_ol_edition_id")
        db.commit()

        print("Resolving the redirects so there are consistent ID references.")
        update_redirected_ids(
            db, "ia", "ia_ol_work_id", "resolved_ia_ol_work_id", redirect_db
        )
        update_redirected_ids(
            db, "ol", "ol_work_id", "resolved_ol_work_id", redirect_db
        )
        db.commit()

        print("Creating the edition -> work mapping")
        # TODO: needs status bar
        create_resolved_edition_work_mapping(db, map_db)

        print("Building the edition-> work table in ia")
        # # TODO: needs status bar
        build_ia_ol_edition_to_ol_work_column(db, redirect_db, map_db)

    fire.Fire(
        {
            "fetch-data": get_and_extract_data,
            "create-db": create_db,
            "resolve-redirects": resolve_redirects,
            "all-reports": all_reports,
        }
    )
