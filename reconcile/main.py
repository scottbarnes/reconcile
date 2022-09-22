import configparser
import csv
import logging
import multiprocessing as mp
import sqlite3
import sys
from pathlib import Path

import fetch
import typer
from database import Database
from dump_reader import make_chunk_ranges, process_chunk
from lmdbm import Lmdb
from openlibrary_editions import (
    insert_ol_data_in_ol_table,
    pre_create_ol_table_file_cleanup,
    update_ia_editions_from_parsed_tsvs,
)
from openlibrary_works import (
    build_ia_ol_edition_to_ol_work_column,
    copy_db_column,
    create_resolved_edition_work_mapping,
    update_redirected_ids,
)
from redirect_resolver import create_redirects_db
from tqdm import tqdm
from utils import bufcount, nuller, path_check

from reports import (
    get_broken_ol_ia_backlinks_after_edition_to_work_resolution0,
    get_broken_ol_ia_backlinks_after_edition_to_work_resolution1,
    get_editions_with_multiple_works,
    get_ia_links_to_ol_but_ol_edition_has_no_ocaid,
    get_ia_with_same_ol_edition_id,
    get_ol_edition_has_ocaid_but_no_ia_source_record,
    get_ol_has_ocaid_but_ia_has_no_ol_edition,
    get_ol_has_ocaid_but_ia_has_no_ol_edition_join,
    query_ol_id_differences,
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
REDIRECT_DB = config.get(CONF_SECTION, "redirect_db")
MAPPING_DB = config.get(CONF_SECTION, "mapping_db")
REPORT_ERRORS = config.get(CONF_SECTION, "report_errors")

app = typer.Typer()
app.registered_commands += fetch.app.registered_commands


def create_ia_table(db: Database, ia_dump_path: str = IA_PHYSICAL_DIRECT_DUMP) -> None:
    """
    Create the `ia` table in {db} and populate it with data from
    [date]_inlibrary_direct.tsv from Internet Archive.
    Dump available from https://archive.org/download/ia-abc-historical-data.

    This data is INSERTed first and therefore doesn't rely on anything else being in
    there.
    """
    # TODO: Find out if it's usual to open and close the session from
    # within a function such as this, or to pass the session as an argument
    # to the function, and handle opening/committing/closing outside the
    # function.

    try:
        db.execute(
            "CREATE TABLE ia (ia_id TEXT, ia_ol_edition_id TEXT, ia_ol_work_id TEXT, \
            ol_edition_id TEXT, resolved_ia_ol_work_id TEXT, \
            resolved_ia_ol_work_from_edition TEXT)"
        )
    except sqlite3.OperationalError as err:
        print(f"SQLite error: {err}")
        print(f"You may need to delete {SQLITE_DB}.")
        sys.exit(1)

    # Populate the DB with IA physical direct dump data.
    dump_file = Path(ia_dump_path)
    if not dump_file.is_file():
        print(f"Cannot find {ia_dump_path}.")
        print("Either `fetch-data` or check `ia_physical_direct_dump` in setup.cfg")
        typer.Exit(1)

    record_total = bufcount(ia_dump_path)
    print("Inserting the Internet Archive records.")
    with open(ia_dump_path, newline="", encoding="UTF-8") as file, tqdm(
        total=record_total
    ) as pbar:
        reader = csv.reader(file, delimiter="\t")
        for row in reader:
            # TODO: Is this 'better' than try/except?
            if len(row) < 4:
                continue

            ia_id, ia_ol_edition_id, ia_ol_work_id = row[1], row[2], row[3]

            # Why is writing empty strings breaking this?
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


def create_ol_table(
    db: Database,
    filename: str = OL_ALL_DUMP,
    size: int = 1024 * 1024 * 1024,
) -> None:
    """
    Parse the (uncompressed) Open Library editions dump named {filename} and insert
    it into {db}.
    Dumps available at: https://openlibrary.org/developers/dumps

    Because uncompressed dump is so large, the script parallel processes the files
    in chunks of {size} bytes. For each chunk, it's read from the disk, parsed, and
    added to the database.
    """

    in_path = Path(OL_ALL_DUMP)
    if not in_path.is_file():
        print(f"Cannot find {OL_ALL_DUMP}.")
        print("Either `fetch-data` or check `ol_editions_dump` in setup.cfg")
        typer.Exit(1)

    # Clean up files from previous runs.
    pre_create_ol_table_file_cleanup()

    chunks = make_chunk_ranges(filename, size)
    num_parallel = mp.cpu_count() - 1

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

    print("Processing Open Library editions dump and writing to disk.")
    print("Note: this progress bar is a little lumpy because of multiprocessing.")
    total_chunks = len(chunks)
    with mp.Pool(num_parallel) as pool, tqdm(total=total_chunks) as pbar:
        result = pool.imap_unordered(process_chunk, chunks)
        for _ in result:
            pbar.update(1)

    print("Inserting the Open Library editions data.")
    insert_ol_data_in_ol_table(db)

    # Create the index after INSERT for performance gain.
    db.execute("CREATE INDEX idx_ol_edition ON ol(ol_edition_id)")
    db.execute("CREATE INDEX idx_ol_work ON ol(ol_work_id)")

    print("Now updating the Internet Archive table with Open Library data.")
    print("This shouldn't take as long.")
    update_ia_editions_from_parsed_tsvs(db)

    db.commit()


@app.command()
def create_db() -> None:
    """Create the tables and insert the data. NOTE: You must fetch the data first."""
    db = Database()
    create_ia_table(db)
    create_ol_table(db)


@app.command()
def all_reports() -> None:
    """Just run all the reports because these commands are way too long to type."""
    db = Database()

    try:
        query_ol_id_differences(db)
        print("\n")
        get_editions_with_multiple_works(db)
        print("\n")
        get_ol_has_ocaid_but_ia_has_no_ol_edition(db)
        print("\n")
        get_ol_edition_has_ocaid_but_no_ia_source_record(db)
        print("\nThe next queries use joins and are slower.\n")
        get_ol_has_ocaid_but_ia_has_no_ol_edition_join(db)
        print("\n")
        get_ia_links_to_ol_but_ol_edition_has_no_ocaid(db)
        print("\n")
        get_ia_with_same_ol_edition_id(db)
        print("\n")
        get_broken_ol_ia_backlinks_after_edition_to_work_resolution0(db)
        print("\n")
        get_broken_ol_ia_backlinks_after_edition_to_work_resolution1(db)

    except sqlite3.OperationalError as err:
        print(f"SQLite error: {err}")
        if "no such table" in err.args[0]:
            print("Perhaps you need to run with `create-db` first.")
        typer.Exit(1)


@app.command()
def resolve_redirects() -> None:
    """
    Resolve the Open Library redirect type to create {redirect_db}, a key-value database
    for looking up redirects, and {map_db}, a key-value store of arbitrary editions and
    their fully resolved works.

    Both of these databases can be accessed like Python dictionaries.They return binary
    values. E.g.:
        In [2]: map_db["OL31779107M"] = "OL24083253W"
        In [3]: map_db.get("OL31779107M")
        Out[3]: b'OL24083253W'
    """

    db = Database()
    redirect_db: Lmdb = Lmdb.open(REDIRECT_DB, "c")
    map_db: Lmdb = Lmdb.open(MAPPING_DB, "c")

    print("Creating a key-value store for the redirects.")
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


@app.callback()
def main() -> None:
    """
    The order to run these in: (1) fetch, (2) create-db, and (3) all-reports

    Note: resolve-redirects is not currently used in any reports and isn't helpful.
    """
    # Create necessary paths
    paths = [FILES_DIR, REPORTS_DIR]
    [path_check(d) for d in paths]


if __name__ == "__main__":
    logging.basicConfig(
        filename="reconcile.log",
        filemode="w",
        format="%(asctime)s: %(name)s - %(levelname)s - %(message)s",
    )
    app()
