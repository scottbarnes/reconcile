"""
Functions for chunking, reading, parsing, and INSERTing the Open Library editions data.
This is used by create_ol_table() from main.py.
"""
import configparser
import mmap
import sqlite3
import sys
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

import orjson
from database import Database
from isbnlib import to_isbn13
from tqdm import tqdm

from reconcile.datatypes import ParsedEdition
from reconcile.utils import (
    bufcount,
    get_bad_isbn_10s,
    get_bad_isbn_13s,
    nuller,
    record_errors,
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
SCRUB_DATA = config.getboolean(CONF_SECTION, "scrub_data")


db = Database(SQLITE_DB)


def pre_create_ol_table_file_cleanup() -> None:
    """Clean up stale files."""
    # OL_EDITIONS_DUMP_PARSED base.
    out_path = Path(OL_DUMP_PARSED_PREFIX)
    files = Path(FILES_DIR).glob(f"{out_path.stem}*{out_path.suffix}")
    for f in files:
        f.unlink()

    # Clean up stale data scrubbing reports, if scrub_data = True.
    if SCRUB_DATA:
        p = Path(REPORT_BAD_ISBNS)
        if p.is_file():
            p.unlink()


def process_edition_line(row: list[str]) -> ParsedEdition:  # noqa: C901
    """
    For each decoded line in the editions dump, process it to get values for insertion
    into the database.

    Input:
    ['/type/edition', '/books/OL10000149M', '2', '2010-03-11T23:51:36.723486', '{JSON}']
    """
    # Annotate some variables to make this a bit cleaner. Maybe
    ol_edition_id: str
    ol_ocaid: str | None = None
    ol_work_id: str | None = None
    has_multiple_works: int = 0  # No boolean in SQLite
    has_ia_source_record: int = 0
    has_cover: int = 0
    d: dict[str, Any] = orjson.loads(row[4])

    ol_ocaid = d.get("ocaid", None)
    ol_edition_id = d.get("key", "").split("/")[-1]
    isbn_10s: list[str] = d.get("isbn_10", None)
    isbn_13s: list[str] = d.get("isbn_13", None)

    if work_id := d.get("works"):
        ol_work_id = work_id[0].get("key", "").split("/")[-1]
        has_multiple_works = int(len(work_id) > 1)

    if (source_records := d.get("source_records")) and isinstance(source_records, list):
        # Check if each record has an "ia:" in it. If any does, return True
        # and convert to 1 for SQLite, and 0 otherwise.
        has_ia_source_record = int(
            any(["ia" in record for record in source_records if record is not None])
        )

    # Check and report bad ISBNs.
    # Set `scrub_data = True` in setup.cfg to enable this. This adds 3-5 minutes to the
    # processing.
    if SCRUB_DATA:
        # Get any bad ISBNs and report them.
        bad_isbn10s = get_bad_isbn_10s(isbn_10s) if isbn_10s else isbn_10s
        bad_isbn13s = get_bad_isbn_13s(isbn_13s) if isbn_13s else isbn_13s
        if bad_isbn10s or bad_isbn13s:
            record_errors(
                f"Invalid ISBNs for {ol_edition_id}: {bad_isbn10s} {bad_isbn13s}",
                REPORT_BAD_ISBNS,
            )

    # Used to help supplement BWBCoverBot's database of editions with covers.
    covers = d.get("covers")
    if covers and len(covers) > 0:
        has_cover = 1

    # Attempt to get one ISBN for comparison with Internet Archive items.
    isbns = set(isbn_13s or {})
    isbns.update({to_isbn13(isbn) for isbn in isbn_10s or {}})
    # With every ISBN as ISBN 13 and deduplicated, save them all for BWBCoverBot.
    isbn_13s_for_covers = ",".join(isbns)
    try:
        isbn_13 = isbns.pop()
    except KeyError:
        isbn_13 = ""

    return ParsedEdition(
        edition_id=ol_edition_id,
        work_id=ol_work_id,
        ocaid=ol_ocaid,
        isbn_13=isbn_13,
        has_multiple_works=has_multiple_works,
        has_ia_source_record=has_ia_source_record,
        has_cover=has_cover,
        isbn_13s=isbn_13s_for_covers,
    )


def insert_ol_data_in_ol_table(
    db: Database, filename: str = OL_DUMP_PARSED_PREFIX
) -> None:
    """
    Read the parsed Open Library edition TSVs and INSERT the contents into the ol table
    of {db}. {filename} is the base filename without the hex string that
    write_chunk_to_disk() adds.
    """
    path = Path(filename)

    files = list(Path(FILES_DIR).glob(f"{path.stem}_edition_*{path.suffix}"))
    lines = [bufcount(f) for f in files]
    total = sum(lines)

    def get_ol_rows() -> Iterator[Sequence[str | None]]:
        """
        Read {filename} in TSV format, decode the lines, and yield them.
        Format of decoded line, formatted by ParsedEdition.to_list():
        edition_id, work_id, ocaid, isbn_13, has_multiple_works, has_ia_source_record, has_cover isbn_13s
        e.g. OL12459902M OL9945028W  mafamillemitterr0000cahi  1234567890123  0  1  1 ""

        Returns the same data as a list, but everything is a string (or None).
        ["OL12459902M", "OL9945028W", "mafamillemitterr0000cahi", "1234567890123", "0", "1", "1", ""]
        """
        pbar = tqdm(total=total)
        for file in files:
            with file.open(mode="r+b") as fp:
                mm = mmap.mmap(fp.fileno(), 0)
                for line in iter(mm.readline, b""):
                    row = line.decode("utf-8").split("\t")
                    # Because another function reads isbn_13s, we can pop the index of it as
                    # it is not needed here and doesn't go into the database.
                    row.pop()
                    # Convert empty strings to None because in CSV None is stored as "".
                    nulled_row = [nuller(column) for column in row]
                    pbar.update(1)
                    if len(nulled_row) != 7:
                        record_errors(nulled_row, REPORT_ERRORS)
                        continue
                    nulled_row += (
                        "",
                        "",
                    )  # Faster append of to-be-used columns.
                    yield nulled_row

    collection = get_ol_rows()
    db.execute("PRAGMA synchronous = OFF")
    db.executemany("INSERT INTO ol VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", collection)
    db.commit()


def insert_ol_cover_data_into_cover_db(  # noqa: C901
    db: Database, filename: str = OL_DUMP_PARSED_PREFIX
) -> None:
    """
    Read the data files from the already parsed Open Library dump to pull out ISBNs with covers
    so that they can be written to ./bwb-cover-bot.sqlite for use by BWBCoverBot.
    """
    print("Note: OL data must be inserted first as this reads files that creates.")

    try:
        db.execute(
            "CREATE TABLE EditionCoverData (isbn_13 TEXT PRIMARY KEY, cover_exists INTEGER)"
        )
    except sqlite3.OperationalError as err:
        print(f"SQLite error: {err}")
        print("You may need to delete ./bwb-cover-bot.sqlite.")
        sys.exit(1)

    path = Path(filename)

    files = list(Path(FILES_DIR).glob(f"{path.stem}_edition_*{path.suffix}"))
    lines = [bufcount(f) for f in files]
    total = sum(lines)

    def get_ol_rows() -> Iterator[tuple[str, int]]:
        """
        For each ParsedEdition written to the CSV files this reads, check if has_isbn = 1, and
        if it is, process the string of ISBNs that corresponds to ParsedEdition.isbn_13s,
        returning a tuple of (isbn13, 1), corresponding to isbn_13, and cover_exists in the DB.
        """
        pbar = tqdm(total=total)
        for file in files:
            with file.open(mode="r+b") as fp:
                mm = mmap.mmap(fp.fileno(), 0)
                for line in iter(mm.readline, b""):
                    pbar.update(1)
                    row = line.decode("utf-8").split("\t")
                    # Check for has_cover
                    if row[6] != "1":
                        continue

                    try:
                        isbns = row[7].split(",")  # Unpack possible multiple ISBN 13s.

                        for isbn in isbns:
                            isbn = isbn.strip()
                            if isbn:
                                yield (isbn, 1)
                    except IndexError:
                        continue

    collection = get_ol_rows()
    db.execute("PRAGMA synchronous=0")
    db.execute("PRAGMA journal_mode=wal")
    db.executemany("INSERT OR IGNORE INTO EditionCoverData VALUES (?, ?)", collection)
    db.commit()


def update_ia_editions_from_parsed_tsvs(
    db: Database, filename: str = OL_DUMP_PARSED_PREFIX
) -> None:
    """
    Read the parsed Open Library editions TSV from {filename} and use the parsed data to
    UPDATE the ia table with the ol_edition_id, based on an ocaid being present on both
    sides. This is to quickly compare the ia_ol_edition_id and ol_edition_id.

    There is glob matching between the filename stem and suffix.
    """
    path = Path(filename)

    files = list(Path(FILES_DIR).glob(f"{path.stem}_edition_*{path.suffix}"))
    lines = [bufcount(f) for f in files]
    total = sum(lines)

    def get_ol_ia_pairs() -> Iterator[tuple[str, str]]:
        """
        From the parsed OL data, find edition_id and ocaid pairs.
        Format is:
        edition_id, work_id, ocaid, isbn_13, has_multiple_works, has_ia_source_record
        e.g. OL12459902M OL9945028W  mafamillemitterr0000cahi  012345678X  0  1
        """
        pbar = tqdm(total=total)
        for file in files:
            with file.open(mode="r+b") as fp:
                mm = mmap.mmap(fp.fileno(), 0)
                for line in iter(mm.readline, b""):
                    row = line.decode("utf-8").split("\t")
                    pbar.update(1)
                    if row[0] and row[2]:
                        yield (row[0], row[2])

    collection = get_ol_ia_pairs()
    db.executemany("UPDATE ia SET ol_edition_id = ? WHERE ia_id = ?", collection)
    db.commit()
