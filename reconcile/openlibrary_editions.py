"""
Functions for chunking, reading, parsing, and INSERTing the Open Library editions data.
This is used by Reconcile.create_ol_table from main.py.
"""
import configparser
import csv
import mmap
import sys
import uuid
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import orjson
from database import Database
from tqdm import tqdm
from utils import bufcount, get_bad_isbn_10s, get_bad_isbn_13s, nuller, record_errors

# Load configuration
config = configparser.ConfigParser()
config.read("setup.cfg")
CONF_SECTION = "reconcile-test" if "pytest" in sys.modules else "reconcile"
FILES_DIR = config.get(CONF_SECTION, "files_dir")
OL_EDITIONS_DUMP = config.get(CONF_SECTION, "ol_editions_dump")
OL_EDITIONS_DUMP_PARSED = config.get(CONF_SECTION, "ol_editions_dump_parsed")
SQLITE_DB = config.get(CONF_SECTION, "sqlite_db")
REPORT_ERRORS = config.get(CONF_SECTION, "report_errors")
REPORT_BAD_ISBNS = config.get(CONF_SECTION, "report_bad_isbns")
SCRUB_DATA = config.getboolean(CONF_SECTION, "scrub_data")


db = Database(SQLITE_DB)


def pre_create_ol_table_file_cleanup() -> None:
    """Clean up stale files."""
    # OL_EDITIONS_DUMP_PARSED base.
    out_path = Path(OL_EDITIONS_DUMP_PARSED)
    files = Path(FILES_DIR).glob(f"{out_path.stem}*{out_path.suffix}")
    for f in files:
        f.unlink()

    # Clean up stale data scrubbing reports, if scrub_data = True.
    if SCRUB_DATA:
        p = Path(REPORT_BAD_ISBNS)
        if p.is_file():
            p.unlink()


def process_line(row: list[str]) -> tuple[str | None, str | None, str | None, int, int]:
    """
    For each decoded line in the editions dump, process it to get values for insertion
    into the database.

    Input:
    ['/type/edition', '/books/OL10000149M', '2', '2010-03-11T23:51:36.723486', '{JSON}']

    Output (all from the {JSON}):
    (ol_edition_id, ol_work_id, ol_ocaid, has_multiple_works, has_ia_source_record)

    :param list row: Items from a row of the Open Library editions dump.
    """
    # Annotate some variables to make this a bit cleaner. Maybe
    ol_edition_id: str | None = None
    ol_ocaid: str | None = None
    ol_work_id: str | None = None
    has_multiple_works: int = 0  # No boolean in SQLite
    has_ia_source_record: int = 0
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

    return (
        nuller(ol_edition_id),
        nuller(ol_work_id),
        nuller(ol_ocaid),
        has_multiple_works,
        has_ia_source_record,
    )


def make_chunk_ranges(file_name: str, size: int) -> list[tuple[int, int, str]]:
    """
    For reading large files in chunks. Create byte start/end/filepath tuples so the file
    can be read in chunks from {start} to {end} of each tuple.

    Returns:
    start, end, filepath
    [(0, 32769146, '/path/to/file'), (32769146, 65538896, '/path/to/file')]

    :param file_name str: Path of file to be chunked.
    :param int size: Size in bytes for each chunk.
    """
    chunks: list[tuple[int, int, str]] = []
    path = Path(file_name)
    cursor = 0
    file_end = path.stat().st_size

    with path.open(mode="rb") as file:
        while True:
            chunk_start = cursor
            file.seek(file.tell() + size, 1)
            file.readline()  # Move the cursor to the bytes at the end of the line.
            chunk_end = file.tell()
            cursor = chunk_end
            chunks.append((chunk_start, chunk_end, file_name))

            if chunk_end > file_end:
                break

    return chunks


def read_and_convert_chunk(
    chunk: tuple[int, int, str]
) -> Iterable[tuple[str | None, str | None, str | None, int, int]]:
    """
    For {chunk}, get the byte range to read, read it, and process the lines therein.
    Chunk:
    start, end, filepath
    (0, 32769146, '/path/to/file')

    Return generator of:
    (ol_edition_id, ol_work_id, ol_ocaid, has_multiple_works, has_ia_source_record)

    :param tuple chunk: The byte range of the file chunk, and the file to process.
    :returns Iterable: Generator of tuples as shown above.
    """
    start, end, file = chunk
    position = start

    with open(file, "r+b") as fp:
        mm = mmap.mmap(fp.fileno(), 0)
        mm.seek(start)
        for line in iter(mm.readline, b""):
            position = mm.tell()
            if position >= end:
                return

            row = line.decode("utf-8").split("\t")
            if len(row) != 5:
                record_errors(row, REPORT_ERRORS)
                continue

            parsed_row = process_line(row)
            yield parsed_row


def write_chunk_to_disk(
    chunk: tuple[int, int, str], output_base: str = OL_EDITIONS_DUMP_PARSED
) -> None:
    """
    Take a chunk and write it to TSV with a unique filename based on {output_base}.
    With Open Library Edition data written chunks look like:
    OL1001295M\tOL3338473W\tjewishchristiand0000boys\t0\t1
    edition_id\twork_id\tocaid\thas_multiple_works\thas_ia_source_record

    :param iterable converted_chunk: converted chunk to write.
    :param str output_base: base filename for output.
    """
    path = Path(output_base)

    # Inject a uuid4().hex in the file name. E.g.
    # files/ol_dump_parsed.txt ->
    # files/ol_dump_parsed_07d2ea51d64549b4876e5b621ca8c85a.txt
    new_stem = path.stem + "_" + uuid.uuid4().hex
    unique_fname = path.with_stem(new_stem)
    data = read_and_convert_chunk(chunk)
    with unique_fname.open(mode="w") as fp:
        writer = csv.writer(fp, delimiter="\t")
        for row in data:
            if len(row) != 5:
                record_errors(row, REPORT_ERRORS)
                continue
            writer.writerow(row)


def insert_ol_data_in_ol_table(
    db: Database, filename: str = OL_EDITIONS_DUMP_PARSED
) -> None:
    """
    Read the parsed Open Library edition TSVs and INSERT them into the ol table.
    There is glob matching between the filename stem and suffix.

    :param Database db: the database connection.
    :param str filename: the base dump filename.
    """
    path = Path(filename)

    # Glob the input parsed files, get line total for tqdm, and reassign the exhausted
    # {files} generator.
    files = Path(FILES_DIR).glob(f"{path.stem}*{path.suffix}")
    lines = [bufcount(f) for f in files]
    total = sum(lines)
    files = Path(FILES_DIR).glob(f"{path.stem}*{path.suffix}")

    def get_ol_rows():
        """
        From the OL data, find edition_id and ocaid pairs.
        Format is:
        edition_id, work_id, ocaid, has_multiple_works, has_ia_source_record
        e.g. OL12459902M OL9945028W  mafamillemitterr0000cahi    0   1
        """
        pbar = tqdm(total=total)
        for file in files:
            with file.open(mode="r+b") as fp:
                mm = mmap.mmap(fp.fileno(), 0)
                for line in iter(mm.readline, b""):
                    row = line.decode("utf-8").split("\t")
                    # Convert empty strings to None because in CSV None is stored as "".
                    row = [nuller(column) for column in row]
                    pbar.update(1)
                    if len(row) != 5:
                        record_errors(row, REPORT_ERRORS)
                        continue
                    yield row

    collection = get_ol_rows()
    db.execute("PRAGMA synchronous = OFF")
    db.executemany("INSERT INTO ol VALUES (?, ?, ?, ?, ?)", collection)
    db.commit()


def update_ia_editions_from_parsed_tsvs(
    db: Database, filename: str = OL_EDITIONS_DUMP_PARSED
) -> None:
    """
    Read the parsed Open Library editions TSV and use the parsed Open Library data to
    UPDATE the ia table with the ol_edition_id. For each ocaid in the ia table, this is
    to quickly compare the ia_ol_edition_id and ol_edition_id.

    There is glob matching between the filename stem and suffix.

    :param Database db: The database connection to use.
    :param str filename: filename used for OL_EDITIONS_DUMP_PARSED. See setup.cfg.
    """
    path = Path(filename)

    # Glob the input parsed files, get line total for tqdm, and reassign the exhausted
    # {files} generator.
    files = Path(FILES_DIR).glob(f"{path.stem}*{path.suffix}")
    lines = [bufcount(f) for f in files]
    total = sum(lines)
    files = Path(FILES_DIR).glob(f"{path.stem}*{path.suffix}")

    def get_ol_ia_pairs():
        """
        From the OL data, find edition_id and ocaid pairs.
        Format is:
        edition_id, work_id, ocaid, has_multiple_works, has_ia_source_record
        e.g. OL12459902M OL9945028W  mafamillemitterr0000cahi    0   1
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


######
# Some functions not currently in use
######

# def update_ia_table_with_ol_data(chunk: tuple[int, int, str]) -> None:
#     """
#     Read and parse the Open Library data in the byte range of {chunk}. Then for
#     each line, check if there is both ol_id AND ol_ocaid. If there are both,
#     UPDATE the ia table to include the ol_id associated with the ia table's
#     ocaid/ia_id.

#     Chunk:
#     start, end, file path.
#     (0, 32769146, '/path/to/file')

#     read_and_converted_chunk():
#     (ol_edition_id, ol_work_id, ol_ocaid, has_multiple_works, has_ia_source_record)

#     :param chunk tuple: Chunk to insert.
#     """
#     # The DB isn't (can't?) be passed as an argument because of issues with pickling
#     # and the map function in multiprocessing.pool.
#     db = Database(SQLITE_DB)

#     def get_ol_ids_with_ol_ocaid(data) -> Iterable[tuple[str, str]]:
#         """Find ol_id and ol_ocaid pairs to UPDATE ia table."""
#         for row in data:
#             ol_id, _, ol_ocaid, _, _ = row
#             if ol_id and ol_ocaid:
#                 yield (ol_id, ol_ocaid)

#     data = read_and_convert_chunk(chunk)
#     oid_ocaid_pairs = get_ol_ids_with_ol_ocaid(data)
#     # db.execute("PRAGMA synchronous = OFF")
#     db.executemany("UPDATE ia SET ol_edition_id = ? WHERE ia_id = ?", oid_ocaid_pairs)
#     db.close()

# def insert_ol_data_in_ol_table(chunk: tuple[int, int, str]) -> None:
#     """
#     For {chunk}, read_and_convert_chunk(), then insert ito into the database.

#     Chunk:
#     start, end, filepath
#     (0, 32769146, '/path/to/file')

#     :param chunk tuple: chunk to insert.
#     """
#     # The DB isn't (can't?) be passed as an argument because of issues with pickling
#     # and the map function in multiprocessing.pool.
#     db = Database(SQLITE_DB)

#     data = read_and_convert_chunk(chunk)
#     db.execute("PRAGMA synchronous = OFF")
#     db.executemany("INSERT INTO ol VALUES (?, ?, ?, ?, ?)", data)
#     db.close()
