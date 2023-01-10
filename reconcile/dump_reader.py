import configparser
import csv
import logging
import mmap
import sys
import uuid
from collections.abc import Iterable, Iterator
from pathlib import Path

from database import Database

from reconcile.datatypes import ParsedEdition, ParsedRedirect
from reconcile.openlibrary_editions import process_edition_line
from reconcile.redirect_resolver import process_redirect_line

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


db = Database()
logger = logging.getLogger(__name__)


def make_chunk_ranges(file_name: str, size: int) -> list[tuple[int, int, str]]:
    """
    Reads {file_name} in chunks of {size} bytes. Creates byte start/end/filepath
    tuples so {file_name} can be read in chunks from index[0] to index[1] of each tuple.

    Returns:
    start, end, filepath
    [(0, 32769146, '/path/to/file'), (32769146, 65538896, '/path/to/file')]
    """
    chunks: list[tuple[int, int, str]] = []
    path = Path(file_name)
    cursor = 0
    file_end = path.stat().st_size

    with path.open(mode="rb") as file:
        while True:
            chunk_start = cursor
            file.seek(file.tell() + size, 0)
            file.readline()  # Move the cursor to the bytes at the end of the line.
            chunk_end = file.tell()
            cursor = chunk_end
            chunks.append((chunk_start, chunk_end, file_name))

            if chunk_end > file_end:
                break

    return chunks


def read_chunk_lines(chunk: tuple[int, int, str]) -> Iterator[list[str]]:
    """
    Read a chunk and return its decoded line. Chunks are of the form:
    [(start_byte, end_byte, 'patht_to_file'), (...)]. E.g.:
    [(0, 32769146, '/path/to/file'), (32769146, 65538896, '/path/to/file')]
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

            yield line.decode("utf-8").split("\t")


def process_chunk_lines(
    lines: Iterable[list[str]],
) -> Iterator[ParsedRedirect | ParsedEdition]:
    """
    Process {lines} as returned by read_chunk_lines(). Each line looks like:
    ['/type/edition', '/books/OL5756837M', '9', 'datetimestmap', '{JSON}']
    ['/type/redirect', '/authors/OL10219261A', '2', 'datetimestmap', '{"location": "/authors/OL3894951A"}']  # noqa E501

    Lines are then processed by their respective parsers, and a tuple is created to pass
    to the disk writer. E.g., for the above edition this will return:
    ('edition', ('OL5756837M', 'OL6600544W', 'guidetojohnmuirt0000star', 0, 0))
    """
    for line in lines:
        match line[0]:
            case "/type/redirect":
                try:
                    redirect = process_redirect_line(line)
                    if redirect:
                        yield redirect
                except IndexError:
                    print(f"IndexError on: {line}")
                    continue
            case "/type/edition":
                try:
                    edition = process_edition_line(line)
                    if edition:
                        yield edition
                except IndexError:
                    print(f"IndexError on: {line}")
                    continue
            case _:
                logger.debug(f"{line} fell through process_chunk_lines()")
                continue


def write_processed_chunk_lines_to_disk(
    lines: Iterable[ParsedEdition | ParsedRedirect], output_base: str
) -> None:
    """
    Iterate through {lines} from process_chunk_lines() and write the lines to the
    relevant file based on the Open Library type found at index 0 of the tuple.

    E.g. for an edition, input looks like:
    ('edition', ('OL5756837M', 'OL6600544W', 'guidetojohnmuirt0000star', 0, 0))
    """
    path = Path(output_base)

    edition_stem = path.stem + "_" + "edition" + "_" + uuid.uuid4().hex
    redirect_stem = path.stem + "_" + "redirect" + "_" + uuid.uuid4().hex
    unique_edition_fname = path.with_stem(edition_stem)
    unique_redirect_fname = path.with_stem(redirect_stem)

    with unique_edition_fname.open(mode="w") as edition_fp, unique_redirect_fname.open(
        mode="w"
    ) as redirect_fp:
        edition_writer = csv.writer(edition_fp, delimiter="\t")
        redirect_writer = csv.writer(redirect_fp, delimiter="\t")

        for line in lines:
            match line:
                case ParsedEdition():
                    edition_writer.writerow(line.to_list())
                case ParsedRedirect():
                    redirect_writer.writerow(line.to_list())
                case _:
                    logger.warning(
                        f"{line} fell through write_processed_chunk_lines_to_disk()"
                    )
                    continue


def process_chunk(
    chunk: tuple[int, int, str], output_base: str = OL_DUMP_PARSED_PREFIX
) -> None:
    """
    Take a tuple of chunks from make_chunk_ranges() and read the chunks from disk,
    process them, and write them back to disk with only the relevant information.
    This is used by the multiprocessing feature to combine the steps.
    """
    lines = read_chunk_lines(chunk)
    processed_lines = process_chunk_lines(lines)
    write_processed_chunk_lines_to_disk(processed_lines, output_base)
