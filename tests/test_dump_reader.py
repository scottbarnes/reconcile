import configparser
import copy
import json
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from lmdbm import Lmdb

from reconcile.datatypes import ParsedEdition, ParsedRedirect
from reconcile.dump_reader import (
    make_chunk_ranges,
    process_chunk_lines,
    read_chunk_lines,
    write_processed_chunk_lines_to_disk,
)


@pytest.fixture(scope="session")
def setup_db(tmp_path_factory) -> Iterator:
    """Set up a database to use for the session."""
    r = tmp_path_factory.mktemp("data") / "resolver.db"
    m = tmp_path_factory.mktemp("data") / "edition_to_work_mapper.db"
    with Lmdb.open(str(r), "c") as resolve_db, Lmdb.open(str(m), "c") as map_db:
        yield (resolve_db, map_db)


# Load configuration
config = configparser.ConfigParser()
config.read("setup.cfg")
CONF_SECTION = "reconcile-test" if "pytest" in sys.modules else "reconcile"
FILES_DIR = config.get(CONF_SECTION, "files_dir")
REPORTS_DIR = config.get(CONF_SECTION, "reports_dir")
IA_PHYSICAL_DIRECT_DUMP = config.get(CONF_SECTION, "ia_physical_direct_dump")
OL_ALL_DUMP = config.get(CONF_SECTION, "ol_all_dump")
OL_DUMP_PARSED_PREFIX = config.get(CONF_SECTION, "ol_dump_parse_prefix")


def test_make_chunk_ranges() -> None:
    """Make sure chunk ranges create properly."""
    assert make_chunk_ranges(
        OL_ALL_DUMP, 15_000
    ) == [  # Size must be identical everywhere.
        (0, 15_401, "./tests/seed_ol_dump_all.txt"),
        (15_401, 30_401, "./tests/seed_ol_dump_all.txt"),
    ]


def test_read_chunk_lines() -> None:
    """Just read the 2nd and 4th line."""
    # TODO can I just match the binary somehow without decoding?
    # May need to change these line numbers and next(lines) order if that works.
    chunk = (0, 10307, "./tests/seed_ol_dump_all.txt")
    second = [
        "/type/redirect",
        "/books/OL001M",
        "3",
        "2010-04-14T02:53:24.620268",
        '{"created": {"type": "/type/datetime", "value": "2008-04-01T03:28:50.625462"}, "covers": [5685889], "last_modified": {"type": "/type/datetime", "value": "2010-04-14T02:53:24.620268"}, "latest_revision": 3, "location": "/books/OL002M", "key": "/books/OL001M", "type": {"key": "/type/redirect"}, "revision": 3}\n',  # noqa #E501
    ]
    fourth = [
        "/type/edition",
        "/books/OL003M",
        "4",
        "2010-04-14T02:44:13.274395",
        '{"publishers": ["J. & A. Churchill"], "subtitle": "a treatise of decomposition", "covers": [5737156], "last_modified": {"type": "/type/datetime", "value": "2010-04-14T02:44:13.274395"}, "latest_revision": 4, "key": "/books/OL003M", "authors": [{"key": "/authors/OL2429124A"}], "ocaid": "backlink_diff_editions_same_work", "publish_places": ["London"], "pagination": "v. ;", "source_records": ["ia:backlink_diff_editions_same_work", "ia:commercialorgani04allerich", "ia:commercialorgani31allerich", "ia:commercialorgani32allerich", "ia:commercialorgani33allerich"], "created": {"type": "/type/datetime", "value": "2008-04-01T03:28:50.625462"}, "title": "Commercial organic analysis", "edition_name": "2d ed., rev. and enl.", "subjects": ["Chemistry, Analytic", "Chemistry, Organic"], "publish_date": "1884", "publish_country": "enk", "by_statement": "by Alfred H. Allen.", "works": [{"key": "/works/OL003W"}], "type": {"key": "/type/edition"}, "revision": 4}\n',  # noqa E501
    ]
    lines = read_chunk_lines(chunk)
    next(lines)
    assert next(lines) == second
    next(lines)
    assert next(lines) == fourth


def test_process_chunk_lines() -> None:
    """
    Process hypothetical lines from read_chunk(). Open Library type. If a line isn't of
    type redirect or edition, ignore it.
    """
    # Lines from read_chunk()
    bad_index_values = ["/type/redirect", "/books/OL005M"]
    redirect = [
        "/type/redirect",
        "/books/OL001M",
        "3",
        "2010-04-14T02:53:24.620268",
        '{"created": {"type": "/type/datetime", "value": "2008-04-01T03:28:50.625462"}, "covers": [5685889], "last_modified": {"type": "/type/datetime", "value": "2010-04-14T02:53:24.620268"}, "latest_revision": 3, "location": "/books/OL002M", "key": "/books/OL001M", "type": {"key": "/type/redirect"}, "revision": 3}\n',  # noqa #E501
    ]
    edition = [
        "/type/edition",
        "/books/OL003M",
        "4",
        "2010-04-14T02:44:13.274395",
        '{"publishers": ["J. & A. Churchill"], "subtitle": "a treatise of decomposition", "last_modified": {"type": "/type/datetime", "value": "2010-04-14T02:44:13.274395"}, "latest_revision": 4, "key": "/books/OL003M", "authors": [{"key": "/authors/OL2429124A"}], "ocaid": "backlink_diff_editions_same_work", "publish_places": ["London"], "pagination": "v. ;", "source_records": ["ia:backlink_diff_editions_same_work", "ia:commercialorgani04allerich", "ia:commercialorgani31allerich", "ia:commercialorgani32allerich", "ia:commercialorgani33allerich"], "created": {"type": "/type/datetime", "value": "2008-04-01T03:28:50.625462"}, "title": "Commercial organic analysis", "edition_name": "2d ed., rev. and enl.", "subjects": ["Chemistry, Analytic", "Chemistry, Organic"], "publish_date": "1884", "publish_country": "enk", "by_statement": "by Alfred H. Allen.", "works": [{"key": "/works/OL003W"}], "type": {"key": "/type/edition"}, "revision": 4}\n',  # noqa E501
    ]
    j = json.loads(edition[4])
    j["key"] = "/books/OL006M"
    edition2 = copy.copy(edition)
    edition2[4] = json.dumps(j)
    author = [
        "/type/author",
        "/authors/OL001A",
        "4",
        "2010-04-14T02:44:13.274395",
        '{"type": {"key": "/type/author"}, "name": "Brian D. Egger", "key": "/authors/OL10001673A", "source_records": ["bwb:9781440580598"], "latest_revision": 1, "revision": 1, "created": {"type": "/type/datetime", "value": "2021-12-27T01:34:50.401635"}, "last_modified": {"type": "/type/datetime", "value": "2021-12-27T01:34:50.401635"}}\n',  # noqa E501
    ]
    unprocessed_lines = [bad_index_values, redirect, edition, author, edition2]
    gen = process_chunk_lines(unprocessed_lines)

    assert next(gen) == ParsedRedirect(origin_id="OL001M", destination_id="OL002M")
    assert next(gen) == ParsedEdition(
        edition_id="OL003M",
        work_id="OL003W",
        ocaid="backlink_diff_editions_same_work",
        has_multiple_works=0,
        has_ia_source_record=1,
    )
    # Ensure author is skipped.
    assert next(gen) == ParsedEdition(
        edition_id="OL006M",
        work_id="OL003W",
        ocaid="backlink_diff_editions_same_work",
        has_multiple_works=0,
        has_ia_source_record=1,
    )


def test_write_processed_chunk_lines_to_disk() -> None:
    """Write out the chunk lines."""
    # Delete any existing written chunks.
    path = Path(OL_DUMP_PARSED_PREFIX)
    files = list(Path(FILES_DIR).glob(f"{path.stem}*{path.suffix}"))
    for file in files:
        file.unlink()

    chunk = (0, 10307, "./tests/seed_ol_dump_all.txt")
    lines = read_chunk_lines(chunk)
    processed_lines = process_chunk_lines(lines)

    write_processed_chunk_lines_to_disk(processed_lines, OL_DUMP_PARSED_PREFIX)

    # The written files have random hex strings, so use globbing to get the filenames
    # to search the chunk. Note: the search term must be contained with what would be
    # within the first chunk, as this is just writing one chunk. Something too far
    # down the unparsed file won't be in the first chunk.
    path = Path(OL_DUMP_PARSED_PREFIX)
    files = list(Path(FILES_DIR).glob(f"{path.stem}*{path.suffix}"))

    edition = "OL1002158M\tOL1883432W\torganizinggenius0000benn\t9780201570519\t1\t1"
    redirect = "OL001M\tOL002M"
    assert any(edition in file.read_text() for file in files) is True
    assert any(redirect in file.read_text() for file in files) is True


# TODO: This needs to do the test on each line it reads and
# then call the correct parser.
# The processers could use some suffix on the OL parsed prefix. e.g. editions,
# redirects, etc.
# def test_write_chunk_to_disk() -> None:
#     """Write a chunk to disk."""
#     # Delete any existing written chunks.
#     path = Path(OL_DUMP_PARSED_PREFIX)
#     files = Path(FILES_DIR).glob(f"{path.stem}*{path.suffix}")
#     for file in files:
#         file.unlink()

#     chunk = (0, 10884, "./tests/seed_ol_dump_editions.txt")
#     yielded_chunk = read_and_convert_chunk(chunk)
#     write_chunk_to_disk(yielded_chunk, OL_DUMP_PARSED_PREFIX)

#     # The written files have random hex strings, so use globbing to get the filenames
#     # to search the chunk. Note: the search term must be contained with what would be
#     # within the first chunk, as this is just writing one chunk. Something too far
#     # down the unparsed file won't be in the first chunk.
#     path = Path(OL_DUMP_PARSED_PREFIX)
#     files = Path(FILES_DIR).glob(f"{path.stem}*{path.suffix}")

#     edition = "OL1002158M\tOL1883432W\torganizinggenius0000benn\t1\t1"
#     assert any(edition in file.read_text() for file in files) is True
