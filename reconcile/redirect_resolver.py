import configparser
import mmap
import sys
from collections.abc import Generator, Iterator
from pathlib import Path

import orjson
from lmdbm import Lmdb
from utils import batcher

"""
Functions to resolve redirects and put them in a key/value store.
"""

# Load configuration
config = configparser.ConfigParser()
config.read("setup.cfg")
CONF_SECTION = "reconcile-test" if "pytest" in sys.modules else "reconcile"
FILES_DIR = config.get(CONF_SECTION, "files_dir")


def process_redirect_line(line: list[str]) -> tuple[str, str] | None:
    """
    Read a line of the full dump and pull out the redirect keys and values for use in
    making a key-value store of redirects.
    Takes:
    ['/type/redirect', '/books/OL001M', '3', '<datetimestr>, '{JSON}\n']
    ['/type/redirect', '/books/OL001M', '3', '2010-04-14T02:53:24.620268', '{"created": {"type": "/type/datetime", "value": "2008-04-01T03:28:50.625462"}, "covers": [5685889], "last_modified": {"type": "/type/datetime", "value": "2010-04-14T02:53:24.620268"}, "latest_revision": 3, "location": "/books/OL002M", "key": "/books/OL001M", "type": {"key": "/type/redirect"}, "revision": 3}\n']  # noqa E501

    Returns tuple pairs of either edition or work redirects, where the first item is
    the redirector_id, and the second item is the destination_id.
    ("OL001M", "OL002M")
    """
    key = line[1].split("/")[-1]

    # Only process editions and works.
    if not key.endswith(("W", "M")):
        return None

    d = orjson.loads(line[4])
    value = d.get("location", "").split("/")[-1]
    return (key, value)


def create_redirects_db(dict_db: Lmdb, base_filename: str) -> None:
    """
    Use {base_file} to read all processed redirect TSVs and to and insert the redirects
    into {dict_db}, which is a dict-like key-value
    store.
    By default filenames are:
        ol_dump_parsed_redirect_<uuid>.txt
    Contents are:
        OL7029749M\tOL7022571M
    Where the item an Open Library ID, and the second is its redirected ID.
    """
    path = Path(base_filename)
    files = Path(FILES_DIR).glob(f"{path.stem}_redirect_*{path.suffix}")

    def get_redirects_from_disk(files: Generator) -> Iterator[tuple[str, str]]:
        """Read from disk, process, create generator for use in batching."""
        for file in files:
            with file.open(mode="r+b") as fp:
                mm = mmap.mmap(fp.fileno(), 0)
                for line in iter(mm.readline, b""):
                    original_id, redirected_id = line.decode("utf-8").split("\t")
                    yield (original_id.strip(), redirected_id.strip())

    redirects = get_redirects_from_disk(files)
    batches = batcher(redirects, 5000)

    for batch in batches:
        dict_db.update(batch)
