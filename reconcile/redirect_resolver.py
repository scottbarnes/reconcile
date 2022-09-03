import mmap
from collections.abc import Iterator

import orjson
from lmdbm import Lmdb
from tqdm import tqdm
from utils import batcher, bufcount

"""
Functions to resolve redirects and put them in a key/value store.
"""


def process_redirect_line(line: list[str]) -> Iterator[tuple[str, str]]:
    """
    Read a line of the full dump and pull out the redirect keys and values for use in
    making a key-value store of redirects.
    Takes:
    ['/type/redirect', '/books/OL001M', '3', '<datetimestr>, '{JSON}\n']
    ['/type/redirect', '/books/OL001M', '3', '2010-04-14T02:53:24.620268', '{"created": {"type": "/type/datetime", "value": "2008-04-01T03:28:50.625462"}, "covers": [5685889], "last_modified": {"type": "/type/datetime", "value": "2010-04-14T02:53:24.620268"}, "latest_revision": 3, "location": "/books/OL002M", "key": "/books/OL001M", "type": {"key": "/type/redirect"}, "revision": 3}\n']  # noqa E501

    Returns tuple pairs of either edition or work redirects, wwhere the first item is
    the redirector_id, and the second item is the destination_id.
    """
    if line[0] != "/type/redirect":
        return

    key = line[1].split("/")[-1]

    # Only process editions and works.
    if not key.endswith(("W", "M")):
        return

    d = orjson.loads(line[4])
    value = d.get("location", "").split("/")[-1]
    yield (key, value)


# def read_file_linearly(file: str, row_command: Callable, *args) -> Iterator[tuple]:
def read_file_linearly(file: str) -> Iterator[tuple[str, str]]:
    """
    Read {file} line by line without multiprocessing and yield the output from
    process_redirect_line().

    Returns
    """
    lines = bufcount(file)
    with open(file, "r+b") as fp, tqdm(total=lines) as pbar:
        mm = mmap.mmap(fp.fileno(), 0)
        for line in iter(mm.readline, b""):
            decoded_line = line.decode("utf-8").split("\t")
            pbar.update(1)
            yield from process_redirect_line(decoded_line)


def create_redirects_db(dict_db: Lmdb, file: str) -> None:
    """
    Read {file} and insert the redirects into {dict_db}, which is a dict-like key-value
    store.
    """
    redirects = read_file_linearly(file)
    batches = batcher(redirects, 5000)

    for batch in batches:
        dict_db.update(batch)
