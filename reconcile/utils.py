import csv
import sys
from collections.abc import Iterable, Iterator
from datetime import datetime
from itertools import islice
from pathlib import Path
from typing import TypeVar

from isbnlib import is_isbn10, is_isbn13

# Various utility functions.

T = TypeVar("T")


def nuller(v: str | None) -> str | None:
    """
    Utility function to set '' to None so it's Null in DB. This may be
    pointless.
    """
    if v == "":
        return None
    else:
        return v


def query_output_writer(query_result: list[str], out_file: str) -> None:
    """
    Helper function to write output from queries to TSV.
    """
    with open(out_file, "w", encoding="UTF-8") as file:
        writer = csv.writer(file, delimiter="\t")
        for row in query_result:
            writer.writerow(row)


def bufcount(filename: str | Path) -> int:
    """
    Get the total number of lines in a file. Useful for TQDM progress bars.
    Per https://stackoverflow.com/a/850962
    """
    if isinstance(filename, str):
        path = Path(filename)
    else:
        path = filename

    if not path.is_file():
        print(f"Error counting lines in {path.cwd() / path.name}: file not found")
        sys.exit(1)
    with path.open(mode="r") as f:
        lines = 0
        buf_size = 1024 * 1024
        read_f = f.read  # loop optimization

        buf = read_f(buf_size)
        while buf:
            lines += buf.count("\n")
            buf = read_f(buf_size)

        return lines


def path_check(pathname: str) -> None:
    """
    Create a directory path if it doesn't exist.

    :param str path: path to check/create.
    """
    path = Path(pathname)
    if not path.is_dir():
        path.mkdir(parents=True, exist_ok=True)


def record_errors(err: list[str | None] | str, filename: str) -> None:
    """
    Record {err} to {filename}.

    :param str filename: path to outfile
    :param list err: error to record.
    """
    with Path(filename).open(mode="a") as fp:
        fp.writelines(f"{datetime.now()}: {err}\n")


def get_bad_isbn_10s(isbn_10s: Iterable[str]) -> list[str]:
    """
    Iterates thtrough canonical {isbn_10s} and returns a list of invalid ISBNs.
    """
    return [isbn for isbn in isbn_10s if not is_isbn10(isbn)]


def get_bad_isbn_13s(isbn_13s: Iterable[str]) -> list[str]:
    """
    Iterates thtrough canonical {isbn_13s} and returns a list of invalid ISBNs.
    """
    return [isbn for isbn in isbn_13s if not is_isbn13(isbn)]


def batcher(iterator: Iterator[T], batch_size: int) -> Iterator[tuple[T, ...]]:
    """
    Take a generic iterator and slice it into tuples that contain the number of items
    specified by {batch_size}. These new tuples make up the contents of a new iterator.

    For example, batcher turns this iterator into another iterator of two-batch tuples.
    open_library_ids = iter(["OL1M", "OL2M", ("OL3M", "OL4M"), "OL5M", OL6M"])
    batch = batcher(ol_ids, 2)
    next(batch)
    >>> ("OL1M", "OL2M")
    next(batch)
    >>> (("OL3M", "OL4M"), "OL5M")
    """
    while batch := tuple(islice(iterator, batch_size)):
        yield batch
