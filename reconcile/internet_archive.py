"""
Functions for working with Internet Archive files.
"""
from collections.abc import Iterator

import orjson
from isbnlib import to_isbn13
from tqdm import tqdm
from utils import bufcount


def parse_ia_inlibrary_jsonl(  # noqa: C901
    filename: str,
) -> Iterator[tuple[str, str, bool, bool, str, str]]:
    """
    Parse the Internet archive YYYYMMDD_inlibrary.jsonl dump and return an
    an Iterator of the parsed lines for executemany to consume. It returns
    tuples rather than a dataclass because of how SQLite bulk imports work.

    Because we're only looking for Open Library stuff, this only returns lines that:
        - have an Open Library edition; and
        - have at most one ISBN 13, after converting all ISBNs to ISBN 13.

    """
    record_total = bufcount(filename)
    with open(filename, newline="", encoding="UTF-8") as file, tqdm(
        total=record_total
    ) as pbar:
        while True:
            pbar.update(1)
            line = file.readline()
            if not line:
                break

            d: dict[str, str] = orjson.loads(line)
            ol_edition_id = d.get("openlibrary_edition", "")
            isbns = d.get("isbn")

            if not isbns:
                continue

            # The jsonl dump "isbn" value is a list or a string. Make it consistent.
            isbn_collection = []
            if isinstance(isbns, str):
                isbn_collection.append(isbns)
            else:
                isbn_collection += isbns

            # Attempt to deduplicate down to one unique ISBN 13 for IA comparison.
            isbn_13s = {to_isbn13(isbn) for isbn in isbn_collection}
            isbn_13s.remove("") if "" in isbn_13s else isbn_13s
            sole_isbn_13 = len(isbn_13s) == 1
            # Except here where we want to track multiple ISBN 13s.
            multiple_isbn_13 = len(isbn_13s) > 1
            try:
                isbn_13 = max(isbn_13s)
            except KeyError:
                isbn_13 = ""
            except ValueError:
                isbn_13 = ""
            ocaid = d["identifier"]

            record = (
                ocaid,
                ol_edition_id,
                sole_isbn_13,
                multiple_isbn_13,
                isbn_13,
                line,
            )
            yield record
