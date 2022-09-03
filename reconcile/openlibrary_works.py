"""
Functions for working with Open Library works.
"""
import csv
import tempfile
from collections.abc import Iterator
from typing import IO

from database import Database
from lmdbm import Lmdb
from tqdm import tqdm
from utils import batcher


def copy_db_column(db: Database, table: str, from_column: str, to_column: str):
    """Copy {from_column} to {to_column} on {table} in {db}."""
    db.execute(f"UPDATE {table} SET {to_column} = {from_column}")
    db.commit()
    pass


def update_redirected_ids(
    db: Database,
    table: str,
    read_column: str,
    write_column: str,
    redirect_db: Lmdb,
):
    """
    Resolve redirected IDs and write them to a new column.

    Iterate through {read_column} of {table} on {db} and use each field value as a key
    to query {redirect_db}. If {redirect_db} returns a value, write that value to
    {write_column} on {table}. This resolves all redirects to their final value.

    This exists to create a consistent set of IDs to use when comparing backlinks,
    because without a consistent set of IDs, both IA and OL may refer to the same work
    or edition or author, but because of merges, the IDs appear inconsistent.
    """
    # Get column values to use as dictionary keys
    unchecked_ids = db.query(f"SELECT {read_column} FROM {table}")

    def get_id_update_pairs(unchecked_ids: list[tuple]) -> Iterator[tuple[str, str]]:
        """
        Iterate through {unchecked_ids} to get tuples of an ID and its final redirected
        ID.

        This is done by done by querying {redirect_db}, which contains all the
        (from -> to) pairings for redirects. If there's no entry in {redirect_db} for a
        particular key, that key is the most current, so don't add it to the results.

        For keys that do have values, use the value as a key to see if there is a
        further redirect. Repeat until the key has no value. That key is our final
        destination ID.

        Then combine all the intermediate IDs with the final destination ID and return
        them as a tuple of (original_id, final_destination_id)
        """
        # Check if each ID needs updating.
        for (original_id,) in tqdm(unchecked_ids):  # Unpack the tuple from the db query
            if original_id is None:
                continue
            current_id: str = original_id

            # Hold intermediate destination IDs to later pair with the final destination
            # ID so each link of the chain points to the final destination ID.
            intermediate_ids: list[str] = []

            # Look for redirected ids.
            while True:
                redirected_id = redirect_db.get(current_id)
                if not redirected_id:
                    # Update the final_id in case this is the final redirect ID.
                    final_id = current_id
                    break

                # Add intermediate ID to our list for later processing and check if this
                # intermediate ID is redirected again.
                intermediate_ids += (current_id,)
                current_id = redirected_id.decode()  # decode bytes from LMDB

            duos = [(final_id, intermediate_id) for intermediate_id in intermediate_ids]
            yield from duos

    collection = get_id_update_pairs(unchecked_ids)
    db.executemany(
        f"UPDATE {table} SET {write_column} = ? WHERE {read_column} IS ?", collection
    )
    db.commit()


def create_resolved_edition_work_mapping(db: Database, map_db: Lmdb) -> None:
    """
    Create an lmdbm-backed mapping of fully resolved editions to their corresponding
    fully resolved works.

    # Input: (('OL2514702M', 'OL23026723W'), ('edition4', 'work2'))

    {edition_work_pairs} should be de-duplicated first. E.g., if
    {edition_work_pairs} is is returned from an SQL query, the query should include
    "SELECT DISTINCT".
    """
    # Iterate on the cursor because db.query() does fetchall() and that may exhaust RAM.
    sql = """SELECT DISTINCT resolved_ol_edition_id, resolved_ol_work_id FROM ol WHERE
    resolved_ol_edition_id is not null and resolved_ol_work_id is not null"""
    db.execute(sql)
    edition_work_pairs = iter(tqdm(db.cursor))
    batches = batcher(edition_work_pairs, 5000)

    # Just put in the first batch. Should be able to get key from example.
    for batch in batches:
        map_db.update(batch)


def get_resolved_work_from_edition(
    redirect_db: Lmdb, map_db: Lmdb, edition_id: str
) -> str:
    """
    Get the fully resolved work corresponding to an arbitrary edition. If a work is
    not found, a KeyError is raised.

    Uses {redirect_db} to to resolve the edition ID before using {map_db} to look up
    the fully resolved edition->work mapping.
    """
    current_id = edition_id
    while True:
        redirected_id: str = redirect_db.get(current_id, None)
        if not redirected_id:
            final_id = current_id
            break
        current_id = redirected_id

    if work_id := map_db.get(final_id):
        return work_id.decode()
    raise KeyError


def get_ocaid_and_resolved_ia_work_from_edition(
    redirect_db: Lmdb,
    map_db: Lmdb,
    fp: IO[str],
) -> Iterator[tuple[str, str]]:
    """
    Returns (ocaid, resolved_ia_work_from_edition) pairs.

    Reads {fp}, a temp file, to get tab-delimited ocaid, ol_edition_id pairs, and the
    ol_edition_id is turned into a fully resolved ol_work_id. {redirect_db} has all the
    redirects, and {map_db} holds the map of editions to works.
    """
    reader = csv.reader(fp, delimiter="\t")
    fp.seek(0)  # fp is open and the stream position is on the last written line.

    for ocaid, edition in tqdm(reader):
        if not edition:
            continue

        try:
            if resolved_work := get_resolved_work_from_edition(
                redirect_db, map_db, edition
            ):
                yield (resolved_work, ocaid)
        except KeyError:
            continue


def build_ia_ol_edition_to_ol_work_column(
    db: Database, redirect_db: Lmdb, map_db: Lmdb
) -> None:
    """
    Build {db} column of fully resolved OL works from OL editions on the ia table.

    As part of the problem with verifying backlinks from OL->IA->OL, one challenge is
    that an OL edition may link to the same work on IA, and IA may link back to a
    different edition of the same work, so the editions cannot be compared directly.

    The workaround here is to fully resolve the editions and works in the ol table, and
    then put that mapping in the {map_db} edition->work store, so that when iterating
    through the ia_ol_edition_ids from the IA physical dump, those editions can be fully
    resolved and used to get corresponding and fully resolved works from the
    edition->work store.
    """

    sql = """
    SELECT ia_id,
           ia_ol_edition_id
    FROM   ia
    WHERE  ia_ol_edition_id IS NOT NULL
    """

    # Use a temp file to store the query results. Iterating on the db cursor while
    # updating was slow. Both ways avoid memory exhaustion.
    with tempfile.TemporaryFile(mode="w+") as fp:
        writer = csv.writer(fp, delimiter="\t")
        db.execute(sql)
        for row in db.cursor:
            writer.writerow(row)

        collection = get_ocaid_and_resolved_ia_work_from_edition(
            redirect_db, map_db, fp
        )
        db.executemany(
            "UPDATE ia SET resolved_ia_ol_work_from_edition = ? WHERE ia_id = ?",
            collection,
        )
        db.commit()
