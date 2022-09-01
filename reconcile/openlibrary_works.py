"""
Functions for working with Open Library works.
"""
from collections.abc import Iterator

from database import Database
from lmdbm import Lmdb
from tqdm import tqdm

# Perhaps much of this should be refactored into a parsers file as it's not work
# specific?


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
        Iterate through {ids} to get the final destination ID of any redirects. This is
        done by querying sqlitedict, which contains all the (from -> to) pairings for
        redirects. If there's no entry in sqlitedict for a particular key, that key is
        the most current, so don't add it to the results.

        For keys that do have values, use the value as a key to see if there is a
        further redirect. Repeat until the key has no value. That key is our final
        destination ID.

        Returns a tuple of the (original_id, final_destination_id)
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
            # print(f"intermediate_links are: {intermediate_ids}")
            # print(f"duos are: {duos}")
            # print(f"redirected_id is: {redirected_id}")
            yield from duos

    collection = get_id_update_pairs(unchecked_ids)
    # for _ in collection:
    #     print(next(collection))
    db.executemany(
        f"UPDATE {table} SET {write_column} = ? WHERE {read_column} IS ?", collection
    )
    db.commit()
