import csv  # TODO: Decide between CSV and JSON for everything?
import os
import sqlite3
import sys
from typing import Any, Iterator, Optional, Tuple, Union

import fire
import orjson
from database import Database
from fetch import get_and_extract_data

# from collections.abc import



# Read the file paths from environment variables first, otherwise try to use the
# filenames from the download scripts.
files_dir = os.getcwd() + "/files"
IA_PHYSICAL_DIRECT_DUMP = os.environ.get(
    "IA_PHYSICAL_DIRECT_DUMP", f"{files_dir}/ia_physical_direct_latest.tsv"
)
OL_EDITIONS_DUMP = os.environ.get(
    "OL_EDITIONS_DUMP", f"{files_dir}/ol_dump_editions_latest.txt"
)
OL_EDITIONS_DUMP_PARSED = os.environ.get(
    "OL_EDITIONS_DUMP_PARSED", f"{files_dir}/ol_dump_editions_latest_parsed.tsv"
)
SQLITE_DB = os.environ.get("SQLITE_DB", f"{files_dir}/ol_test.db")
# TODO: This may get rewritten to be 'REPORTS' or something.
OL_IA_ID_DIFF = os.environ.get("OL_IA_ID_DIFF", f"{files_dir}/ol_ia_id_diff.tsv")

# Custom type
IA_JSON = dict[str, Union[str, list[str], list[dict[str, str]], dict[str, str]]]


class Reconciler:
    """Class object for working witht the various IA <-> OL data
    reconciliation functions."""

    # def get_db(self, file: str) -> Tuple[sqlite3.Connection, sqlite3.Cursor]:
    #     """Get a connection and cursor for our little database."""
    #     connection = sqlite3.connect(file)
    #     return (connection, connection.cursor())

    def create_db(
        self, db: Database, ia_dump: str = IA_PHYSICAL_DIRECT_DUMP
    ):  # , db_name: str = SQLITE_DB):
        """
        Populate the DB with IA data from the [date]_inlibrary_direct.tsv
        dump, available https://archive.org/download/ia-abc-historical-data.
        This data is all entered first and therefore doesn't rely on anything
        else being in there.
        """
        # TODO: Find out if it's usual to open and close the session from
        # within a function such as this, or to pass the session as an argument
        # to the function, and handle opening/commiting/closing outside the
        # function.

        # Create the reconcile table.
        try:
            db.execute(
                "CREATE TABLE reconcile (ia_id TEXT,  ia_ol_edition_id TEXT, \
                           ia_ol_work_id TEXT, ol_edition_id TEXT, ol_aid TEXT)"
            )
            # Indexing ia_id massively speeds up adding OL records.
            db.execute("CREATE INDEX idx ON reconcile(ia_id)")
        except sqlite3.OperationalError as err:
            print(f"SQLite error: {err}")
            sys.exit(1)

        # Populate the DB with IA physical direct dump data.
        with open(ia_dump, newline="") as file:
            reader = csv.reader(file, delimiter="\t")
            for row in reader:
                # Get the IDs, though some are empty strings.
                # The row is in the format: id   ia_id   ia_ol_edition   ia_ol_work
                ia_id, ia_ol_edition_id, ia_ol_work_id = row[1], row[2], row[3]
                # Has an OL edition but no OL work
                if ia_id and ia_ol_edition_id and not ia_ol_work_id:
                    db.execute(
                        "INSERT INTO reconcile VALUES (?, ?, ?, ?, ?)",
                        (ia_id, ia_ol_edition_id, None, None, None),
                    )
                # No OL edition but has an OL work.
                elif ia_id and not ia_ol_edition_id and ia_ol_work_id:
                    db.execute(
                        "INSERT INTO reconcile VALUES (?, ?, ?, ?, ?)",
                        (ia_id, None, ia_ol_work_id, None, None),
                    )
                # Has both an OL edition and work.
                elif ia_id and ia_ol_edition_id and ia_ol_work_id:
                    db.execute(
                        "INSERT INTO reconcile VALUES (?, ?, ?, ?, ?)",
                        (ia_id, ia_ol_edition_id, ia_ol_work_id, None, None),
                    )

        db.commit()
        return

    def parse_ol_dump_and_write_ids(
        self, in_file: str = OL_EDITIONS_DUMP, out_file: str = OL_EDITIONS_DUMP_PARSED
    ) -> None:
        """
        Parse an Open Library editions dump from
        https://openlibrary.org/developers/dumps and write the output to a .tsv in
        the format:
        ol_id\tocaid
        """
        # Fix for: _csv.Error: field larger than field limit (131072)
        csv.field_size_limit(sys.maxsize)
        # For each row in the Open Library editions dump, get the edition ID
        # and the associated JSON, and from the JSON extract the ocaid record
        # and the work ID.
        with open(out_file, "w") as outfile, open(in_file) as infile:
            writer = csv.writer(outfile, delimiter="\t")
            reader = csv.reader(infile, delimiter="\t")

            has_multiple_works = []
            for row in reader:
                # Skip if no JSON.
                if len(row) < 5:
                    continue
                # Parse the json to get what we need.
                # ol_edition_id = row[1].split("/")[-1]
                # ol_ocaid = orjson.loads(row[4]).get("ocaid")
                d = orjson.loads(row[4])
                ol_edition_id = d.get("key").split("/")[-1]
                ol_work_id = []
                if (result := d.get("works")):
                    ol_work_id = result[0].get("key").split("/")[-1]

                ol_ocaid = d.get("ocaid")
                if d.get("works") and len(d.get("works")) > 1:
                    print(f"{ol_edition_id} has mork than one work")
                    has_multiple_works.append(ol_edition_id)

                if ol_edition_id and ol_ocaid:
                    writer.writerow([ol_edition_id, ol_work_id, ol_ocaid])

            print(f"There are {len(has_multiple_works)} editions with multiple works.")
            print(f"They are: {has_multiple_works}")

    def insert_ol_data_from_tsv(
        self, db: Database, parsed_ol_data: str = OL_EDITIONS_DUMP_PARSED
    ) -> None:
        """
        Add the OL data from parse_ol_dump_and_write_ids() by collating it with
        the already inserted IA data. Specifically, for each row in the parsed
        OL .tsv, find that row's ia_id within the database, and then within the
        database update that database row to include the ol_id as noted within
        the OL tsv that we're reading.
        """
        # TODO: Try a IA table and an OL table and see how fast this is doing
        # queries with an inner join.

        def collection() -> Iterator[tuple[str, str]]:
            with open(parsed_ol_data) as file:
                reader = csv.reader(file, delimiter="\t")
                for row in reader:
                    ol_id, ol_ocaid = row[0], row[1]

                    if ol_id and ol_ocaid:
                        yield (ol_id, ol_ocaid)

        db.executemany(
            "UPDATE reconcile SET ol_edition_id = ? WHERE ia_id = ?", collection()
        )
        db.commit()

    def query_ol_id_differences(self, db: Database) -> None:
        """
        Query the database to find archive.org item links to Open Library editions
        that do not themselves link back to the original archive.org item.
        """
        # db.execute(
        #     "SELECT * FROM reconcile where ia_ol_edition_id is not ol_edition_id"
        # )

        # result = db.fetchall()
        result = db.get_ol_ia_id_differences()

        # Get the total of ostenisible broken backlinks and write everything to a
        # TSV.
        broken_backlinks = 0
        with open(OL_IA_ID_DIFF, "w") as file:
            writer = csv.writer(file, delimiter="\t")
            for row in result:
                writer.writerow(row)
                broken_backlinks += 1

        print(f"Total of broken back-links: {broken_backlinks}")
        print(f"Output written to {OL_IA_ID_DIFF}")

        return

    # Not currently in use.
    # def insert_ol_data_from_json(self) -> None:
    #     """
    #     Populate the DB with OL data from a json file.
    #     """
    #     connection, cursor = self.get_db(SQLITE_DB)

    #     # TODO: This loads all the JSON into memory as a dictionary. Iterator?
    #     with open(OL_EDITIONS_DUMP_PARSED) as file:
    #         # Create dictionary of the form:
    #         # {'ol_id0': ['ia_id0, ..., ia_idx'], 'ol_id1: ['ia_id0, ... ia_idx']'}
    #         ol_and_ia_ids: dict[str, list[str]] = json.load(file)

    #         # For each OL item, iterate through the list of IA IDs, update any matching
    #         # IA IDs with OL information.
    #         collection = []
    #         for ol_edition_id in ol_and_ia_ids:
    #             ia_ids = ol_and_ia_ids.get(ol_edition_id)

    #             if not ia_ids:
    #                 continue

    #             """
    #             What I can try here is to put my list of IDs to update in some sort
    #             of iterator, and then  use executemany() on it.
    #             """
    #             for ia_id in ia_ids:
    #                 collection.append((ol_edition_id, ia_id))

    #         cursor.execute("BEGIN TRANSACTION;")
    #         cursor.executemany(
    #             "UPDATE reconcile SET ol_edition_id = ? WHERE ia_id = ?", collection
    #         )
    #         cursor.execute("COMMIT;")

    #     connection.commit()
    #     connection.close()

    # while (line := file.readline().rstrip()):
    #     # ia_record = ""
    #     value = []

    #     # Extract the OL record JSON.
    #     if not (pattern := re.compile(r"\s([{\[].*?[}\]])$")):
    #         continue  # No match
    #     if not (data := pattern.search(line)):
    #         continue  # No match

    #     # Load the JSON for parsing.
    #     parsed_json: IA_JSON = json.loads(data.group(1))
    #     if not parsed_json:
    #         continue

    #     # Get the key (OL ID) and value (IA ID) found in dict["key"] and
    #     # dict["source_records"] respectively.
    #     key = parsed_json["key"]
    #     if isinstance(key, str):
    #         key = key.split("/")[-1]

    #     source_records = parsed_json.get("source_records")
    #     if source_records:
    #         unparsed_ia_records = [r for r in source_records if r and "ia:" in r ]
    #         if unparsed_ia_records:
    #             value = [r.split(":")[-1] for r in unparsed_ia_records if isinstance(r, str)]

    #     # Create the dictionary of OL ID keys and IA ID values.
    #     if key and value:
    #         ia_ol_ids[key] = value

    # return ia_ol_ids

    # def write_ol_and_ia_ids(dictionary: dict[str, list[str]]) -> None:
    #     """
    #     Write the OL and IA IDs to a CSV.
    #     """
    #     with open(OL_EDITIONS_DUMP_PARSED, "w") as file:
    #         file.write(json.dumps(dictionary))
    #         # id_writer = csv.writer(file, delimiter=",", quotechar="|")
    #         # for key, value in dictionary.items():
    #         #     id_writer.writerow([key, value])

    # def parse_dump() -> None:
    #     """
    #     Parse the OL dump from the command line.
    #     """
    #     d = get_ol_and_ia_ids()
    #     if d:
    #         write_ol_and_ia_ids(d)


if __name__ == "__main__":
    reconciler = Reconciler()
    # connection, session = reconciler.get_db(SQLITE_DB)
    db = Database(SQLITE_DB)
    # Some functions to work around passing arguments to Fire.
    # TODO: Do this the right way, because this is so ugly.
    def cdb():
        reconciler.create_db(db)

    def iodft():
        reconciler.insert_ol_data_from_tsv(db)

    def qod():
        reconciler.query_ol_id_differences(db)

    fire.Fire(
        {
            # "createdb": reconciler.create_db(db),
            "createdb": cdb,
            "parse-ol-data": reconciler.parse_ol_dump_and_write_ids,
            # "insert-ol-data-json": reconciler.insert_ol_data_from_json,
            "insert-ol-data-tsv": iodft,
            "query-ol-differences": qod,
            "fetch-data": get_and_extract_data,
        }
    )
