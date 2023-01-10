import configparser
import sqlite3
import sys
from collections.abc import Iterable
from typing import Any

from utils import path_check

# Load configuration
config = configparser.ConfigParser()
config.read("setup.cfg")
CONF_SECTION = "reconcile-test" if "pytest" in sys.modules else "reconcile"
FILES_DIR = config.get(CONF_SECTION, "files_dir")
REPORTS_DIR = config.get(CONF_SECTION, "reports_dir")
SQLITE_DB = config.get(CONF_SECTION, "sqlite_db")


class Database:
    """
    A class for more easily interacting with the database.
    Adapted from https://stackoverflow.com/a/38078544.
    """

    def __init__(self, name: str = SQLITE_DB):
        # Create any necessary paths. This deserves a better fix.
        paths = [FILES_DIR, REPORTS_DIR]
        [path_check(d) for d in paths]

        self._conn = sqlite3.connect(name, timeout=60)
        self._cursor = self._conn.cursor()

    def __enter__(self):  # type: ignore[no-untyped-def]
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # type: ignore[no-untyped-def]
        self.close()

    @property
    def connection(self) -> sqlite3.Connection:
        return self._conn

    @property
    def cursor(self) -> sqlite3.Cursor:
        return self._cursor

    def commit(self) -> None:
        self.connection.commit()

    def close(self, commit: bool = True) -> None:
        if commit:
            self.commit()
        self.connection.close()

    def execute(self, sql: str, params: tuple[str] | None = None) -> None:
        self.cursor.execute(sql, params or ())

    def executemany(
        self, sql: str, params: tuple[str] | Iterable[str] | None = None
    ) -> None:
        self.cursor.executemany(sql, params or ())

    def fetchall(self) -> list[Any]:
        return self.cursor.fetchall()

    def fetchone(self) -> Any:
        return self.cursor.fetchone()

    def query(self, sql: str, params: tuple[str] | None = None) -> list[Any]:
        self.cursor.execute(sql, params or ())
        return self.fetchall()

    def get_ol_ia_id_differences(self) -> list[Any]:
        """
        Get inconsistent Open Library IDs (OLIDs) based on the associated Internet
        Archive OCAID, according to the OLID that Open Library itself associates
        with an OCAID, and with the OLID that Internet Archive associates with an
        ocaid.
        """
        sql = """
        SELECT *
        FROM   ia
        WHERE  ia_ol_edition_id IS NOT ol_edition_id
        AND    ol_edition_id IS NOT NULL
        AND    ia_ol_edition_id IS NOT NULL
        """
        return self.query(sql)

    def get_editions_with_multiple_works(self) -> list[Any]:
        """
        Get records where an Open Library Edition has more than one associated Work.
        """
        sql = """
        SELECT ol_edition_id
        FROM   ol
        WHERE  has_multiple_works IS 1
        """
        return self.query(sql)

    def get_ocaid_where_ol_edition_has_ocaid_and_ia_has_no_ol_edition(
        self,
    ) -> list[Any]:
        """
        Get records where an Open Library edition has an OCAID but Internet
        Archive has no Open Library edition associated with that OCAID.
        """
        sql = """
        SELECT ia_id,
               ol_edition_id
        FROM   ia
        WHERE  ol_edition_id IS NOT NULL
               AND ia_ol_edition_id IS NULL
        """
        return self.query(sql)

    def get_ocaid_where_ol_edition_has_ocaid_and_ia_has_no_ol_edition_join(
        self,
    ) -> list[Any]:
        """
        Same as get_records_where_ol_has_ocaid_but_ia_has_no_ol_edition, but this time
        using a database inner join.
        """
        sql = """
        SELECT ia.ia_id,
               ol.ol_edition_id
        FROM   ia
               INNER JOIN ol
                       ON ia.ia_id = ol.ol_ocaid
        WHERE  ia.ia_ol_edition_id IS NULL
        """
        return self.query(sql)

    def get_ia_links_to_ol_but_ol_edition_has_no_ocaid(self) -> list[Any]:
        """
        Get records where Internet Archive links to an Open Library Edition, but that
        Open Library Edition has no OCAID.
        """
        sql = """
        SELECT ia.ia_id,
               ia.ia_ol_edition_id
        FROM   ia
               INNER JOIN ol
                       ON ia.ia_ol_edition_id = ol.ol_edition_id
        WHERE  ol.ol_ocaid IS NULL
        """
        return self.query(sql)

    def get_ia_links_to_ol_but_ol_edition_has_no_ocaid_jsonl(self) -> list[Any]:
        """
        Get records where Internet Archive links to an Open Library Edition, but that
        Open Library Edition has no OCAID.
        """
        # sql = """
        # SELECT ia_jsonl.info ->> 'openlibrary_edition',
        #        ia_jsonl.info ->> 'identifier'
        # FROM   ia_jsonl
        #        INNER JOIN ol
        #                ON ol.ol_edition_id = ia_jsonl.info ->> 'openlibrary_edition'
        # WHERE  ol.ol_ocaid IS NULL
        # """
        sql = """
        SELECT ol.ol_edition_id,
               ia_jsonl.ocaid
        FROM   ia_jsonl
               INNER JOIN ol
                    ON ia_jsonl.ol_edition_id = ol.ol_edition_id
        WHERE  ol.ol_ocaid IS NULL and ia_jsonl.sole_isbn_13 is 1
        """
        # WHERE  ol.ol_ocaid IS NULL and ia_jsonl.sole_isbn_13 is 1 and ol.isbn_13 = ia_jsonl.isbn_13
        return self.query(sql)

    def get_ia_links_to_ol_but_ol_edition_has_no_ocaid_jsonl_multiple(
        self,
    ) -> list[Any]:
        """
        Get records where Internet Archive links to an Open Library Edition, but that
        Open Library Edition has no OCAID. And that have multiple ISBN 13s.
        """
        sql = """
        SELECT ol.ol_edition_id,
               ia_jsonl.ocaid
        FROM   ia_jsonl
               INNER JOIN ol
                    ON ia_jsonl.ol_edition_id = ol.ol_edition_id
        WHERE  ol.ol_ocaid IS NULL and ia_jsonl.multiple_isbn_13 is 1
        """
        return self.query(sql)

    def get_ia_item_has_one_isbn_13_and_no_link_to_ol(self) -> list[Any]:
        """
        Get records where Internet Archive has one ISBN 13 and there is no link to Open Library.
        Does this need to limit the collection?
        """
        sql = """
        SELECT ia_jsonl.ocaid
        FROM ia_jsonl
        WHERE ia_jsonl.sole_isbn_13 IS 1 AND ia_jsonl.ol_edition_id IS NULL OR ia_jsonl.ol_edition_id IS ""
        """
        return self.query(sql)

    def get_ol_edition_has_ocaid_but_no_ia_source_record(self) -> list[Any]:
        """
        Get records where an Open Library Edition has an OCAID but the source_record
        key has no 'ia:<ocaid>' value.
        """
        sql = """
        SELECT ol.ol_ocaid,
               ol.ol_edition_id
        FROM   ol
        WHERE  ol.ol_ocaid IS NOT NULL
        AND    ol.has_ia_source_record IS 0
        """
        return self.query(sql)

    def get_work_ids_associated_with_different_ol_works(self) -> list[Any]:
        """
        Get Internet Archive OCAIDs (and Open Library Work IDs), where different Open
        Library Work IDs link to the same OCAID.
        NOTE: This report largely seems to find works where one is a redirect to the
        other. Better to use the Works dump?
        """
        sql = """
        SELECT     ol.ol_ocaid,
                   ol.ol_edition_id,
                   ol.ol_work_id,
                   ia.ia_ol_edition_id,
                   ia.ia_ol_work_id
        FROM       ia
        INNER JOIN ol
        ON         ia.ia_id = ol.ol_ocaid
        WHERE      ia.ia_ol_work_id IS NOT ol.ol_work_id
        """
        return self.query(sql)

    def get_ia_id_with_same_ol_edition_id(self) -> list[Any]:
        """
        Get (Internet Archive OCAID, Open Library Edition ID) pairings where the Open
        Library edition ID is associated with more than one Internet Archive OCAID.

        NOTE: Many of these duplicates are because the Internet Archive dump includes
        the same OCAID with many different ISBNs, and in doing so it links, usually, to
        the same Open Library edition ID.
        """
        sql = """
        SELECT a.ia_id,
               a.ia_ol_edition_id
        FROM   ia AS a
               JOIN (SELECT ia_id,
                            ia_ol_edition_id,
                            Count(*)
                     FROM   ia
                     GROUP  BY ia_ol_edition_id
                     HAVING Count(*) > 1) AS b
                 ON a.ia_ol_edition_id = b.ia_ol_edition_id
        ORDER  BY a.ia_ol_edition_id
        """
        return self.query(sql)

    def get_broken_ol_ia_backlinks_after_edition_to_work_resolution0(self) -> list[Any]:
        """
        This appears to find backlinks that are definitely broken because the OCAIDs
        match but even after resolution, using works as a proxy, the IA link is
        internally inconsistent between its work and edition reference. Maybe
        """
        sql = """
        SELECT     ia.ia_id,
                   ia.ia_ol_work_id,
                   ia.resolved_ia_ol_work_id,
                   ol.ol_work_id,
                   ol.resolved_ol_work_id
        FROM       ia
        INNER JOIN ol
        ON         ia.ia_id = ol.ol_ocaid
        WHERE      ia.resolved_ia_ol_work_id IS NOT ol.resolved_ol_work_id
        AND        ia.ia_ol_work_id IS NOT ia.resolved_ia_ol_work_id
        """
        return self.query(sql)

    def get_broken_ol_ia_backlinks_after_edition_to_work_resolution1(self) -> list[Any]:
        """
        Similar to version 1, but with many false positives. Seems to find works that
        are likely to be merge candidates. Even after resolution of works, the works
        are different on both ends, but many appear to be works that should be merged.
        """
        sql = """
        SELECT     ia.ia_id,
                   ia.ia_ol_work_id,
                   ia.resolved_ia_ol_work_id,
                   ol.ol_work_id,
                   ol.resolved_ol_work_id
        FROM       ia
        INNER JOIN ol
        ON         ia.ia_id = ol.ol_ocaid
        WHERE      ia.resolved_ia_ol_work_id IS NOT ol.resolved_ol_work_id
        AND        ia.ia_ol_work_id IS NOT NULL
        """
        return self.query(sql)
