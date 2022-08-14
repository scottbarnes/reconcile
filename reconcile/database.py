import sqlite3
from collections.abc import Iterator
from typing import Any


class Database:
    """
    A class for more easily interacting with the database.
    Adapted from https://stackoverflow.com/a/38078544.
    """

    def __init__(self, name: str):
        self._conn = sqlite3.connect(name)
        self._cursor = self._conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def connection(self) -> sqlite3.Connection:
        return self._conn

    @property
    def cursor(self) -> sqlite3.Cursor:
        return self._cursor

    def commit(self):
        self.connection.commit()

    def close(self, commit: bool = True):
        if commit:
            self.commit()
        self.connection.close()

    def execute(self, sql: str, params: tuple = None):
        self.cursor.execute(sql, params or ())

    def executemany(self, sql: str, params: tuple | Iterator | None = None):
        self.cursor.executemany(sql, params or ())

    def fetchall(self) -> list[Any]:
        return self.cursor.fetchall()

    def fetchone(self) -> Any:
        return self.cursor.fetchone()

    def query(self, sql, params=None) -> list[Any]:
        self.cursor.execute(sql, params or ())
        return self.fetchall()

    def get_ol_ia_id_differences(self) -> list[Any]:
        """
        Get inconsistent Open Library IDs (OLIDs) based on the associated Internet
        Archive OCAID, according to the OLID that Open Library itself associates
        with an OCAID, and with the OLID that Internet Archive associates with an
        ocaid.
        """
        sql = """SELECT * FROM ia WHERE (ia_ol_edition_id IS NOT ol_edition_id)
            AND (ol_edition_id IS NOT NULL AND ia_ol_edition_id IS NOT NULL)"""
        return self.query(sql)

    def get_editions_with_multiple_works(self) -> list[Any]:
        """
        Get records where an Open Library Edition has more than one associated Work.
        """
        sql = """SELECT ol_edition_id FROM ol WHERE has_multiple_works IS 1"""
        return self.query(sql)

    def get_ocaid_where_ol_edition_has_ocaid_and_ia_has_no_ol_edition(
        self,
    ) -> list[Any]:
        """
        Get records where an Open Library edition has an OCAID but Internet
        Archive has no Open Library edition associated with that OCAID.
        """
        sql = """SELECT ia_id, ol_edition_id FROM ia WHERE (ol_edition_id IS NOT
            NULL) AND (ia_ol_edition_id IS NULL)"""
        return self.query(sql)

    def get_ocaid_where_ol_edition_has_ocaid_and_ia_has_no_ol_edition_join(
        self,
    ) -> list[Any]:
        """
        Same as get_records_where_ol_has_ocaid_but_ia_has_no_ol_edition, but this time
        using a database inner join.
        """
        sql = """
        SELECT
            ia.ia_id,
            ol.ol_edition_id
        FROM
            ia INNER JOIN ol
            ON ia.ia_id = ol.ol_ocaid
        WHERE
            ia.ia_ol_edition_id IS NULL
        """
        return self.query(sql)

    def get_ia_links_to_ol_but_ol_edition_has_no_ocaid(self) -> list[Any]:
        """
        Get records where Internet Archive links to an Open Library Edition, but that
        Open Library Edition has no OCAID.
        """
        sql = """
        SELECT
            ia.ia_id,
            ia.ia_ol_edition_id
        FROM
            ia inner join ol
            ON ia.ia_ol_edition_id = ol.ol_edition_id
        WHERE
            ol.ol_ocaid IS NULL
        """
        return self.query(sql)
