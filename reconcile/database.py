import sqlite3
from typing import Any, Iterator


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

    def get_ol_ia_id_differences(self):
        """
        Get inconsistent Open Library IDs (OLIDs) based on the associated Internet
        Archive OCAID, according to the OLID that Open Library itself associates
        with an OCAID, and with the OLID that Internet Archive associates with an
        ocaid.
        """
        sql = "SELECT * FROM reconcile WHERE (ia_ol_edition_id IS NOT ol_edition_id) AND (ol_edition_id IS NOT NULL AND ia_ol_edition_id IS NOT NULL)"
        return self.query(sql)

    def get_ocaid_where_ol_edition_has_ocaid_and_ia_has_no_ol_edition(self):
        """
        Get records where an Open Library edition has an OCAID but Internet
        Archive has no Open Library edition associated with that OCAID.
        """
        sql = "SELECT ia_id, ol_edition_id FROM reconcile WHERE (ol_edition_id IS NOT NULL) AND (ia_ol_edition_id IS NULL)"
        return self.query(sql)
