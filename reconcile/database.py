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


class Database:
    """
    A class for more easily interacting with the database.
    Adapted from https://stackoverflow.com/a/38078544.
    """

    def __init__(self, name: str):
        # Create any necessary paths. This deserves a better fix.
        paths = [FILES_DIR, REPORTS_DIR]
        [path_check(d) for d in paths]

        self._conn = sqlite3.connect(name, timeout=60)
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

    def executemany(self, sql: str, params: tuple | Iterable | None = None):
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
            ia INNER JOIN ol
            ON ia.ia_ol_edition_id = ol.ol_edition_id
        WHERE
            ol.ol_ocaid IS NULL
        """
        return self.query(sql)

    def get_ol_edition_has_ocaid_but_no_ia_source_record(self) -> list[Any]:
        """
        Get records where an Open Library Edition has an OCAID but the source_record
        key has no 'ia:<ocaid>' value.
        """
        sql = """
        SELECT
            ol.ol_ocaid,
            ol.ol_edition_id
        FROM
            ol
        WHERE
            (ol.ol_ocaid IS NOT NULL) AND (ol.has_ia_source_record is 0)
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
        SELECT
            ol.ol_ocaid,
            ol.ol_edition_id,
            ol.ol_work_id,
            ia.ia_ol_edition_id,
            ia.ia_ol_work_id
        FROM
            ia INNER JOIN ol
            ON ia.ia_id = ol.ol_ocaid
        WHERE
            ia.ia_ol_work_id IS NOT ol.ol_work_id
        """
        return self.query(sql)
