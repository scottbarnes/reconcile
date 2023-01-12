"""Miscellaneous data types used for Reconcile."""
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ParsedRedirect:
    """
    An REDIRECT type as parsed from the Open Library 'all' dump.
    NOTE: {destination_id} is NOT the final destination ID. It may be intermediate.
    """

    origin_id: str
    destination_id: str

    def to_list(self) -> list[str]:
        return [self.origin_id, self.destination_id]


@dataclass(frozen=True, slots=True)
class ParsedEdition:
    """An EDITION as parsed from the Open Library 'all' dump."""

    edition_id: str
    work_id: str | None = None
    ocaid: str | None = None
    isbn_13: str = ""
    has_multiple_works: int = 0
    has_ia_source_record: int = 0
    has_cover: int = 0
    isbn_13s: str = (
        ""  # This becomes a CSV of the ISBNs because they're written to a file.
    )

    def to_list(self) -> list[str | int | None]:
        return [
            self.edition_id,
            self.work_id,
            self.ocaid,
            self.isbn_13,
            self.has_multiple_works,
            self.has_ia_source_record,
            self.has_cover,
            self.isbn_13s,
        ]
