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
    has_multiple_works: int = 0
    has_ia_source_record: int = 0

    def to_list(self) -> list[str | int | None]:
        return [
            self.edition_id,
            self.work_id,
            self.ocaid,
            self.has_multiple_works,
            self.has_ia_source_record,
        ]
