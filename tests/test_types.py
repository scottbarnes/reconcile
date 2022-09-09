# import pytest

from reconcile.types import ParsedEdition, ParsedRedirect


def test_parse_redirect_returns_list():
    redirect = ParsedRedirect(origin_id="OL001M", destination_id="OL002M")
    assert redirect.to_list() == ["OL001M", "OL002M"]


def test_parse_edition_returns_list():
    edition = ParsedEdition(
        edition_id="OL001M",
        work_id="OL001W",
        ocaid="johnmuirtrail",
        has_multiple_works=0,
        has_ia_source_record=1,
    )
    assert edition.to_list() == ["OL001M", "OL001W", "johnmuirtrail", 0, 1]
