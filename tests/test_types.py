# import pytest

from reconcile.datatypes import ParsedEdition, ParsedRedirect


def test_parse_redirect_returns_list():
    redirect = ParsedRedirect(origin_id="OL001M", destination_id="OL002M")
    assert redirect.to_list() == ["OL001M", "OL002M"]


def test_parse_edition_returns_list():
    edition = ParsedEdition(
        edition_id="OL001M",
        work_id="OL001W",
        ocaid="johnmuirtrail",
        isbn_13="1234567890123",
        has_multiple_works=0,
        has_ia_source_record=1,
        has_cover=0,
        isbn_13s="123,456",
    )
    assert edition.to_list() == [
        "OL001M",
        "OL001W",
        "johnmuirtrail",
        "1234567890123",
        0,
        1,
        0,
        "123,456",
    ]
