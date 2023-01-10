from reconcile.internet_archive import parse_ia_inlibrary_jsonl


def test_parse_ia_inlibrary_jsonl() -> None:
    expected = [
        (
            "links_to_ol_edition_but_ol_does_not_link_to_it",
            "OL010M",
            True,
            False,
            "9781933060224",
            '{"identifier": "links_to_ol_edition_but_ol_does_not_link_to_it", "isbn": ["1933060220", "9781933060224"], "openlibrary_work": "OL010W", "openlibrary_edition": "OL010M"}\n',  # noqa E501
        ),
        (
            "links_both_ways",
            "OL011M",
            True,
            False,
            "9781451675504",
            '{"identifier": "links_both_ways", "isbn": ["9781451675504", "145167550X"], "openlibrary_work": "OL011W", "openlibrary_edition": "OL011M"}\n',  # noqa E501
        ),
        (
            "links_to_ol_edition_but_ol_does_not_link_to_it_two_isbn_13",
            "OL012M",
            False,
            True,
            "9781933060224",
            '{"identifier": "links_to_ol_edition_but_ol_does_not_link_to_it_two_isbn_13", "isbn": ["1933060220", "9781933060224", "9781566199094"], "openlibrary_work": "OL012W", "openlibrary_edition": "OL012M"}\n',  # noqa E501
        ),
    ]

    seed_file = "./tests/seed_ia_inlibrary.jsonl"
    items = parse_ia_inlibrary_jsonl(seed_file)
    parsed_content = list(items)

    assert parsed_content[:3] == expected
