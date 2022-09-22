from pathlib import Path

import pytest

from reconcile.utils import (
    batcher,
    bufcount,
    get_bad_isbn_10s,
    get_bad_isbn_13s,
    path_check,
    record_errors,
)

###########
# Utilities
###########


def test_bufcount() -> None:
    """Count the number of lines in a file."""
    f = Path("peaks.txt")
    if f.exists():
        raise Exception("peaks.txt exists.")
    f.write_text("Olancha\nPeak\n")
    assert bufcount("peaks.txt") == 2
    f.unlink()


def test_bufcount_fails_without_file() -> None:
    """Verify bufcount() fails without a file."""
    with pytest.raises(SystemExit):
        bufcount("MountBrewer.txt")


def test_path_check() -> None:
    """Verify the path creation helper utility works."""
    path = Path("Sierra_Peaks_Section")
    if path.exists():
        raise Exception("'Sierra' directory exists.")
    path_check("Sierra_Peaks_Section")
    assert path.is_dir() is True
    path.rmdir()


class TestFaciallyInvalidIsbns:
    """For testing get_facially_invalid_isbns()."""

    def test_get_bad_isbn_10s_returns_only_bad_isbn_10s(self) -> None:
        # Key: bad, too long, invalid checksum, extra characters but valid, valid
        isbns = ["blob", "12345678901", "X111111111", "111111a1111", "0836931335"]
        assert get_bad_isbn_10s(isbns) == ["blob", "12345678901", "X111111111"]

    def test_get_bad_isbn_13s_returns_only_bad_isbn_13s(self) -> None:
        # Key: bad, too short, invalid checksum, extra characters but valid, valid
        isbns = [
            "blob",
            "123456789011",
            "978-3-16-148410-1",
            "978-3-16-A148410-0",
            "978-3-16-148410-0",
        ]
        assert get_bad_isbn_13s(isbns) == ["blob", "123456789011", "978-3-16-148410-1"]


def test_get_bad_isbn_10s() -> None:
    """Verify bad ISBN 10s are found and accuraterly reported."""
    isbn_10s = ["", "a", "083693133X", "1111111111", "0836931335", "9780735211308"]
    assert (
        get_bad_isbn_10s(isbn_10s=isbn_10s).sort()
        == ["a", "083693133X", "9780735211308"].sort()
    )


def test_check_and_report_bad_isbns_13() -> None:
    """Verify bad ISBN 13s are found and accuraterly reported."""
    isbn_13s = ["a", "0836931335", "1111111111111", "9780735211308"]
    assert (
        get_bad_isbn_13s(isbn_13s=isbn_13s).sort()
        == ["a", "0836931335", "1111111111111"].sort()
    )


def test_record_errors() -> None:
    """Verify errors are written to disk"""
    filename = "record_error_test.txt"
    p = Path(filename)
    record_errors("some test error", filename)
    assert "some test error" in p.read_text()
    p.unlink()


def test_batcher() -> None:
    """
    Verify batcher batches items by {count}, however they're formed, and that if there
    are fewer than {count} items in the last batch, they're still returned.
    """
    ol_ids = iter(["OL1M", "OL2M", ("OL3M", "OL4M"), "OL5M", "OL6M"])
    batch = batcher(ol_ids, 2)
    assert next(batch) == ("OL1M", "OL2M")
    assert next(batch) == (("OL3M", "OL4M"), "OL5M")
    assert next(batch) == ("OL6M",)
