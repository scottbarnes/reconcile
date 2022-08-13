import datetime
import gzip
import os
import shutil
import sys
from inspect import cleandoc

import requests


def download_file(urls: list[str]) -> str:
    """
    Download a file without storing it all in RAM and return the filename.
    If multiple files are in the list, the script will continue to the next
    each time there is a 404.
    Adapted from https://stackoverflow.com/a/39217788
    """
    local_filename = ""
    while urls:
        url = urls.pop(0)
        local_filename = url.split("/")[-1]
        with requests.get(url, stream=True) as response:
            if response.status_code == 404:
                continue
            else:
                urls = []

            with open(local_filename, "wb") as file:
                shutil.copyfileobj(response.raw, file)

    return local_filename


def extract_gzip(file: str) -> str:
    """
    Extract a gzipped file and return the filename.
    """
    # Parse file.name.ext.gz to file.name.ext
    output_filename = ".".join(file.split(".")[:-1])
    with gzip.open(file, "rb") as in_file, open(output_filename, "wb") as out_file:
        shutil.copyfileobj(in_file, out_file)

    return output_filename


def get_ia_dump_urls(
    dt: datetime.date, count: int, initial: bool = True, urls: list[str] = []
) -> list[str]:
    """
    There doesn't seem to be a 'latest' file for the IA
    [date]_inlibrary_direct.tsv dump. As they are generated on the first of each
    month, this script tries to guess the most recent {count} URLs based on the
    system date, accounting for the fact the month may change and the file may
    not be uploaded yet.

    This is a recursive function that returns itself, decrements the count, and
    eventually returns a list of the download URLs based on the current month.
    """
    URL_PREFIX = "https://archive.org/download/ia-abc-historical-data/"
    URL_SUFFIX = "_physical_direct.tsv"

    # All done
    if count <= 0:
        return urls

    # After initial iteration, the passed dt is always the 1st. Go to the last
    # day of the previous month, then set the day to the first again. Use this
    # date to build a new URL, and append it to the list. Call again and
    # decrement {count}
    if initial is False:
        prior_month = dt - datetime.timedelta(days=1)
        prior_month = prior_month.replace(day=1)
        prior_month_parsed = prior_month.strftime("%Y%m%d")
        url = URL_PREFIX + prior_month_parsed + URL_SUFFIX
        urls.append(url)
        return get_ia_dump_urls(prior_month, count - 1, False, urls)

    # First pass. Use the passed dt and set it to the first. Use this to build
    # the first URL to try. Call again and decrement {count}.
    else:
        first = dt.replace(day=1)
        first_parsed = first.strftime("%Y%m%d")
        url = URL_PREFIX + first_parsed + URL_SUFFIX
        urls.append(url)
        return get_ia_dump_urls(first, count - 1, False, urls)


def get_and_extract_data(show_prompt: bool = True) -> None:
    """
    Download the latest OL editions dump from
    https://openlibrary.org/data/ol_dump_editions_latest.txt.gz and the latest
    (ideally) [date]_physical_direct.tsv, from
    https://archive.org/download/ia-abc-historical-data/ which has the
    ia_od <-> ia_ol_id mapping.

    After download, extract if necessary and delete unextracted files.
    """
    OL_EDITIONS_DUMP_URL = [
        "https://openlibrary.org/data/ol_dump_editions_latest.txt.gz"
    ]
    # Get The three most recent possible URLs for the IA
    # _physical_direct_direct.tsv
    today = datetime.date.today()
    IA_PHYSICAL_DIRECT_DUMP_URLS = get_ia_dump_urls(today, 3)

    # Prompt whether to continue. Use --show_prompt=False to skip.
    cwd = os.getcwd()
    response = ""
    if show_prompt:
        response = input(
            cleandoc(
                f"""This is a convenience function to download the necessary files into
            {cwd + '/files/'}. As of August this takes about 38GB. If you wish to
            store the files elsewhere, see README.rst for information on manually
            fetching the files.

            (Specify --show_prompt=False to suppress this message.)

            Continue? (y/n): """
            )
        )
        response = response.lower()

        if response not in ["y", "yes"]:
            print(
                cleandoc(
                    """See README.md for directions to manually download the files and
                specify the paths."""
                )
            )
            sys.exit(0)

    # User is continuing or skipped the prompt.
    # Create ./files if necessary.
    files_exists = os.path.exists(cwd + "/files")
    if not files_exists:
        os.mkdir(cwd + "/files")

    # Use ./files as the CWD from now on.
    os.chdir(cwd + "/files")
    cwd = os.getcwd()

    # Fetch the IA and OL files, along with renaming and extracting them.
    # See README.rst for a clearer explanation of which files are being
    # downloaded.
    print("NOTE: Downloads/extraction can take some time and there is no status bar.")
    print(
        f"Trying to fetch the Internet Archive dump from {IA_PHYSICAL_DIRECT_DUMP_URLS}"
    )
    ia_file = download_file(IA_PHYSICAL_DIRECT_DUMP_URLS)
    # Rename the IA file to something consistent.
    os.replace(ia_file, "ia_physical_direct_latest.tsv")
    ia_file = cwd + "/ia_physical_direct_latest.tsv"
    print(f"Trying to fetch the Open Library editions dump from {OL_EDITIONS_DUMP_URL}")
    ol_file_gz = download_file(OL_EDITIONS_DUMP_URL)

    # Only the OL dump needs extraction
    print(f"Trying to extract {ol_file_gz}")
    ol_file = extract_gzip(ol_file_gz)
    os.remove(ol_file_gz)

    ia_file_path = cwd + "/" + ia_file
    ol_file_path = cwd + "/" + ol_file
    print(f"All done. Files downloaded to:\n{ia_file_path}\n{ol_file_path}")
