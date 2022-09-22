"""
Convenience functions to automatically download and extract the neecessary data files.
An alternative is to get the files from:
    https://archive.org/download/ia-abc-historical-data/
    https://openlibrary.org/data/ol_dump_editions_latest.txt.gz
With that done, they can be put in ./files/ (see setup.cfg to change). The Open Library
editions dump will need to be extracted first.
"""
import configparser
import datetime
import gzip
import os
import shutil
import sys
from inspect import cleandoc

import requests
import typer
from tqdm.auto import tqdm

# Load configuration
config = configparser.ConfigParser()
config.read("setup.cfg")
CONF_SECTION = "reconcile-test" if "pytest" in sys.modules else "reconcile"
FILES_DIR = config.get(CONF_SECTION, "files_dir")

app = typer.Typer()


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

            result = response.headers.get("Content-Length")
            total_length = int(result) if result else 0

            with tqdm.wrapattr(  # noqa SIM117
                response.raw, "read", total=total_length, desc=""
            ) as raw:

                with open(local_filename, "wb") as file:
                    shutil.copyfileobj(raw, file, 128 * 1024)

    return local_filename


def extract_gzip(file: str) -> str:
    """
    Extract a gzipped file and return the filename.
    Renames file.name.ext.gz to file.name.ext
    """
    output_filename = ".".join(file.split(".")[:-1])
    with gzip.open(file, "rb") as in_file, open(output_filename, "wb") as out_file:
        shutil.copyfileobj(in_file, out_file, 128 * 1024)

    return output_filename


def get_ia_dump_urls(
    urls: list[str], dt: datetime.date, count: int, initial: bool = True
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
    urls = urls or []
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
        return get_ia_dump_urls(urls, prior_month, count - 1, False)

    # First pass. Use the passed dt and set it to the first. Use this to build
    # the first URL to try. Call again and decrement {count}.
    else:
        first = dt.replace(day=1)
        first_parsed = first.strftime("%Y%m%d")
        url = URL_PREFIX + first_parsed + URL_SUFFIX
        urls.append(url)
        return get_ia_dump_urls(urls, first, count - 1, False)


@app.command()
def fetch_data(show_prompt: bool = True) -> None:
    """
    Download the latest OL editions dump from
    https://openlibrary.org/data/ol_dump_editions_latest.txt.gz and the latest
    (ideally) [date]_physical_direct.tsv, from
    https://archive.org/download/ia-abc-historical-data/ which has the
    ia_od <-> ia_ol_id mapping.

    After download, extract if necessary and delete unextracted files.
    """
    OL_EDITIONS_DUMP_URL = ["https://openlibrary.org/data/ol_dump_latest.txt.gz"]
    # Get The three most recent possible URLs for the IA
    # _physical_direct_direct.tsv
    today = datetime.date.today()
    IA_PHYSICAL_DIRECT_DUMP_URLS = get_ia_dump_urls([], today, 3)

    # Prompt whether to continue. Use --show_prompt=False to skip.
    cwd = os.getcwd()
    response = ""
    if show_prompt:
        response = input(
            cleandoc(
                f"""This is a convenience function to download the necessary files into
            {cwd + '/files/'}. As of August 2022 this takes about 60GB. If you wish to
            store the files elsewhere, see README.md for information on manually
            fetching the files and editing setup.cfg to specify their locations.

            (Specify --show_prompt=False to suppress this message.)

            Continue? (y/n): """
            )
        )
        response = response.lower()

        if response not in ["y", "yes"]:
            print(
                cleandoc(
                    """See README.md for directions to manually download the files and
                specify the paths via environment variables."""
                )
            )
            sys.exit(0)

    # User is continuing or skipped the prompt.
    # Use FILES_DIR as the CWD from now on.
    os.chdir(FILES_DIR)
    cwd = os.getcwd()

    # Fetch the IA and OL files, along with renaming and extracting them.
    # See README.rst for a clearer explanation of which files are being
    # downloaded.
    print(
        f"Trying to fetch the Internet Archive dump from {IA_PHYSICAL_DIRECT_DUMP_URLS}"
    )
    print("This may take a while. Go get some runts.")
    ia_file = download_file(IA_PHYSICAL_DIRECT_DUMP_URLS)
    # Rename the IA file to something consistent.
    os.replace(ia_file, "ia_physical_direct_latest.tsv")
    ia_file = "ia_physical_direct_latest.tsv"
    print(f"Trying to fetch the Open Library editions dump from {OL_EDITIONS_DUMP_URL}")
    ol_file_gz = download_file(OL_EDITIONS_DUMP_URL)

    # Only the OL dump needs extraction
    print(f"Trying to extract {ol_file_gz}")
    print("Note: this has no progress bar.")
    ol_file = extract_gzip(ol_file_gz)
    os.remove(ol_file_gz)

    ia_file_path = cwd + "/" + ia_file
    ol_file_path = cwd + "/" + ol_file
    print(f"\nAll done. Files downloaded to:\n{ia_file_path}\n{ol_file_path}\n")
    print("Run with `create-db` to parse and insert the data into the database.")


if __name__ == "__main__":
    app()
