# reconcile
## A Python CLI tool for comparing data in Open Library and Internet Archive databases
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

### Requirements
- [Python >= 3.10](https://www.python.org/downloads/release/python-3100/)
- [Poetry](https://github.com/python-poetry/poetry) ([Installation](https://github.com/python-poetry/poetry#installation))

### Get the source
`git clone git@github.com:scottbarnes/reconcile.git`

### Using Poetry
- `cd /path/to/cloned/reconcile`
- `poetry install`
- `poetry run python reconcile/main.py`

Optionally, to open a shell within the Poetry virtual environment, where commands can be
invoked directly:
- `poetry shell`
- `python reconcile/main.py`

### Configuration
There isn't much configuration, but if you want to store the files in somewhere other
than /path/to/reconcile/files, you can export the following, hopefully relatively
self-explanatory environment variables:
- IA_PHYSICAL_DIRECT_DUMP
- OL_EDITIONS_DUMP
- OL_EDITIONS_DUMP_PARSED
- REPORT_OL_IA_BACKLINKS
- REPORT_OL_HAS_OCAID_IA_HAS_NO_OL_EDITION
- REPORT_OL_HAS_OCAID_IA_HAS_NO_OL_EDITION_JOIN
- REPORT_EDITIONS_WITH_MULTIPLE_WORKS
- REPORT_IA_LINKS_TO_OL_BUT_OL_EDITION_HAS_NO_OCAID
- REPORT_OL_EDITION_HAS_OCAID_BUT_NO_IA_SOURCE_RECORD

### Running reconcile
Whether you `poetry run python reconcile/main.py` or run `poetry shell` and then `python reconcile/main.py`, either way, you should see something similar to:
```
NAME
    main.py

SYNOPSIS
    main.py COMMAND

COMMANDS
    COMMAND is one of the following:

     fetch-data
       Download the latest OL editions dump from https://openlibrary.org/data/ol_dump_editions_latest.txt.gz and the latest (ideally) [date]_physical_direct.tsv, from https://archive.org/download/ia-abc-historical-data/ which has the ia_od <-> ia_ol_id mapping.

     parse-data
       Parse an Open Library editions dump from https://openlibrary.org/developers/dumps and write the output to a .tsv in the format: ol_edition_id   ol_work_id      ol_ocaid        has_multiple_works      has_ia_source_record

     create-db
       Create the tables and insert the data. NOTE: You must parse the data first.

     all-reports
       Just run all the reports because these commands are way too long to type.
```
Run the commands in order, top to bottom, to:
- download the necessary database dumps;
- parse them;
- create the database and import the parsed data, and
- run the reports.

### Step by step
```
poetry run python reconcile/main.py fetch-data
```
Fetch the exported database data. This takes me about 8 minutes.
Note: This is a convenience function. The data can be manually into /you/path/to/reconcile/files
- Open Library Editions dump: https://openlibrary.org/data/ol_dump_editions_latest.txt.gz
- The newest Internet Archive physical_direct.tsv: https://archive.org/download/ia-abc-historical-data/ (it's near the bottom)

```
poetry run reconcile/main.py parse-data
```
Parse the data so it's readable more quickly by reconcile. This takes me about 10
minutes, but it only needs to be done once.

```
poetry run python reconcile/main.py create-db
```
Create the database tables and insert the parased data. This takes about 6 minutes on my
computer, but it only needs to be done once.

```
poetry run python reconcile/main.py all-reports
```
Run all the reports. It will print out some poorly formatted information to the screen,
and also write the data to TSV files and print their location. This one takes maybe 30
seconds.

### The reports
The handful of reports are:
- Total (ostensibly) broken back-links to Open Library
- Total Internet Archive records where an Open Library Edition has an OCAID but Internet Archive has no Open Library Edition
- Total Internet Archive records where an Open Library Edition has an OCAID but Internet
  Archive has no Open Library Edition (this time using a database join, which gets
  a slightly different, and more accurate, result, because of how the data for the first
  query is collated
- Total Open Library Editions with more than on associated work
- Total Internet Archive items that link to an Open Library Edition, and that Edition does not have an OCAID
- Total Open Library Editions that have an OCAID but have no Internet Archive entry in their source_records

### Contributing
- Run the tests manually: `poetry run pytest`
- Using pre-commit: `poetry run pre-commit install`, then just `git add`, `git commit`
  etc. as usual.
