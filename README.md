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

### Feedback
Not that this will get much use, but I'd love to hear if people encounter
`sqlite3.OperationalError: database is locked` while running create-db. If that happens
one can remove SQLite db in `files/reconcile.db` and try the import again.

### Configuration
There isn't much configuration, but if you want to store the files in somewhere other
than /path/to/reconcile/files, take a look at the values under [reconcile] in `setup.cfg`.

Similarly, if you wish to enable data scrubbing, set `scrub_data = True` in `setup.cfg`. Currently this only scrubs (validates) ISBNs, writing bad ISBNs to `./reports/report_bad_isbns.txt`. On my computer this option is fairly expensive and adds about five minutes.

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

     create-db
       Parse the data, create the tables, and insert the data. NOTE: you must fetch the data first.

     all-reports
       Just run all the reports because typing them individually too long.
```
Run the commands in order, top to bottom, to:
- download the necessary database dumps;
- create the database, parse and import the data, and
- run the reports.

### Step by step
```
poetry run python reconcile/main.py fetch-data
```
Fetch the exported database data. This takes me about 8 minutes.
Note: This is a convenience function. The data can be manually into /your/path/to/reconcile/files
- Open Library Editions dump: https://openlibrary.org/data/ol_dump_editions_latest.txt.gz
- The newest Internet Archive physical_direct.tsv: https://archive.org/download/ia-abc-historical-data/ (it's near the bottom)

```
poetry run python reconcile/main.py create-db
```
Parse the data, create the database tables, and insert the parased data. This takes about 6 minutes on my
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

There are also a handful of checks for errors, with recording to `reports/report_errors.txt`
See `setup.cfg` to change the location. *NOTE*: This file isn't deleted to
automatically; rather, successive imports will append to it.

### Contributing
- Run the tests manually: `poetry run pytest`
- Using pre-commit: `poetry run pre-commit install`, then just `git add`, `git commit`
  etc. as usual.
