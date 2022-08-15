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
- create the database; and
- run the reports.

### Step by step
```
poetry run python reconcile/main.py fetch-data
```
Fetch the exported database data (this takes a bit).
Note: This is a convenience function. The data can be manually into /you/path/to/reconcile/files
- Open Library Editions dump: https://openlibrary.org/data/ol_dump_editions_latest.txt.gz
- The newest Internet Archive physical_direct.tsv: https://archive.org/download/ia-abc-historical-data/ (it's near the bottom)

```
poetry run reconcile/main.py parse-data
```
Parse the data so it's readable more quickly by reconcile. This takes about 10 minutes
on my computer, but it only needs to be done once.

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

### Contributing
- Run the tests manually: `poetry run pytest`
- Using pre-commit: `poetry run pre-commit install`
