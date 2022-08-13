import csv

# Various utility functions.


def nuller(v):
    """
    Utility function to set '' to None so it's Null in DB. This may be
    pointless.
    """
    if v == "":
        return None
    else:
        return v


def query_output_writer(query_result: list[str], out_file: str) -> None:
    """
    Helper function to write output from queries to TSV.
    """
    with open(out_file, "w") as file:
        writer = csv.writer(file, delimiter="\t")
        for row in query_result:
            writer.writerow(row)
