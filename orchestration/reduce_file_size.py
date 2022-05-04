"""
Set of routines to reduce manifest.json file
"""
import json
import os
import argparse
import logging

from typing import Dict, Any, Union

ENCODING = "utf-8"
info = {}

"""
picked "nodes_accepted_values" up from 2 jobs:
- dbt_test_source
- dbt_model_source
"""
NODES_ACCEPTED_VALUES = {
    "unique_id",
    "name",
    "alias",
    "database",
    "schema",
    "package_name",
    "tags",
    "refs",
    "resource_type",
    "test_metadata",
    "config",
    "depends_on",
}


def reduce_nodes_section(source_nodes_json: dict) -> dict:
    """
    Function to reduce section in use from "nodes" section
    in manifest.json file
    :rtype: dict
    :param source_nodes_json: "nodes" section from manifest.json file
    """
    reduced_nodes_json = {}

    for nodes_key, nodes_value in source_nodes_json:
        temp_dict: Dict[Any, Union[Union[str, None, Dict[Any, Any]], Any]] = {}
        for nodes_child_key, nodes_child_value in nodes_value.items():
            if nodes_child_key in NODES_ACCEPTED_VALUES:
                # as run manifest.json from a local machine need to
                # rename database value
                if nodes_child_key == "config":
                    # exception for [config][severity],
                    # the only sub-level
                    # we need from [config] and will reduce the size
                    temp_dict[nodes_child_key] = dict(
                        severity=nodes_child_value.get("severity")
                    )
                else:
                    temp_dict[nodes_child_key] = nodes_child_value
        reduced_nodes_json[nodes_key] = temp_dict
    return reduced_nodes_json


def save_json_file(reduced_json: dict, target_file: str) -> None:
    """
    Save json file
    :param reduced_json: dict
    :param target_file: str
    :return: None
    """

    with open(target_file, "w", encoding=ENCODING) as reduced_json_file:
        json.dump(reduced_json, reduced_json_file, indent=4)
        info["target"] = (
            f"File size for {target_file} is : "
            f"{os.path.getsize(target_file) / 1024 / 1024:.2f} "
            f"MB"
        )


def load_json_file(source_file: str) -> dict:
    """
    Load json file from the file system
    :param source_file: str
    :return: dict
    """
    try:
        with open(source_file, "r", encoding=ENCODING) as manifest_file:
            json_data = json.load(manifest_file)
            info["source"] = (
                f"File size for {source_file} is :"
                f" {os.path.getsize(source_file) / 1024 / 1024:.2f} MB"
            )

        return json_data
    except FileNotFoundError as file_not_found_error:
        logging.error(
            f"File {source_file} doesn't exist! Error: {file_not_found_error}"
        )
        raise


def reduce_manifest_file(raw_json: dict) -> dict:
    """
    Function to reduce section in use from manifest.json file
    What is interesting, are 2 sections:
        - "metadata"
        - "nodes"
    :param raw_json: dict
    :return: dict
    """

    reduced_json = {
        "metadata": raw_json["metadata"],
        "nodes": reduce_nodes_section(raw_json["nodes"].items()),
    }

    return reduced_json


def get_file_size(file_to_measure: str) -> float:
    """
    param file_name: file name is input
    return: file size in MBs
    """
    return os.path.getsize(file_to_measure) / 1024 / 1024


def main():
    """
    Standalone main function to test routine if needed
    """

    parser = argparse.ArgumentParser(
        description="Parameters for reducing manifest.json file"
    )
    parser.add_argument(
        "-i",
        "--inputfile",
        dest="inputfile",
        type=str,
        help="Full destination for input file (.json)",
    )
    parser.add_argument(
        "-o",
        "--outputfile",
        dest="outputfile",
        type=str,
        help="Full destination for output file (.json)",
    )

    args = parser.parse_args()

    source_file = args.inputfile
    target_file = args.outputfile

    raw_json = load_json_file(source_file=source_file)
    reduced_json = reduce_manifest_file(raw_json=raw_json)

    save_json_file(reduced_json=reduced_json, target_file=target_file)

    print(info)


if __name__ == "__main__":
    main()
