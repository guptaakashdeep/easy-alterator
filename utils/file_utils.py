"""Module to handle file and filepath related operations."""

import os
from s3_utils import validate_s3_object, list_s3_objects, read_s3_file
import yaml


def check_paths(files):
    """
    Checks if the provided file or directory path or list of paths is valid.
    Raises Exception in case of invalid file/paths
    :param files: list or str
    :return: None
    """
    valid = True
    if isinstance(files, list):
        for file_path in files:
            if file_path.startswith("s3://"):
                valid = validate_s3_object(file_path)
                if not valid:
                    valid = False
                    print(f"{file_path} is invalid.")
            else:
                if not os.path.exists(file_path):
                    valid = False
                    print(f"{file_path} is invalid.")
    else:
        if isinstance(files, str):
            if files.startswith("s3://"):
                valid = validate_s3_object(files)
                if not valid:
                    valid = False
                    print(f"{files} is invalid.")
            elif not os.path.exists(files):
                valid = False
                print(f"{files} is invalid.")
        else:
            valid = False
            print("path format is invalid.")
    if not valid:
        raise Exception("One or more provided paths are invalid")


def filter_files(paths, prefix, suffix, **kwargs):
    """
    Filtering all the DDL files that needs to be included for tables schema update.
    :param paths: list of paths
    :param prefix: DDL file prefix
    :param suffix: DDL file suffix
    :param kwargs: additional parameters: table_names
    :return: filtered list
    """
    table_list = None
    # in case prefix is not provided
    if prefix is None:
        prefix = ""
    if "table_names" in kwargs:
        table_list = kwargs["table_names"]
    file_list = []
    for path in paths:
        files_list = []
        is_cloud_path = False
        # if the path is a directory, filtering all the files starting with prefix and suffix in file.
        if (not path.startswith("s3://")) and os.path.isdir(path):
            files_list = os.listdir(path)
        elif path.startswith("s3://") and len(path.split(".")) == 1:
            is_cloud_path = True
            files_list = list_s3_objects(path)
        else:
            file_list.append(path)
        if files_list:
            if is_cloud_path:
                filtered_files = list(
                    filter(
                        lambda x: x.rsplit("/", 1)[1].startswith(prefix)
                        and x.rsplit("/", 1)[1].endswith(suffix),
                        files_list,
                    )
                )
            else:
                filtered_files = list(
                    filter(
                        lambda x: x.startswith(prefix) and x.endswith(suffix),
                        files_list,
                    )
                )
            if table_list:
                print("inside filter 2", len(table_list))
                # filtering the file only for the tables mentioned in table list.
                if is_cloud_path:
                    cloud_filenames = [f.rsplit("/", 1)[1] for f in filtered_files]
                    final_list = list(
                        filter(
                            lambda name: name is not None,
                            [
                                f"{prefix}{x}.{suffix}"
                                if f"{prefix}{x}.{suffix}" in cloud_filenames
                                else None
                                for x in table_list
                            ],
                        )
                    )
                else:
                    final_list = list(
                        filter(
                            lambda name: name is not None,
                            [
                                f"{prefix}{x}.{suffix}"
                                if f"{prefix}{x}.{suffix}" in filtered_files
                                else None
                                for x in table_list
                            ],
                        )
                    )
            else:
                final_list = filtered_files
            # need to add support for Windows OS ?? Supports only linux FS as of now.
            file_list = file_list + list(
                map(
                    lambda x: f"{path}{x}" if path.endswith("/") else f"{path}/{x}",
                    final_list,
                )
            )
    # print(len(os.listdir(paths[0])))
    # print(len(filtered_files))
    # print(len(final_list))
    return file_list


def read_yaml(path):
    """
    Reads yaml file from the provided path.
    :param path:
    :return: json object
    """
    if path.startswith("s3://"):
        data = yaml.safe_load(read_s3_file(path))
    else:
        with open(path, "r", encoding="utf-8") as fs:
            data = yaml.safe_load(fs)
    return data
