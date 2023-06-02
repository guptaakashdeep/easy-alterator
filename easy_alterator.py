import re
import yaml
import sys
import os
import boto3
import argparse
from botocore.exceptions import ClientError
import pandas as pd
from copy import deepcopy
from os import popen
from .rule_book import *


def _check_paths(files):
    """
    Checks if the provided file or directory path or list of paths is valid.
    Raises Exception in case of invalid file/paths
    :param files: list or str
    :return: None
    """
    valid = True
    if isinstance(files, list):
        for file_path in files:
            if not os.path.exists(file_path):
                valid = False
                print(f"{file_path} is invalid.")
    else:
        if not os.path.exists(files):
            valid = False
            print(f"{files} is invalid.")
    if not valid:
        raise Exception("Provided paths are invalid")


def _filter_files(paths, prefix, suffix, **kwargs):
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
    if 'table_names' in kwargs:
        table_list = kwargs['table_names']
    file_list = []
    for path in paths:
        # if the path is a directory, filtering all the files starting with prefix and suffix in file.
        if os.path.isdir(path):
            filtered_files = list(filter(lambda x: x.startswith(prefix) and x.endswith(suffix), os.listdir(path)))
            if table_list:
                print("inside filter 2", len(table_list))
                # filtering the file only for the tables mentioned in table list.
                final_list = list(filter(lambda name: name is not None,
                                         [f'{prefix}{x}.{suffix}' if f'{prefix}{x}.{suffix}' in filtered_files else None
                                          for x in table_list]))
            else:
                final_list = filtered_files
            # TODO: need to add support for Windows OS ?? Supports only linux FS as of now.
            file_list = file_list + list(map(lambda x: f'{path}{x}' if path.endswith("/") else f'{path}/{x}', final_list))
        else:
            file_list.append(path)
    # print(len(os.listdir(paths[0])))
    # print(len(filtered_files))
    # print(len(final_list))
    return file_list


def _read_yaml(path):
    """
    Reads yaml file from the provided path.
    :param path:
    :return: json object
    """
    with open(path, 'r') as f:
        data = yaml.safe_load(f)
    return data


def _intial_checks(table_info):
    """
    Runs the initial validation rules
    :param table_info: dict
    :return: bool
    """
    print("run some checks before table updates")
    # RULES:
    # 1. TABLE_TYPE is EXTERNAL
    # 2. TABLE IS A PARQUET TABLE => check serde info
    # 3. NO PARTITION COLUMNS ARE CHANGED (ADDED or REMOVED)
    for key, value in INITIAL_RULE_DICT.items():
        result = value(table_info)
        if not result:
            print(f"{key} validation failed.")
            # TODO: MAYBE update this later to something more meaningful ?
            return False
    return True


def _get_table_details(client, database, table):
    try:
        response = client.get_table(
            DatabaseName=database,
            Name=table
        )
        return response
    except ClientError as error:
        err_response = error.response
        if err_response['Error']['Code'] == 'EntityNotFoundException':
            print(err_response['Message'])
        return err_response
    except Exception as e:
        print("Error occured while getting table from catalog.")
        raise e


def _update_table_schema(glue_client, table, new_cols, del_cols):
    updated_table = deepcopy(table)
    db_name = table['Table']['DatabaseName']
    table_name = table['Table']['Name']
    # removing unecessary keys
    # can be implemented better with @dataclass
    extra_keys = ['DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy', 'IsRegisteredWithLakeFormation',
                  'CatalogId', 'VersionId', 'FederatedTable']
    for key in extra_keys:
        updated_table['Table'].pop(key, None)

    # add new cols:
    existing_columns = table['Table']['StorageDescriptor']['Columns']
    new_cols_list = existing_columns + new_cols

    # remove deleted columns
    if del_cols:
        updated_columns = list(filter(lambda d: d['Name'] not in del_cols, new_cols_list))
    else:
        updated_columns = new_cols_list

    print("Final cols list ==>", updated_columns)
    updated_table['Table']['StorageDescriptor']['Columns'] = updated_columns

    up_response = glue_client.update_table(
        DatabaseName=db_name,
        TableInput=updated_table['Table'])

    # TODO: instead of print return a dictionary or some response ?
    if up_response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print(f"Update successful for {db_name}.{table_name}")
    else:
        print(f"Update failure for {db_name}.{table_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--path", nargs='*', required='--config' not in sys.argv and '-c' not in sys.argv,
                        help="Paths to DDL folder separated by comma.")
    parser.add_argument("-c", "--config", type=str, required='--path' not in sys.argv and '-p' not in sys.argv,
                        help="DDL config yaml.")
    parser.add_argument("-cp", "--key_for_path", type=str,
                        required=('--path' not in sys.argv and '-p' not in sys.argv) and (
                                '--config' in sys.argv or '-c' in sys.argv),
                        help="Key in DDL config file for reading path to DDL folder.")
    parser.add_argument("-fs", "--file_suffix", type=str, required=False, default="hql", choices=['hql', 'txt'],
                        help="Suffix for DDL files to be picked from path.")
    parser.add_argument("-fp", "--file_prefix", type=str, required=False, default="",
                        help="Prefix for DDL files to be picked from path.")

    # print(sys.argv)
    args = parser.parse_args()
    paths = args.path
    ddl_config_path = args.config
    path_key = args.key_for_path
    ddl_file_suffix = args.file_suffix
    ddl_file_prefix = args.file_prefix

    hql_paths = []
    config = None
    # check if the paths provided are valid:
    if paths:
        _check_paths(paths)
        hql_paths = paths
    if ddl_config_path:
        _check_paths(ddl_config_path)
        if os.path.isfile(ddl_config_path):
            if ddl_config_path.endswith(".yaml"):
                config = _read_yaml(ddl_config_path)
                if path_key in config:
                    _check_paths(config[path_key])
                    hql_paths.append(config[path_key])
                else:
                    raise Exception(f"Provided key_for_path is not available in {ddl_config_path} configuration file")
            else:
                raise Exception("Only .yaml configuration files are supported.")
        else:
            raise Exception("Please provide configuration file path with filename.")

    print("DDL paths:", hql_paths)

    # Extract files from path as per the suffix and prefix provided.
    final_file_list = _filter_files(hql_paths, ddl_file_prefix, ddl_file_suffix, table_names=config['tables'])

    # create aws glue client
    glue = boto3.client('glue')

    table_rgx = "TABLE [IF NOT EXISTS]* `(\w+)[\.](\w+)`"
    column_rgs = "`(\w+)`\s+(\w+),*"
    skipped_tables = []
    new_tables = []

    # Fetching AWS ID form EMR
    aws_account_id = str(popen(
        "curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .accountId").read().strip())
    try:
        # loop over DDL files:
        for fname in final_file_list:
            skip = False
            move_to_next = False
            with open(fname, 'r') as f:
                data = f.read().lower().strip().format(aws_account_id=aws_account_id)

            # Check if the table_type = 'EXTERNAL'
            # can get the aws_account id from catalog_id
            """
            with open('sample_response.json', 'r') as f:
                content = f.read()
            partition_keys = [{"Name": "day_rk", "Type": "date"}, {"Name": "jurisdiction_type", "Type": "string"}]
            res = json.loads(content) + partition_keys
            """
            if data:
                table_match = re.search(table_rgx, data, flags=re.IGNORECASE)
                db, table = table_match.groups()
                table_name = f'{db}.{table}'

                # Check if hql is create statement.
                if not data.startswith("create"):
                    print(f"HQL provided for {table_name} is not a create statement.")
                    skipped_tables.append(table_name)
                    skip = True
                else:
                    # run initial checks on HQL
                    print("Running initial validation.")
                    validation_results = _intial_checks(data)
                    if validation_results:
                        print(f"Initial validations are successful for {table_name}.")
                    else:
                        print(f"Initial validation failed for provided HQL {table_name}.")
                        skipped_tables.append(table_name)
                        skip = True
                if not skip:
                    # get table details from glue catalog
                    tbl_details = _get_table_details(glue, db, table)
                    if isinstance(tbl_details, dict):
                        if 'Error' in tbl_details:
                            # in case table doesn't exist in Glue catalog
                            new_tables.append(table_name)
                            move_to_next = True
                        else:
                            partition_keys = tbl_details['Table']['PartitionKeys']
                            fetched_loc = tbl_details['Table']['StorageDescriptor']['Location']
                            columns = tbl_details['Table']['StorageDescriptor']['Columns']
                            # run initial checks
                            catalog_validation = _intial_checks(tbl_details)
                            if catalog_validation:
                                print("Initial validation for catalog passed.")
                            else:
                                skipped_tables.append(table_name)
                                skip = True
                                print("Initial validation for catalog failed.")
                            # run partition column check
                            partition_validation = partition_col_check(data, partition_keys)
                            if partition_validation:
                                print(f"Partition Validation passed for {table_name}.")
                            else:
                                skipped_tables.append(table_name)
                                skip = True
                                print(f"Partition Validation failed for {table_name}.")
                    if not move_to_next and not skip:
                        # Fetch all the columns from HQL file:
                        hql_cols = re.findall(column_rgs, data, flags=re.IGNORECASE)
                        hql_col_dlist = [{'Name': col[0], 'Type': col[1]} for col in hql_cols]

                        # getting all the columns from glue catalog
                        catalog_col_list = columns + partition_keys

                        # Schema comparison logic ==>
                        new_df = pd.DataFrame(hql_col_dlist)
                        old_df = pd.DataFrame(catalog_col_list)

                        new_df["From"] = "new"
                        old_df["From"] = "old"

                        # includes added, deleted and data type changed columns
                        new_cols_df = pd.concat([new_df, old_df]).drop_duplicates(['Name', 'Type'], keep=False)

                        # getting columns with data type change
                        dtype_changes = new_cols_df[new_cols_df.duplicated(['Name'], keep='first')]['Name'].to_list()

                        # added and deleted columns
                        remaining_cols = new_cols_df[~new_cols_df['Name'].isin(dtype_changes)]

                        # new columns
                        added_cols_dlist = remaining_cols[remaining_cols['From'] == 'new'][['Name', 'Type']].to_dict(
                            'records')
                        # deleted columns
                        del_cols_dlist = remaining_cols[remaining_cols['From'] == 'old'][['Name', 'Type']].to_dict(
                            'records')

                        # print(remaining_cols)
                        print("Newly Added columns ==>", added_cols_dlist)
                        print("Removed columns ===>", del_cols_dlist)

                        # TODO: print columns with data type changes
                        if dtype_changes:
                            print("data type changes records for: ", dtype_changes)
                            print(f"Skipping schema update for {table_name}")
                            skipped_tables.append(table_name)
                            skip = True
                            # check for data_type changed compatibility
                            # print("Checking if data_type changed is compatible...")
                            # TODO: implement logic for data_Type compatibility.
                        # Create ALTER statements => TEST it via EMR first.
                        if not skip:
                            if added_cols_dlist or del_cols_dlist:
                                _update_table_schema(glue, table=tbl_details,
                                                     new_cols=added_cols_dlist,
                                                     del_cols=del_cols_dlist)
                            else:
                                print(f"Update is not required for `{table_name}`trs")
                        else:
                            print(f"skipping schema update for table: "
                                  f"{table_name} due to data type change detected for columns.")
                    else:
                        if move_to_next:
                            print(f"{table_name} doesn't exist in the system.")
                        if skip:
                            print(f"Initial Validation failed or Change in partition column detected for {table_name}")
                else:
                    print(f"skipping schema update for table: {table_name} due to initial validation failure")
        # TODO: make these usable somehow for next step instead of just printing ?? can be integrated with SNS if needed
        print("skipped tables: ", skipped_tables)
        print("new tables:", new_tables) # can be used for creating new tables directly
    except Exception as e:
        raise e
