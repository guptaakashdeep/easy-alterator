"""Main class for Alterator, Sync and Validator"""
import re
import os
from rules import rule_book as rbook
from utils import helper as hfunc
from utils import glue_utils as glue
from utils import file_utils as futils
from utils import s3_utils as s3utils


def sync_tables(src, tgt, part_check, validate):
    """_summary_

    Args:
        src (_type_): _description_
        tgt (_type_): _description_
        part_check (_type_): _description_
        validate (_type_): _description_

    Raises:
        Exception: _description_
    """
    print("##### SYNC TABLE PROCESS #####")
    src_db, src_tbl = src.split(".")
    tgt_db, tgt_tbl = tgt.split(".")
    print(f"=> src details >> \n database: {src_db} \n table: {src_tbl}")
    print(f"=> tgt details >> \n database: {tgt_db} \n table: {tgt_tbl}")
    src_tbl_details = glue.get_table_details(src_db, src_tbl)
    tgt_tbl_details = glue.get_table_details(tgt_db, tgt_tbl)
    if isinstance(src_tbl_details, dict):
        if "Error" in src_tbl_details:
            print(
                "Error occured while fetching src schema: ",
                src_tbl_details["Error"],
            )
            raise Exception(src_tbl_details["Error"])
    if isinstance(tgt_tbl_details, dict):
        if "Error" in tgt_tbl_details:
            print(
                "Error occured while fetching tgt schema: ",
                tgt_tbl_details["Error"],
            )
            raise Exception(tgt_tbl_details["Error"])
    # running initial validations
    src_validation = hfunc.intial_checks(src_tbl_details)
    tgt_validation = hfunc.intial_checks(tgt_tbl_details)
    if src_validation and tgt_validation:
        print("=> Initial Validation Passed")
        # compare partition columns
        tgt_part_cols = tgt_tbl_details["Table"]["PartitionKeys"]
        src_part_cols = src_tbl_details["Table"]["PartitionKeys"]
        tgt_cols = (
            tgt_part_cols + tgt_tbl_details["Table"]["StorageDescriptor"]["Columns"]
        )
        src_cols = (
            src_part_cols + src_tbl_details["Table"]["StorageDescriptor"]["Columns"]
        )
        if part_check:
            print("=> Partition Column check is enabled.")
            result = rbook.partition_col_check(tgt_part_cols, src_part_cols)
            if not result:
                raise Exception("Partition column check failed.")
            else:
                print("=> Parition Column check passed.")
        # Get columns that needs to be added or removed in tgt table as per src table to sync schema
        new_cols, removed_cols, combined_df = hfunc.compare_schema(src_cols, tgt_cols)
        print(combined_df)
        if not combined_df.empty:
            print(f"****Validating data type compatibility for {tgt}****")
            response = rbook.check_dtype_compatibility(combined_df)
            if not response:
                update_table = False
                raise Exception(f"Data type Validation failed for {tgt}")
            else:
                update_table = True
                print(f"=> Data type Validation passed for {tgt}")
        else:
            update_table = True
        # if all the checks are passed update the table if validation = False
        if update_table:
            if not validate:
                if new_cols or removed_cols:
                    glue.update_table_schema(tgt_tbl_details, new_cols, removed_cols)
                else:
                    print(f"=> nothing to update for {tgt}")
            else:
                print("=> Validation completed. <=")
    else:
        raise Exception("Initial Validation Failed.")
    print("##### SYNC TABLE PROCESS COMPLETED #####")


def alterator(**kwargs):
    paths = kwargs.get("paths")
    ddl_config_path = kwargs.get("ddl_config_path")
    path_key = kwargs.get("path_key")
    ddl_file_prefix = kwargs.get("ddl_file_prefix")
    ddl_file_suffix = kwargs.get("ddl_file_suffix")
    validate = kwargs.get("validate")

    hql_paths = []
    config = None
    # check if the paths provided are valid:
    if paths:
        futils.check_paths(paths)
        hql_paths = paths
    if ddl_config_path:
        futils.check_paths(ddl_config_path)
        # Added support for reading from S3 file.
        if os.path.isfile(ddl_config_path) or ddl_config_path.startswith("s3://"):
            if ddl_config_path.endswith(".yaml"):
                config = futils.read_yaml(ddl_config_path)
                if path_key in config:
                    futils.check_paths(config[path_key])
                    hql_paths.append(config[path_key])
                else:
                    if not paths:
                        raise Exception(
                            f"Provided key_for_path is not available in {ddl_config_path} configuration file"
                        )
            else:
                raise Exception("Only .yaml configuration files are supported.")
        else:
            raise Exception("Please provide configuration file path with filename.")

    print("=> DDL paths:", hql_paths)

    # Extract files from path as per the suffix and prefix provided.
    if config:
        final_file_list = futils.filter_files(
            hql_paths, ddl_file_prefix, ddl_file_suffix, table_names=config["tables"]
        )
    else:
        final_file_list = futils.filter_files(
            hql_paths, ddl_file_prefix, ddl_file_suffix
        )

    table_rgx = r"""TABLE [IF NOT EXISTS]*\s*`(\w+)[\.](\w+)`"""
    column_rgs = r"""`(\w+)`\s+(\w+(\(\d+,\d+\))?),*"""
    skipped_tables = []
    new_tables = []

    # Fetching AWS ID from EMR
    aws_account_id = hfunc.get_account_id()
    try:
        # loop over DDL files:
        for fname in final_file_list:
            skip = False
            move_to_next = False
            print(f"###### Process started for {fname} ######")
            if fname.startswith("s3://"):
                file_content = s3utils.read_s3_file(fname)
                data = (
                    file_content.lower().strip().format(aws_account_id=aws_account_id)
                )
            else:
                with open(fname, "r", encoding="utf-8") as filestream:
                    data = (
                        filestream.read().lower().strip().format(aws_account_id=aws_account_id)
                    )
            if data:
                table_match = re.search(table_rgx, data, flags=re.IGNORECASE)
                if table_match:
                    db, table = table_match.groups()
                    table_name = f"{db}.{table}"
                else:
                    print(f"==> Please validate the DDL format for {fname}")
                    skip = True
                if not skip:
                    # Check if hql is create statement.
                    if not data.startswith("create"):
                        print(
                            f"==> HQL provided for {table_name} is not a create statement."
                        )
                        skipped_tables.append(table_name)
                        skip = True
                    else:
                        # run initial checks on HQL
                        print("*** Running initial validation.***")
                        validation_results = hfunc.intial_checks(data)
                        if validation_results:
                            print(
                                f"=> Initial validations are successful for {table_name}."
                            )
                        else:
                            print(
                                f"==> Initial validation failed for provided HQL {table_name}."
                            )
                            skipped_tables.append(table_name)
                            skip = True
                    if not skip:
                        # get table details from glue catalog
                        tbl_details = glue.get_table_details(db, table)
                        if isinstance(tbl_details, dict):
                            if "Error" in tbl_details:
                                # in case table doesn't exist in Glue catalog
                                new_tables.append(table_name)
                                move_to_next = True
                            else:
                                partition_keys = tbl_details["Table"]["PartitionKeys"]
                                fetched_loc = tbl_details["Table"]["StorageDescriptor"][
                                    "Location"
                                ]
                                columns = tbl_details["Table"]["StorageDescriptor"][
                                    "Columns"
                                ]
                                # run initial checks
                                catalog_validation = hfunc.intial_checks(tbl_details)
                                if catalog_validation:
                                    print("=> Initial validation for catalog passed.")
                                else:
                                    skipped_tables.append(table_name)
                                    skip = True
                                    print("==> Initial validation for catalog failed.")
                                # run partition column check
                                partition_validation = rbook.partition_col_check(
                                    data, partition_keys
                                )
                                if partition_validation:
                                    print(
                                        f"=> Partition Validation passed for {table_name}."
                                    )
                                else:
                                    skipped_tables.append(table_name)
                                    skip = True
                                    print(
                                        f"==> Partition Validation failed for {table_name}."
                                    )
                        if not move_to_next and not skip:
                            # Fetch all the columns from HQL file:
                            hql_cols = re.findall(column_rgs, data, flags=re.IGNORECASE)
                            hql_col_dlist = [
                                {"Name": col[0], "Type": col[1]} for col in hql_cols
                            ]

                            # getting all the columns from glue catalog
                            catalog_col_list = columns + partition_keys
                            (
                                added_cols_dlist,
                                del_cols_dlist,
                                merged_df,
                            ) = hfunc.compare_schema(hql_col_dlist, catalog_col_list)
                            if not merged_df.empty:
                                print(
                                    f"****Validating data type compatibility for {table_name}****"
                                )
                                response = rbook.check_dtype_compatibility(merged_df)
                                if not response:
                                    print(
                                        f"==> Skipping schema update for {table_name}"
                                    )
                                    skipped_tables.append(table_name)
                                    skip = True
                            # Create ALTER statements => TEST it via EMR first.
                            if not skip:
                                if added_cols_dlist or del_cols_dlist:
                                    if not validate:
                                        glue.update_table_schema(
                                            table=tbl_details,
                                            new_cols=added_cols_dlist,
                                            del_cols=del_cols_dlist,
                                        )
                                    else:
                                        print(
                                            "=> Table will be updated with the identified changes."
                                        )
                                else:
                                    print(
                                        f"=> Update is not required for `{table_name}`"
                                    )
                            else:
                                if not validate:
                                    print(
                                        f"==> skipping schema update for table: {table_name}"
                                    )
                                else:
                                    print(
                                        f"==> schema update for table: {table_name} will be skipped."
                                    )
                        else:
                            if move_to_next:
                                print(f"==> {table_name} doesn't exist in the system.")
                            if skip:
                                print(
                                    f"==> Initial Validation failed or Change in partition column detected for {table_name}"
                                )
                    else:
                        print(
                            f"==> skipping schema update for table: {table_name} due to initial validation failure"
                        )
                else:
                    print(
                        "==> Skipping schema update for table due to incorrect DDL Format in: ",
                        fname,
                    )
            print(f"###### Process finished for {fname} ######")
        # TODO: make these usable somehow for next step instead of just printing ?? can be integrated with SNS if needed
        print("skipped tables: ", skipped_tables)
        print("new tables:", new_tables)  # can be used for creating new tables directly
    except Exception as ex:
        raise ex
