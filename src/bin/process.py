"""Main class for Alterator, Sync and Validator"""
import re
import os
from rules import rule_book as rbook
from utils import helper as hfunc
from utils import glue_utils as glue
from utils import file_utils as futils
from utils import s3_utils as s3utils
import logging


logger_sync = logging.getLogger("EA.process.sync")
logger_alt = logging.getLogger("EA.process.alterator")


def sync_tables(src, tgt, **kwargs):
    """Main SYNC functionality method for syncing the
    target table schema with source table schema.
    Args:
        src (str): Source table name with database
        tgt (str): Target table name with database.
        **kwargs : optional Arguments:
            validate (bool): Flag to validate the sync process.
            part_check (int): Flag to check partition columns.
            force (bool): Flag to IGNORE data type validation.
    Raises:
        Exception: Generic exception in case of failures
    """
    logger_sync.info("##### SYNC TABLE PROCESS #####")
    validate = kwargs.get("validate", False)
    part_check = kwargs.get("part_check", 1)
    force_upd = kwargs.get("force", False)
    if force_upd:
        logger_sync.warning(
            "WARN: FORCE update is enabled. Data type validation will be IGNORED."
        )
    src_db, src_tbl = src.split(".")
    tgt_db, tgt_tbl = tgt.split(".")
    logger_sync.info(f"=> src details >> \n database: {src_db} \n table: {src_tbl}")
    logger_sync.info(f"=> tgt details >> \n database: {tgt_db} \n table: {tgt_tbl}")
    src_tbl_details = glue.get_table_details(src_db, src_tbl)
    tgt_tbl_details = glue.get_table_details(tgt_db, tgt_tbl)
    if isinstance(src_tbl_details, dict):
        if "Error" in src_tbl_details:
            logger_sync.error(
                f'Error occured while fetching src schema: {src_tbl_details["Error"]}'
            )
            raise Exception(src_tbl_details["Error"])
    if isinstance(tgt_tbl_details, dict):
        if "Error" in tgt_tbl_details:
            logger_sync.error(
                f'Error occured while fetching tgt schema: {tgt_tbl_details["Error"]}'
            )
            raise Exception(tgt_tbl_details["Error"])
    # running initial validations
    _, src_validation = hfunc.intial_checks(src_tbl_details)
    _, tgt_validation = hfunc.intial_checks(tgt_tbl_details)
    if src_validation and tgt_validation:
        logger_sync.info("=> Initial Validation Passed")
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
            logger_sync.info("=> Partition Column check is enabled.")
            result = rbook.partition_col_check(tgt_part_cols, src_part_cols)
            if not result:
                raise Exception("Partition column check failed.")
            else:
                logger_sync.info("=> Parition Column check passed.")
        # Get columns that needs to be added or removed in tgt table as per src table to sync schema
        new_cols, removed_cols, combined_df = hfunc.compare_schema(src_cols, tgt_cols)
        logger_sync.debug(combined_df)
        if not combined_df.empty and not force_upd:
            logger_sync.info(f"****Validating data type compatibility for {tgt}****")
            response, _, _ = rbook.check_dtype_compatibility(combined_df)
            if not response:
                update_table = False
                logger_sync.info.critical(f"Data type Validation failed for {tgt}")
                raise Exception(f"Data type Validation failed for {tgt}")
            else:
                update_table = True
                logger_sync.info(f"=> Data type Validation passed for {tgt}")
        else:
            update_table = True
        # if all the checks are passed update the table if validation = False
        if update_table:
            if not validate:
                if new_cols or removed_cols:
                    status, table, error_dict = glue.update_table_schema(
                        tgt_tbl_details, new_cols, removed_cols
                    )
                    if not status:
                        raise Exception(
                            f"""Schema update failed for {table}
                                        due to {error_dict['Code']}: {error_dict['Message']}"""
                        )
                else:
                    logger_sync.info(f"=> nothing to update for {tgt}")
            else:
                logger_sync.info("=> Validation completed. <=")
    else:
        raise Exception("Initial Validation Failed.")
    logger_sync.info("##### SYNC TABLE PROCESS COMPLETED #####")


def alterator(**kwargs) -> dict:
    """
    Main ALTERATOR functionality method for altering the table schema.
    This function is used to alter the table schema.
    It will extract the DDL files from the provided paths,
    compares the schema with already existing table and
    alter the table schema as per the DDL files.

    Parameters
    ----------
    paths : list
        List of paths where DDL files are present.
    ddl_config_path : str
        Path to the configuration file.
    path_key : str
        Key for the path in the configuration file.
    ddl_file_prefix : str
        Prefix for the DDL files.
    ddl_file_suffix : str
        Suffix for the DDL files.
    validate : bool
        dry runs the process and returns the result that will be made when the process is run.
    force : bool
        Flag to force the update of the table schema. IGNORES the data type compatibility validation.

    Returns
    -------
    response : dict
        Response from the Alterator process.
    """
    paths = kwargs.get("paths")
    ddl_config_path = kwargs.get("ddl_config_path")
    path_key = kwargs.get("path_key")
    ddl_file_prefix = kwargs.get("ddl_file_prefix")
    ddl_file_suffix = kwargs.get("ddl_file_suffix")
    validate = kwargs.get("validate")
    force = kwargs.get("force")

    hql_paths = []
    config = {}
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

    logger_alt.info(f"=> DDL paths: {hql_paths}")

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
    success_tables = []
    errored_tables = []
    identical_tables = []

    # Fetching AWS account id
    aws_account_id = hfunc.get_account_id()
    try:
        # loop over DDL files:
        for fname in final_file_list:
            skip = False
            move_to_next = False
            logger_alt.info(f"###### Process started for {fname} ######")
            if fname.startswith("s3://"):
                file_content = s3utils.read_s3_file(fname)
                data = (
                    file_content.lower().strip().format(aws_account_id=aws_account_id)
                )
            else:
                with open(fname, "r", encoding="utf-8") as filestream:
                    data = (
                        filestream.read()
                        .lower()
                        .strip()
                        .format(aws_account_id=aws_account_id)
                    )
            if data:
                table_match = re.search(table_rgx, data, flags=re.IGNORECASE)
                if table_match:
                    db, table = table_match.groups()
                    table_name = f"{db}.{table}"
                else:
                    logger_alt.error(f"==> Please validate the DDL format for {fname}")
                    skipped_tables.append(
                        {
                            "table_name": "",
                            "filename": fname,
                            "reason": "IncorrectSQLFormat",
                        }
                    )
                    skip = True
                if not skip:
                    # Check if hql is create statement.
                    if not data.startswith("create"):
                        logger_alt.error(
                            f"==> HQL provided for {table_name} is not a create statement."
                        )
                        # TODO: IncorrectSQLFormat
                        skipped_tables.append(
                            {
                                "table_name": table_name,
                                "filename": fname,
                                "reason": "NonCreateSQL",
                            }
                        )
                        skip = True
                    else:
                        # run initial checks on HQL
                        logger_alt.info("*** Running initial validation.***")
                        validation_type, validation_results = hfunc.intial_checks(data)
                        if validation_results:
                            logger_alt.info(
                                f"=> Initial validations are successful for {table_name}."
                            )
                        else:
                            logger_alt.error(
                                f"==> Initial validation: {validation_type} failed for provided HQL {table_name}."
                            )
                            # TODO: ValidationError
                            skipped_tables.append(
                                {
                                    "table_name": table_name,
                                    "reason": "ValidationError",
                                    "type": validation_type,
                                    "from": "HQL",
                                }
                            )
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
                                (
                                    catalog_validation_type,
                                    catalog_validation,
                                ) = hfunc.intial_checks(tbl_details)
                                if catalog_validation:
                                    logger_alt.info(
                                        "=> Initial validation for catalog passed."
                                    )
                                else:
                                    # TODO: ValidationError
                                    skipped_tables.append(
                                        {
                                            "table_name": table_name,
                                            "reason": "ValidationError",
                                            "type": catalog_validation_type,
                                            "from": "CATALOG",
                                        }
                                    )
                                    skip = True
                                    logger_alt.error(
                                        "==> Initial validation for catalog failed."
                                    )
                                # run partition column check
                                partition_validation = rbook.partition_col_check(
                                    data, partition_keys
                                )
                                if partition_validation:
                                    logger_alt.info(
                                        f"=> Partition Validation passed for {table_name}."
                                    )
                                else:
                                    # TODO: PartitionValidationError
                                    skipped_tables.append(
                                        {
                                            "table_name": table_name,
                                            "reason": "PartitionValidationError",
                                        }
                                    )
                                    skip = True
                                    logger_alt.error(
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
                                logger_alt.info(
                                    f"****Validating data type compatibility for {table_name}****"
                                )
                                (
                                    response,
                                    compatible,
                                    incompatible,
                                ) = rbook.check_dtype_compatibility(merged_df)
                                if not response:
                                    if force:
                                        logger_alt.warning(
                                            "FORCE flag is enabled. Table will be updated with incompatible data type changes."
                                        )
                                        new_dtype_cols = (
                                            merged_df[["Name", "Type_new"]]
                                            .rename(columns={"Type_new": "Type"})
                                            .to_dict("records")
                                        )
                                        old_dtype_cols = (
                                            merged_df[["Name", "Type_old"]]
                                            .rename(columns={"Type_old": "Type"})
                                            .to_dict("records")
                                        )
                                        added_cols_dlist = (
                                            added_cols_dlist + new_dtype_cols
                                        )
                                        del_cols_dlist = del_cols_dlist + old_dtype_cols
                                    else:
                                        logger_alt.info(
                                            f"==> Skipping schema update for {table_name}"
                                        )
                                        # TODO: Add details dict.
                                        compatible_cols = (
                                            compatible[["Name", "Type_old", "Type_new"]]
                                            .rename(
                                                columns={
                                                    "Type_old": "Type",
                                                    "Type_new": "updated_type",
                                                }
                                            )
                                            .to_dict("records")
                                        )
                                        incompatible_cols = (
                                            incompatible[
                                                ["Name", "Type_old", "Type_new"]
                                            ]
                                            .rename(
                                                columns={
                                                    "Type_old": "Type",
                                                    "Type_new": "updated_type",
                                                }
                                            )
                                            .to_dict("records")
                                        )
                                        skipped_tables.append(
                                            {
                                                "table_name": table_name,
                                                "reason": "IncompatibleDataTypeError",
                                                "details": {
                                                    "compatible": compatible_cols,
                                                    "incompatible": incompatible_cols,
                                                    "add": added_cols_dlist,
                                                    "delete": del_cols_dlist,
                                                },
                                            }
                                        )
                                        skip = True
                                else:  # get the compatible data type changes if any
                                    if not compatible.empty:
                                        logger_alt.info(
                                            "Getting compatible datatype columns."
                                        )
                                        new_dtype_cols = (
                                            compatible[["Name", "Type_new"]]
                                            .rename(columns={"Type_new": "Type"})
                                            .to_dict("records")
                                        )
                                        old_dtype_cols = (
                                            compatible[["Name", "Type_old"]]
                                            .rename(columns={"Type_old": "Type"})
                                            .to_dict("records")
                                        )
                                        added_cols_dlist = (
                                            added_cols_dlist + new_dtype_cols
                                        )
                                        del_cols_dlist = del_cols_dlist + old_dtype_cols
                            # TODO: incase of force update implement added_cols_list.append(new cols) and del_cols_dlist.append(old cols).
                            if not skip:
                                if added_cols_dlist or del_cols_dlist:
                                    if not validate:
                                        previous_ver = glue.get_latest_table_version(
                                            db, table
                                        )
                                        status, _, error = glue.update_table_schema(
                                            table=tbl_details,
                                            new_cols=added_cols_dlist,
                                            del_cols=del_cols_dlist,
                                        )
                                        updated_ver = glue.get_latest_table_version(
                                            db, table
                                        )
                                        success_response = {
                                            "table_name": table_name,
                                            "previous_version": previous_ver,
                                            "current_version": updated_ver,
                                            "details": {
                                                "add": added_cols_dlist,
                                                "delete": del_cols_dlist,
                                            },
                                        }
                                        if not status:
                                            logger_alt.error(
                                                f"==> Exception occurred while updating table schema for {table_name}."
                                            )
                                            logger_alt.error(
                                                f"Exception details: {error['Code']} - {error['Message']}"
                                            )
                                            errored_tables.append(table_name)
                                        else:
                                            success_tables.append(success_response)
                                    else:
                                        logger_alt.info(
                                            "=> Table will be updated with the identified changes."
                                        )
                                        current_ver = glue.get_latest_table_version(
                                            db, table
                                        )
                                        success_response = {
                                            "table_name": table_name,
                                            "previous_version": current_ver,
                                            "current_version": current_ver,
                                            "details": {
                                                "add": added_cols_dlist,
                                                "delete": del_cols_dlist,
                                            },
                                        }
                                        success_tables.append(success_response)
                                else:
                                    identical_tables.append(table_name)
                                    logger_alt.info(
                                        f"=> Update is not required for `{table_name}`"
                                    )
                            else:
                                if not validate:
                                    logger_alt.info(
                                        f"==> skipping schema update for table: {table_name}"
                                    )
                                else:
                                    logger_alt.warning(
                                        f"==> schema update for table: {table_name} will be skipped."
                                    )
                        else:
                            if move_to_next:
                                logger_alt.error(
                                    f"==> {table_name} doesn't exist in the system."
                                )
                            if skip:
                                logger_alt.error(
                                    f"==> Initial Validation failed or Change in partition column detected for {table_name}"
                                )
                    else:
                        logger_alt.warning(
                            f"==> skipping schema update for table: {table_name} due to initial validation failure"
                        )
                else:
                    logger_alt.warning(
                        f"==> Skipping schema update for table due to incorrect DDL Format in: {fname}",
                    )
            logger_alt.info(f"###### Process finished for {fname} ######")
        # can be integrated with SNS if needed
        logger_alt.debug(f"skipped tables: {skipped_tables}")
        logger_alt.debug(
            f"new tables: {new_tables}"
        )  # can be used for creating new tables directly
        alterator_response = {
            "ResponseMetadata": {
                "validation": str(validate),
                "force": str(force),
                "stats": {
                    "num_tables_analyzed": len(skipped_tables)
                    + len(new_tables)
                    + len(errored_tables)
                    + len(success_tables)
                    + len(identical_tables),
                    "num_updates": len(success_tables),
                    "num_skipped": len(skipped_tables),
                    "num_new": len(new_tables),
                    "num_errored": len(errored_tables),
                    "num_identical": len(identical_tables),
                },
            },
            "skipped_tables": skipped_tables,
            "new_tables": new_tables,
            "success_tables": success_tables,
            "errored_tables": errored_tables,
            "identical_tables": identical_tables,
        }
        return alterator_response
    except Exception as ex:
        logger_alt.error(ex)
        raise ex
