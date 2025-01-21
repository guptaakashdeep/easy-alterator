"""Main class for Alterator"""
import re
import os
from rules import rule_book as rbook
from utils import helper as hfunc
from utils import glue_utils as glue
from utils import file_utils as futils
from utils import s3_utils as s3utils
import logging


class Alterator:
    """
    Alterator class for managing and altering table schemas based on provided configurations and validations.

    Attributes:
        paths (list): List of paths to HQL files.
        path_key (str): Key for the HQL file path in the configuration.
        ddl_config_path (str): Path to the DDL configuration file.
        ddl_file_prefix (str): Prefix for DDL files.
        ddl_file_suffix (str): Suffix for DDL files.
        validate (bool): Flag to enable validation mode.
        force (bool): Flag to force schema updates despite incompatible changes.
        config (dict): Configuration dictionary read from the DDL configuration file.
        logger (Logger): Logger instance for logging messages.
        table_rgx (str): Regular expression for extracting table names from HQL files.
        column_rgs (str): Regular expression for extracting column definitions from HQL files.
        hql_paths (list): List of validated HQL file paths.
        skipped_tables (list): List of tables that were skipped during processing.
        new_tables (list): List of new tables identified during processing.
        success_tables (list): List of tables that were successfully updated.
        errored_tables (list): List of tables that encountered errors during processing.
        identical_tables (list): List of tables that were found to be identical.
        aws_account_id (str): AWS account ID.

    Methods:
        _initialize_paths(): Initializes the HQL paths based on the provided paths and configuration.
        _filter_files(): Filters the HQL files based on the provided configuration.
        _read_file_content(fname): Reads the content of a file and processes it.
        _extract_table_name(data, fname): Extracts the table name from the provided data using a regular expression.
        _validate_create_statement(data, table_name, fname): Validates if the provided HQL statement is a CREATE statement.
        _run_initial_validation(data, table_name): Runs initial validation checks on the provided data.
        _fetch_table_details(db, table): Fetches the details of a specified table from the AWS Glue Catalog.
        _validate_catalog(tbl_details, table_name): Runs initial validation on the schema present in AWS Glue Catalog.
        _validate_partition_columns(data, partition_keys, table_name): Validates the partition columns of the given HQL against the partition keys present in AWS Glue Catalog.
        _compare_schemas(data, columns, partition_keys): Compares the schemas between the provided data and catalog columns.
        _update_table_schema(db, table, tbl_details, added_cols_dlist, del_cols_dlist, table_name): Updates the schema of a specified table in the database.
        alter_schema(): Alters the schema of tables based on the provided configurations and validations.
        get_results(): Generates a dictionary containing the results of the table analysis.
    """
    def __init__(self, paths, path_key, ddl_config_path, ddl_file_prefix, ddl_file_suffix, validate, force):
        self.paths = paths
        self.path_key = path_key
        self.ddl_config_path = ddl_config_path
        self.ddl_file_prefix = ddl_file_prefix
        self.ddl_file_suffix = ddl_file_suffix
        self.config = None
        self.logger = logging.getLogger("EA.process.alterator")
        self.table_rgx = r"""TABLE [IF NOT EXISTS]*\s*`(\w+)[\.](\w+)`"""
        self.column_rgs = r"""`(\w+)`\s+(\w+((\(\d+,\d+\))|(\(\d+\)))?),*"""
        self.validate = validate
        self.force = force
        self.hql_paths = []
        self.skipped_tables = []
        self.new_tables = []
        self.success_tables = []
        self.errored_tables = []
        self.identical_tables = []
        self.non_parquet_tables = []
        self.iceberg_tables = []
        self.aws_account_id = hfunc.get_account_id()


    def _initialize_paths(self):
        """
        Initializes the HQL paths based on the provided paths and configuration.

        If paths are provided, it validates and sets them. If a DDL configuration path is provided,
        it reads the configuration and appends the paths specified in the configuration.

        Raises:
            Exception: If the provided configuration file is not valid or the key for path is not available.
        """
        if self.paths:
            futils.check_paths(self.paths)
            self.hql_paths = self.paths
        # Checks if the DDL Configuration file path exists or not
        if self.ddl_config_path:
            futils.check_paths(self.ddl_config_path)
            if self.ddl_config_path.endswith(".yaml") and (os.path.isfile(self.ddl_config_path) or self.ddl_config_path.startswith("s3://")):
                # Read the configuration YAML file
                self.config = futils.read_yaml(self.ddl_config_path)
                # Check if the HQL File path key exists.
                if self.path_key in self.config:
                    # Check if the path is correct
                    futils.check_paths(self.config[self.path_key])
                    self.hql_paths.append(self.config[self.path_key])
                else:
                    if not self.paths:
                        raise Exception(f"Provided key_for_path is not available in {self.ddl_config_path} configuration file")
            else:
                raise Exception("Only .yaml configuration files are supported or invalid file path.")

    def _filter_files(self):
        """
        Filters the HQL files based on the provided configuration.

        If a configuration is provided, it filters the files based on the table names specified in the configuration.
        Otherwise, it filters the files based on the HQL paths, DDL file prefix, and DDL file suffix.

        Returns:
            list: A list of filtered files.
        """
        if self.config:
            return futils.filter_files(
                self.hql_paths, self.ddl_file_prefix, self.ddl_file_suffix, table_names=self.config["tables"]
            )
        else:
            return futils.filter_files(
                self.hql_paths, self.ddl_file_prefix, self.ddl_file_suffix
            )

    def _read_file_content(self, fname):
        """
        Reads the content of a file and processes it.

        If the file is located in an S3 bucket (indicated by the "s3://" prefix),
        it reads the file using the `s3utils.read_s3_file` function. Otherwise, it
        reads the file from the local filesystem.

        The content of the file is converted to lowercase, stripped of leading and
        trailing whitespace, and formatted with the `aws_account_id` attribute.

        Args:
            fname (str): The path to the file. Can be an S3 URI or a local file path.

        Returns:
            str: The processed content of the file.
        """
        if fname.startswith("s3://"):
            file_content = s3utils.read_s3_file(fname)
            return file_content.lower().strip().format(aws_account_id=self.aws_account_id)
        else:
            with open(fname, "r", encoding="utf-8") as filestream:
                return filestream.read().lower().strip().format(aws_account_id=self.aws_account_id)

    def _extract_table_name(self, data, fname):
        """
        Extracts the table name from the provided data using a regular expression.

        Args:
            data (str): The string data containing the DDL statement.
            fname (str): The filename from which the data was read, used for logging purposes.

        Returns:
            tuple: A tuple containing the extracted table name in the format "db.table" and a boolean flag.
                   The boolean flag is False if the table name was successfully extracted, and True if there was an error.
        """
        table_match = re.search(self.table_rgx, data, flags=re.IGNORECASE)
        if table_match:
            db, table = table_match.groups()
            return f"{db}.{table}", False
        else:
            self.logger.error("==> Please validate the DDL format for %s", fname)
            return "", True

    def _validate_create_statement(self, data, table_name, fname):
        """
        Validates if the provided HQL statement is a CREATE statement.

        Args:
            data (str): The HQL statement to validate.
            table_name (str): The name of the table associated with the HQL statement.
            fname (str): The filename where the HQL statement is located.

        Returns:
            tuple: A tuple containing a dictionary with error details and a boolean indicating validation failure.
                   If the statement is not a CREATE statement, the dictionary contains:
                   - "table_name": The name of the table.
                   - "filename": The filename where the HQL statement is located.
                   - "reason": The reason for validation failure ("NonCreateSQL").
                   The boolean is True if validation fails, otherwise False.
        """
        if not data.startswith("create"):
            self.logger.error("==> HQL provided for %s is not a create statement.", table_name)
            return {
                "table_name": table_name,
                "filename": fname,
                "reason": "NonCreateSQL",
            }, True
        return None, False

    def _run_initial_validation(self, data, table_name):
        """
        Runs initial validation checks on the provided data.

        Args:
            data (Any): The data to be validated.
            table_name (str): The name of the table associated with the data.

        Returns:
            tuple: A tuple containing:
                - dict or None: A dictionary with error details if validation fails, otherwise None.
                - bool: True if validation fails, otherwise False.
        """
        self.logger.info("*** Running initial validation.***")
        validation_type, validation_results = hfunc.initial_checks(data)
        if validation_results:
            self.logger.info("=> Initial validations are successful for %s.", table_name)
            return None, False
        else:
            self.logger.error("==> Initial validation: %s failed for provided HQL %s.", validation_type, table_name)
            return {
                "table_name": table_name,
                "reason": "ValidationError",
                "type": validation_type,
                "from": "HQL",
            }, True

    def _fetch_table_details(self, db, table):
        """
        Fetches the details of a specified table from the AWS Glue Catalog.

        Args:
            db (str): The name of the database.
            table (str): The name of the table.

        Returns:
            tuple: A tuple containing:
                - dict or None: The details of the table if found, otherwise None.
                - bool: True if there was an error fetching the table details, otherwise False.
        """
        tbl_details = glue.get_table_details(db, table)
        if isinstance(tbl_details, dict) and "Error" in tbl_details:
            return None, True
        return tbl_details, False

    def _validate_catalog(self, tbl_details, table_name):
        """
        Runs initial validation on the schema present in AWS Glue Catalog

        Args:
            tbl_details (dict): The details of the table to validate.
            table_name (str): The name of the table.

        Returns:
            tuple: A tuple containing:
                - dict or None: If validation fails, a dictionary with details of the failure. Otherwise, None.
                - bool: True if validation fails, False otherwise.
        """
        catalog_validation_type, catalog_validation = hfunc.initial_checks(tbl_details)
        if catalog_validation:
            self.logger.info("=> Initial validation for catalog passed.")
            return None, False
        else:
            self.logger.error("==> Initial validation for catalog failed.")
            return {
                "table_name": table_name,
                "reason": "ValidationError",
                "type": catalog_validation_type,
                "from": "CATALOG",
            }, True

    def _validate_partition_columns(self, data, partition_keys, table_name):
        """
        Validates the partition columns of the given hql against the partition keys
        present in AWS Glue Catalog.

        Args:
            data (DataFrame): The data to be validated.
            partition_keys (list): The list of partition keys to validate against.
            table_name (str): The name of the table being validated.

        Returns:
            tuple: A tuple containing:
                - dict or None: If validation fails, returns a dictionary with the table name and reason for failure.
                                If validation passes, returns None.
                - bool: A boolean indicating whether the validation failed (True) or passed (False).
        """
        partition_validation = rbook.partition_col_check(data, partition_keys)
        if partition_validation:
            self.logger.info("=> Partition Validation passed for %s.", table_name)
            return None, False
        else:
            self.logger.error("==> Partition Validation failed for %s.", table_name)
            return {
                "table_name": table_name,
                "reason": "PartitionValidationError",
            }, True

    def _compare_schemas(self, data, columns, partition_keys):
        """
        Compare the schemas between the provided data and catalog columns.
        HQL vs Glue Catalog Schema

        Args:
            data (str): The data containing the schema information.
            columns (list): A list of column dictionaries from the catalog.
            partition_keys (list): A list of partition key dictionaries from the catalog.

        Returns:
            tuple: A tuple containing:
                - added_cols_dlist (list): A list of dictionaries representing columns added in the schema.
                - del_cols_dlist (list): A list of dictionaries representing columns deleted from the schema.
                - merged_df (DataFrame): A DataFrame representing the merged schema comparison.
        """
        hql_cols = re.findall(self.column_rgs, data, flags=re.IGNORECASE)
        hql_col_dlist = [{"Name": col[0], "Type": col[1]} for col in hql_cols]
        catalog_col_list = columns + partition_keys
        added_cols_dlist, del_cols_dlist, merged_df = hfunc.compare_schema(hql_col_dlist, catalog_col_list)
        return added_cols_dlist, del_cols_dlist, merged_df

    def _update_table_schema(self, db, table, tbl_details, added_cols_dlist, del_cols_dlist, table_name):
        """
        Updates the schema of a specified table in the database.

        Parameters:
        db (str): The database name.
        table (str): The table name.
        tbl_details (dict): The details of the table schema.
        added_cols_dlist (list): List of columns to be added.
        del_cols_dlist (list): List of columns to be deleted.
        table_name (str): The name of the table to be updated.

        Returns:
        tuple: A tuple containing a dictionary with the update details and a boolean indicating success.
        The dictionary contains:
            - table_name (str): The name of the table.
            - previous_version (str): The previous version of the table schema.
            - current_version (str): The current version of the table schema.
            - details (dict): A dictionary with keys 'add' and 'delete' listing the columns added and deleted respectively.
        The boolean indicates whether the update was successful.
        """
        if not (added_cols_dlist or del_cols_dlist):
            self.logger.info("=> Update is not required for `%s`", table_name)
            return None, True

        if self.validate:
            self.logger.info("=> Table will be updated with the identified changes.")
            current_ver = glue.get_latest_table_version(db, table)
            return {
                "table_name": table_name,
                "previous_version": current_ver,
                "current_version": current_ver,
                "details": {
                    "add": added_cols_dlist,
                    "delete": del_cols_dlist,
                },
            }, True

        previous_ver = glue.get_latest_table_version(db, table)
        status, _, error = glue.update_table_schema(
            table=tbl_details,
            new_cols=added_cols_dlist,
            del_cols=del_cols_dlist,
        )
        updated_ver = glue.get_latest_table_version(db, table)
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
            self.logger.error("==> Exception occurred while updating table schema for %s.", table_name)
            self.logger.error("Exception details: %s - %s", error['Code'], error['Message'])
            return success_response, False

        return success_response, True

    def alter_schema(self):
        """
        Alters the schema of tables based on the provided configurations and validations.

        This method performs the following steps:
        1. Initializes paths.
        2. Filters the list of files to process.
        3. Iterates over each file and processes it:
            - Reads the file content.
            - Extracts the table name.
            - Validates the CREATE statement.
            - Runs initial validation on the data.
            - Fetches table details from the Glue Catalog.
            - Validates the catalog information.
            - Validates partition columns.
            - Compares schemas between the file and the Glue Catalog.
            - Validates data type compatibility.
            - Updates the table schema if necessary.

        The method handles various scenarios such as:
        - Skipping tables if the table name cannot be extracted.
        - Skipping tables if the CREATE statement is invalid.
        - Identifies new tables if they do not exist in the Glue Catalog.
        - Skipping schema updates if there are incompatible data type changes (unless forced).
        - Updates table schema if schema updates present in HQL file are valid.
        - Logging and appending tables to respective lists based on the outcome (success, identical, skipped, errored).

        Exceptions:
            Logs any exceptions that occur during the process.

        Returns:
            None
        """
        self._initialize_paths()
        final_file_list = self._filter_files()
        try:
            for fname in final_file_list:
                self.logger.info("###### Process started for %s ######", fname)
                data = self._read_file_content(fname)
                if not data:
                    continue
                table_name, skip = self._extract_table_name(data, fname)
                if skip:
                    self.skipped_tables.append(
                        {
                            "table_name": "",
                            "filename": fname,
                            "reason": "TableNameNotExtracted",
                        }
                    )
                    continue

                error, skip = self._validate_create_statement(data, table_name, fname)
                if skip:
                    self.skipped_tables.append(error)
                    continue

                error, skip = self._run_initial_validation(data, table_name)
                db, table = table_name.split('.')
                # TODO: Update here to identify Text, Iceberg and new tables.
                # TODO: add code here to see which validation is actually failed.
                # and assign it to actual list of tables.
                # If possible identify the format change tables also here.
                if skip:
                    if error['type'] == "PARQUET_CHECK":
                        skip_tbl_details = glue.get_table_details(db, table)
                        if isinstance(skip_tbl_details, dict):
                            if "Error" in skip_tbl_details:
                                self.new_tables.append(table_name)
                            else:
                                # TODO: Check here for ICEBERG table type.
                                self.non_parquet_tables.append(table_name)
                    else:
                        # TODO: Check here where these should go
                        self.skipped_tables.append(error)
                    continue

                # db, table = table_name.split('.')
                tbl_details, error = self._fetch_table_details(db, table)
                if error:
                    self.new_tables.append(table_name)
                    continue

                error, skip = self._validate_catalog(tbl_details, table_name)
                if skip:
                    self.skipped_tables.append(error)
                    continue

                partition_keys = tbl_details["Table"]["PartitionKeys"]
                error, skip = self._validate_partition_columns(data, partition_keys, table_name)
                if skip:
                    self.skipped_tables.append(error)
                    continue

                # Schema comparison HQL vs GlueCatalog
                columns = tbl_details["Table"]["StorageDescriptor"]["Columns"]
                added_cols_dlist, del_cols_dlist, merged_df = self._compare_schemas(data, columns, partition_keys)
                # Data type for column changed
                if not merged_df.empty:
                    self.logger.info("****Validating data type compatibility for %s****", table_name)
                    response, compatible, incompatible = rbook.check_dtype_compatibility(merged_df)
                    # Incompatible data type change detected
                    if not response:
                        if self.force:
                            self.logger.warning("FORCE flag is enabled. Table will be updated with incompatible data type changes.")
                            new_dtype_cols = merged_df[["Name", "Type_new"]].rename(columns={"Type_new": "Type"}).to_dict("records")
                            old_dtype_cols = merged_df[["Name", "Type_old"]].rename(columns={"Type_old": "Type"}).to_dict("records")
                            added_cols_dlist += new_dtype_cols
                            del_cols_dlist += old_dtype_cols
                        else:
                            self.logger.info("==> Skipping schema update for %s", table_name)
                            compatible_cols = compatible[["Name", "Type_old", "Type_new"]].rename(columns={"Type_old": "Type", "Type_new": "updated_type"}).to_dict("records")
                            incompatible_cols = incompatible[["Name", "Type_old", "Type_new"]].rename(columns={"Type_old": "Type", "Type_new": "updated_type"}).to_dict("records")
                            self.skipped_tables.append({
                                "table_name": table_name,
                                "reason": "IncompatibleDataTypeError",
                                "details": {
                                    "compatible": compatible_cols,
                                    "incompatible": incompatible_cols,
                                    "add": added_cols_dlist,
                                    "delete": del_cols_dlist,
                                },
                            })
                            continue
                    else:
                        if not compatible.empty:
                            self.logger.info("Getting compatible datatype columns.")
                            new_dtype_cols = compatible[["Name", "Type_new"]].rename(columns={"Type_new": "Type"}).to_dict("records")
                            old_dtype_cols = compatible[["Name", "Type_old"]].rename(columns={"Type_old": "Type"}).to_dict("records")
                            added_cols_dlist += new_dtype_cols
                            del_cols_dlist += old_dtype_cols

                success_response, status = self._update_table_schema(db, table, tbl_details, added_cols_dlist, del_cols_dlist, table_name)
                if status:
                    if success_response:
                        self.success_tables.append(success_response)
                    else:
                        self.identical_tables.append(table_name)
                else:
                    self.errored_tables.append(table_name)

        except Exception as e:
            self.logger.error("An error occurred: %s", e)

    def get_results(self):
        """
        Generates a dictionary containing the results of the table analysis.

        Returns:
            dict: A dictionary with the following structure:
                - "ResponseMetadata" (dict): Metadata about the response.
                    - "validation" (str): The validation status.
                    - "force" (str): The force status.
                    - "stats" (dict): Statistics about the table analysis.
                        - "num_tables_analyzed" (int): Total number of tables analyzed.
                        - "num_updates" (int): Number of tables successfully updated.
                        - "num_skipped" (int): Number of tables skipped.
                        - "num_new" (int): Number of new tables.
                        - "num_errored" (int): Number of tables that encountered errors.
                        - "num_identical" (int): Number of tables that are identical.
                - "skipped_tables" (list): List of skipped tables.
                - "new_tables" (list): List of new tables.
                - "success_tables" (list): List of successfully updated tables.
                - "errored_tables" (list): List of tables that encountered errors.
                - "identical_tables" (list): List of identical tables.
        """
        # return response and write response to S3.
        alterator_response = {
            "ResponseMetadata": {
                "validation": str(self.validate),
                "force": str(self.force),
                "stats": {
                    "num_tables_analyzed": len(self.skipped_tables)
                    + len(self.new_tables)
                    + len(self.errored_tables)
                    + len(self.success_tables)
                    + len(self.identical_tables)
                    + len(self.non_parquet_tables)
                    + len(self.iceberg_tables),
                    "num_updates": len(self.success_tables),
                    "num_skipped": len(self.skipped_tables),
                    "num_new": len(self.new_tables),
                    "num_errored": len(self.errored_tables),
                    "num_identical": len(self.identical_tables),
                },
            },
            "skipped_tables": self.skipped_tables,
            "new_tables": self.new_tables,
            "success_tables": self.success_tables,
            "errored_tables": self.errored_tables,
            "identical_tables": self.identical_tables,
            "non_parquet_tables": self.non_parquet_tables,
            "iceberg_tables": self.iceberg_tables
        }
        return alterator_response
