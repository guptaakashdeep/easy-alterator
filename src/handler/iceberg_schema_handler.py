"""Main Class for getting the Iceberg Table Schema Changes."""

from typing import Dict, Any, Tuple, List, Union
import json
import logging
import re
from operator import itemgetter
import pandas as pd
from utils.s3_utils import read_s3_file
from utils.glue_utils import get_table_details


class IcebergSchemaHandler:
    """Class to get the schema changes for Iceberg Tables.

    This class handles comparing schema changes between an existing Iceberg table and a new schema
    defined in HQL. It can also handle schema migration from non-Iceberg to Iceberg tables.

    Args:
        table (str): The fully qualified table name in format 'database.table'
        hql_str (str): The HQL string containing the new table schema definition
        iceberg_catalog (str, optional): The Iceberg catalog name. Defaults to "spark_catalog"
        requires_migration (bool, optional): Whether table needs migration from non-Iceberg. Defaults to False
        catalog (str, optional): The catalog type. Defaults to "glue"

    Attributes:
        table (str): The fully qualified table name
        _db (str): The database name parsed from table
        _table (str): The table name parsed from table
        hql (str): The HQL string containing schema
        ic_catalog (str): The Iceberg catalog name
        catalog (str): The catalog type
        migration (bool): Whether table requires migration
        logger (Logger): Logger instance
        col_rgx (str): Regex pattern for matching column definitions
        partition_col_rgx (str): Regex pattern for matching partition columns
        tblprop_rgx (str): Regex pattern for matching table properties
    """

    def __init__(
        self,
        table: str,
        hql_str: str,
        iceberg_catalog: str = "spark_catalog",
        requires_migration: bool = False,
        catalog: str = "glue",
    ):
        self.table = table
        self._db, self._table = table.split(".")
        self.hql = hql_str
        self.ic_catalog = iceberg_catalog
        self.catalog = catalog
        self.migration = requires_migration
        self.logger = logging.getLogger("EA.handler.iceberg_handler")
        self.col_rgx = (
            r"""(--\s*[^\n`]*)?\s*`([\w-]+)`\s+(\w+((\(\d+,\d+\))|(\(\d+\)))?),*"""
        )
        self.partition_col_rgx = r"""PARTITIONED BY \(\s*((?:(?:--[^\n]*)?\s*`[^`]+`\s*(?:,|\n|\r\n)?\s*)+)\)"""
        self.tblprop_rgx = (
            r"""TBLPROPERTIES\s*\(\s*((?:'[\w.-]+'='[\w.-]+'\s*,?\s*)+)\)"""
        )

    # TODO: Fetch columns, partition details, TBLPROPERTIES,  with sequence from HQL file
    def _get_schema_details_hql(self) -> Tuple[List, List, Dict]:
        """Fetch Column, partitions and table properties
        detail from HQL string using REGEX."""

        column_matches = re.findall(self.col_rgx, self.hql, flags=re.IGNORECASE)
        # Add commented here to identify delete columns
        column_details = [
            {
                "id": id,
                "name": column[1],
                "type": column[2],
                "commented": True if "--" in column[0] else False,
            }
            for id, column in enumerate(column_matches, start=1)
        ]

        partition_matches = re.search(self.partition_col_rgx, self.hql, re.DOTALL | re.IGNORECASE)
        if partition_matches:
            columns_string = partition_matches.group(1)
            partition_columns = re.findall(
                r"(--[^\n`]*)?\s*`([^`]+)`", columns_string, re.DOTALL | re.IGNORECASE
            )
            partition_details = [
                {"field-id": id, "name": col_tup[1], "commented": bool(col_tup[0])}
                for id, col_tup in enumerate(partition_columns, start=1000)
            ]
            self.logger.debug("HQL Partition Details %s", partition_details)
        else:
            self.logger.info(
                "No partition columns present in HQL for table %s", self.table
            )
            partition_details = []

        tblprop_matches = re.search(self.tblprop_rgx, self.hql, re.DOTALL | re.IGNORECASE)
        if tblprop_matches:
            properties_string = tblprop_matches.group(1)
            tblprops = re.findall(r"'([\w.-]+)'='([\w.-]+)'", properties_string)
            tblprop_details = dict(tblprops)
        else:
            self.logger.info("No TBLPROPERTIES present in HQL for table %s", self.table)
            tblprop_details = []
        return column_details, partition_details, tblprop_details

    # Get metadata JSON locaiton from Glue Catalog -- if migration=False
    def _get_metadata_location(self, tbl_dict: Dict[str, Any]) -> str:
        if "metadata_location" in tbl_dict.get("Parameters", {}):
            return tbl_dict["Parameters"]["metadata_location"]
        else:
            raise Exception(f"{self.table} is not an ICEBERG table. Please check.")

    # Get Schema Details: migration=False from JSON, migration=True from catalog
    def _get_schema_details_metadata(
        self, metadata_json_path: str
    ) -> Tuple[List, List, Dict[str, str]]:
        metadata_str = read_s3_file(metadata_json_path)
        metadata = json.loads(metadata_str)
        column_details = metadata["schemas"][metadata["current-schema-id"]]["fields"]
        partition_details = metadata["partition-specs"][metadata["default-spec-id"]][
            "fields"
        ]
        tblprop_details = metadata["properties"]
        # Removing owner properties as this is not an actual iceberg property
        tblprop_details.pop("owner", None)
        return column_details, partition_details, tblprop_details

    def _get_schema_details(self, tbl_dict: Dict[str, Any]) -> Tuple[List, List, Dict]:
        """Get Table Schema details from Glue Catalog. Should only be called
        when migration is True, i.e. Glue Catalog still has this table as Parquet
        and will be migrated to ICEBERG."""
        columns = tbl_dict["StorageDescriptor"]["Columns"]
        partition_cols = tbl_dict["PartitionKeys"]
        all_columns: Dict[str, str] = columns + partition_cols
        if all_columns:
            # id column added to match the spec same as ICEBERG table.
            column_details = [
                {"id": i, **d} for i, d in enumerate(all_columns, start=1)
            ]
            # only name and field-id is requried from this.
            partition_details = [
                {"field-id": i, **d} for i, d in enumerate(partition_cols, start=1000)
            ]
            # TBLPROPERTIES dict doesn't matter as table is parquet currently.
            return column_details, partition_details, {}
        else:
            raise Exception("No columns extracted from Glue Catalog.")

    def _compare_schemas(
        self, catalog_details: Dict[str, Any], hql_details: Dict[str, Any]
    ) -> Dict[str, Any]:
        self.logger.info(
            "Catalog details received for schema comparison: \n %s", catalog_details
        )
        self.logger.info(
            "HQL details received for schema comparison: \n %s", hql_details
        )
        comparison_results = {"table": f"{self.ic_catalog}.{self.table}"}

        # Comparing Columns
        if catalog_details.get("columns") and hql_details.get("columns"):
            catalog_df = pd.DataFrame(catalog_details.get("columns"))
            if self.migration:
                catalog_df = catalog_df.rename(columns={"Name": "name", "Type": "type"})

            # Filter out all the records that has commented as true -- deleted columns
            filtered_cols = list(
                filter(lambda x: not x["commented"], hql_details.get("columns"))
            )
            hql_df = pd.DataFrame(filtered_cols)

            # Comparing both the schemas here.
            merged_df = pd.merge(
                catalog_df, hql_df, on=["id"], how="outer", suffixes=["_old", "_new"]
            )

            new_cols = (
                merged_df.loc[
                    lambda df: (df["name_old"].isna()) & (df["type_old"].isna()),
                    ["id", "name_new", "type_new"],
                ]
                .rename(columns={"name_new": "name", "type_new": "type"})
                .to_dict("records")
            )

            deleted_cols = [
                row["name_old"]
                for row in merged_df.loc[
                    lambda df: (df["name_new"].isna()) & (df["type_new"].isna()),
                    ["name_old"],
                ].to_dict("records")
            ]

            renamed_cols = (
                merged_df.loc[
                    lambda df: (df["name_old"] != df["name_new"])
                    & (df["type_old"] == df["type_new"]),
                    ["name_old", "name_new"],
                ]
                .rename(columns={"name_old": "old_name", "name_new": "new_name"})
                .to_dict("records")
            )

            updated_cols = (
                merged_df.loc[
                    lambda df: (df["name_old"] == df["name_new"])
                    & (df["type_old"] != df["type_new"]),
                    ["name_old", "type_old", "type_new"],
                ]
                .rename(
                    columns={
                        "name_old": "name",
                        "type_old": "old_type",
                        "type_new": "new_type",
                    }
                )
                .to_dict("records")
            )

            comparison_results["columns"] = {
                "new": sorted(new_cols, key=itemgetter("id")),
                "dropped": deleted_cols,
                "renamed": renamed_cols,
                "updated": updated_cols,
            }
        else:
            raise ValueError("One of the catalog or hql table details are not fetched.")

        # Comparing Partition Columns
        catalog_part_columns = catalog_details.get("partition_columns")
        hql_part_columns = hql_details.get("partition_columns")
        if catalog_part_columns and hql_part_columns:
            catalog_part_df = pd.DataFrame(catalog_part_columns)
            if self.migration:
                catalog_part_df = catalog_part_df.rename(columns={"Name": "name"})
            catalog_part_df = catalog_part_df[["field-id", "name"]]
            # Filter the commented=True columns
            filtered_part_cols = list(
                filter(lambda x: not x["commented"], hql_part_columns)
            )
            hql_part_df = pd.DataFrame(filtered_part_cols)[["field-id", "name"]]
            # Compare partition columns here.
            merged_part_df = pd.merge(
                catalog_part_df,
                hql_part_df,
                on=["field-id"],
                how="outer",
                suffixes=["_old", "_new"],
            )
            # new partitions cols
            new_part_cols = (
                merged_part_df.loc[
                    lambda df: (df["name_old"].isna()), ["field-id", "name_new"]
                ]
                .rename(columns={"name_new": "name"})
                .to_dict("records")
            )

            # dropped partition cols
            dropped_part_cols = [
                row["name_old"]
                for row in merged_part_df.loc[
                    lambda df: (df["name_new"].isna()), ["name_old"]
                ].to_dict("records")
            ]

            # replaced partition cols
            replaced_part_cols = (
                merged_part_df.loc[
                    lambda df: (df["name_old"] != df["name_new"])
                    & (df["name_old"].notna())
                    & (df["name_new"].notna()),
                    ["name_old", "name_new"],
                ]
                .rename(columns={"name_old": "old_name", "name_new": "new_name"})
                .to_dict("records")
            )
        else:
            replaced_part_cols = []
            dropped_part_cols = catalog_part_columns
            new_part_cols = hql_part_columns

        # Updating comparison_results dict with Partition cols
        comparison_results["partition_columns"] = {
            "new": sorted(new_part_cols, key=itemgetter("field-id")),
            "dropped": dropped_part_cols,
            "replaced": replaced_part_cols,
        }

        # TBLPROP comparison is only possible when both sides are ICEBREG.
        catalog_tblprops = catalog_details.get("table_properties")
        hql_tblprops = hql_details.get("table_properties")
        if not self.migration:
            if catalog_tblprops and hql_tblprops:
                self.logger.debug("Both CATALOG and HQL Props are present.")
                # Checks keys that are removed
                removed_props: List[str] = list(
                    set(catalog_tblprops.keys()) - set(hql_tblprops.keys())
                )
                # Check for any new key added
                new_keys = list(set(hql_tblprops.keys()) - set(catalog_tblprops.keys()))
                new_props: Union[Dict[str, str], dict] = (
                    {new_prop: hql_tblprops[new_prop] for new_prop in new_keys}
                    if new_keys
                    else {}
                )
                # Check for updated columns
                common_keys = list(
                    set(catalog_tblprops.keys()).intersection(set(hql_tblprops.keys()))
                )
                updated_props: Union[Dict[str, str], dict] = (
                    {ckey: hql_tblprops[ckey] for ckey in common_keys}
                    if common_keys
                    else {}
                )
            else:
                updated_props = {}
                removed_props = (
                    list(catalog_tblprops.keys()) if catalog_tblprops else []
                )
                new_props = hql_tblprops if hql_tblprops else {}
        else:
            new_props = hql_tblprops if hql_tblprops else {}
            removed_props = []
            updated_props = {}

        # Updating comparison_results dict with TBLPROPERTIES
        comparison_results["tblprops"] = {
            "new": new_props,
            "removed": removed_props,
            "updated": updated_props,
        }
        return comparison_results

    def get_schema_updates(self) -> Dict[str, Any]:
        """
        Compares schema details between HQL and catalog (Glue) to identify changes.

        Gets schema details from both HQL and catalog source, then compares them to identify:
        - New, dropped, renamed and updated columns
        - New, dropped and replaced partition columns
        - New, removed and updated table properties

        Returns:
            Dict[str, Any]: Dictionary containing schema comparison results with structure:
                {
                    'table': str,  # Table name
                    'columns': {  # Column changes
                        'new': List[Dict],  # New columns added
                        'dropped': List[str],  # Columns removed
                        'renamed': List[Dict],  # Columns renamed
                        'updated': List[Dict]  # Columns with type changes
                    },
                    'partition_columns': {  # Partition column changes
                        'new': List[Dict],  # New partition columns
                        'dropped': List[str],  # Removed partition columns
                        'replaced': List[Dict]  # Modified partition columns
                    },
                    'tblprops': {  # Table property changes
                        'new': Dict,  # New properties added
                        'removed': List[str],  # Properties removed
                        'updated': Dict  # Properties modified
                    }
                }

        Raises:
            ValueError: If catalog type is not supported or schema details cannot be fetched
        """
        h_columns, h_partition_cols, h_tblprop = self._get_schema_details_hql()
        if self.catalog == "glue":
            tbl_details = get_table_details(self._db, self._table)["Table"]
            if not self.migration:
                metadata_path = self._get_metadata_location(tbl_details)
                # c_ is to identify catalog columns, i.e. table details in catalog right now
                c_columns, c_partition_cols, c_tblprop = (
                    self._get_schema_details_metadata(metadata_path)
                )
            else:
                c_columns, c_partition_cols, c_tblprop = self._get_schema_details(
                    tbl_details
                )
        else:
            raise ValueError(f"{self.catalog} is not supported yet.")

        hql_details = {
            "columns": h_columns,
            "partition_columns": h_partition_cols,
            "table_properties": h_tblprop,
        }

        catalog_details = {
            "columns": c_columns,
            "partition_columns": c_partition_cols,
            "table_properties": c_tblprop,
        }

        results = self._compare_schemas(catalog_details, hql_details)
        results["migration"] = self.migration
        self.logger.info("Schema comparison results: \n %s", results)

        # Filter out all the keys that has no data from results nested dict
        cleaned_result = self.clean_results(results)

        return cleaned_result if cleaned_result.get("columns") or \
                cleaned_results.get("partition_columns") or \
                cleaned_results.get("tblprops") else {}

    def clean_results(self, result: Union[Dict, List]) -> Dict[str, Any]:
        """
        Recursively removes empty values from nested dictionaries and lists.

        Args:
            result (Union[Dict, List]): Input dictionary or list that may contain empty values

        Returns:
            Dict[str, Any]: Cleaned dictionary with empty values removed

        Example:
            Input: {'a': [], 'b': {'c': [], 'd': [1,2]}, 'e': [1,2,3]}
            Output: {'b': {'d': [1,2]}, 'e': [1,2,3]}
        """
        if isinstance(result, dict):
            return {k: self.clean_results(v) for k, v in result.items() if v}
        if isinstance(result, list):
            return [self.clean_results(x) for x in result] if result else None
        return result