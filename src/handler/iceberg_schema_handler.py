"""Main Class for getting the Iceberg Table Schema Changes."""
import json
import logging
import re
from operator import itemgetter
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union

import pandas as pd
from rules.column_position_manager import ColumnPositionManager
from rules.rule_book import check_dtype_compatibility
from rules.rule_book import convert_data_type
from rules.rule_book import ICEBERG_DEFAULT_PROP
from rules.rule_book import map_iceberg_to_spark_dtype
from utils.glue_utils import get_table_details
from utils.s3_utils import read_s3_file


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
        # TODO: Update regex to support --backfilled_from:
        self.col_rgx = (
            r"""(--\s*[^\n`]*)?\s*`([\w-]+)`\s+(\w+((\(\d+,\s*\d+\))|(\(\d+\)))?),*"""
            r"""(?:\s*--\s*(?:renamed_from:\s*([^\s,]+)|after:\s*([^\s,]+)|backfilled_from:\s*([^\s,]+)|first))?"""
        )
        self.partition_col_rgx = r"""PARTITIONED BY\s*\(\s*((?:(?:--[^\n]*)?\s*`[^`]+`\s*(?:,|\n|\r\n)?\s*)+)\)"""
        self.tblprop_rgx = (
            r"""TBLPROPERTIES\s*\(\s*((?:'[\w.-]+'='[\w.-]+'\s*,?\s*)+)\)"""
        )
        self.iceberg_metadata = None

    def _get_schema_details_hql(self) -> Tuple[List, List, Dict]:
        """Fetch Column, partitions and table properties
        detail from HQL string using REGEX."""

        column_matches = re.findall(self.col_rgx, self.hql, flags=re.IGNORECASE)
        # 1. Handle data type udpates like in Spark `bigint` is defined as `long`
        # and other such data types.
        # Add commented here to identify delete columns
        # TODO: add backfilled_from column here.
        column_details = [
            {
                "name": column[1],
                "type": (
                    convert_data_type(column[2]) if not self.migration else column[2]
                ),
                "commented": True if "--" in column[0] else False,
                "renamed_from": column[6] if len(column) > 6 and column[6] else None,
                "after": column[7] if len(column) > 7 and column[7] else None,
                "backfilled_from": column[8] if len(column) > 8 and column[8] else None,
                "first": True if len(column) > 9 and column[9] else False,
            }
            for _, column in enumerate(column_matches, start=1)
        ]

        partition_matches = re.search(
            self.partition_col_rgx, self.hql, re.DOTALL | re.IGNORECASE
        )
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

        tblprop_matches = re.search(
            self.tblprop_rgx, self.hql, re.DOTALL | re.IGNORECASE
        )
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
        self.iceberg_metadata = metadata
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

    # TODO: This need changes to identify cascade position changes
    def _get_valid_after_updates(
        self, after_df: pd.DataFrame, metadata_df: pd.DataFrame
    ) -> Any:
        """Validate if after updates are valid."""
        # 1. if the after column exists in the catalog and not marked for deletion.
        after_cols_mapping = after_df[["name", "after"]].to_dict("records")
        after_cols = set([cdict["after"] for cdict in after_cols_mapping])
        metadata_cols = set(metadata_df["name"].to_list())
        matching_cols = after_cols.intersection(metadata_cols)

        if not matching_cols:
            print("After columns are not present in the Table Schema.")
            return None

        # 2. if the position is already in place - no need for after updates anymore.
        # Need to check for the sequence in the metadata.json file
        current_cols_order: List[Dict] = self.iceberg_metadata["schemas"][
            self.iceberg_metadata["current-schema-id"]
        ]["fields"]
        # get the index of next column for matching_cols from the current schema
        after_col_dict = {}
        for idx, col_dict in enumerate(current_cols_order):
            name = col_dict["name"]
            if name in matching_cols:
                next_name = (
                    current_cols_order[idx + 1]["name"]
                    if idx + 1 < len(current_cols_order)
                    else None
                )
                after_col_dict[name] = next_name

        matching_next_col_df = pd.DataFrame(
            {
                "after": list(after_col_dict.keys()),
                "name": list(after_col_dict.values()),
            }
        )

        # Identify the updated positions
        final_df = pd.merge(
            after_df,
            matching_next_col_df,
            on="after",
            how="inner",
            suffixes=["_expected", "_current"],
        )
        # df with details of updated column positions
        valid_after_df = final_df.loc[
            lambda df: df["name_expected"] != df["name_current"]
        ]
        # Check if there are changes it's causing any cascading effects
        if valid_after_df.empty:
            return []

        # If changes are present identify direct and cascading position changes
        position_manager = ColumnPositionManager()
        positions = position_manager.extract_col_position(after_cols_mapping)
        position_manager.build_dependency_graph(positions)
        changes = {
            cdict["name_expected"]: cdict["after"]
            for cdict in valid_after_df[["after", "name_expected"]].to_dict("records")
        }
        # list of dict with keys: "name", "after" and "reason" [direct, cascade]
        all_changes = position_manager.generate_position_changes(changes)
        return all_changes

    def _compare_schemas(
        self, catalog_details: Dict[str, Any], hql_details: Dict[str, Any]
    ) -> Dict[str, Any]:
        self.logger.debug(
            "Catalog details received for schema comparison: \n %s", catalog_details
        )
        self.logger.debug(
            "HQL details received for schema comparison: \n %s", hql_details
        )
        comparison_results = {"table_name": f"{self.ic_catalog}.{self.table}"}

        # Comparing Columns
        if catalog_details.get("columns") and hql_details.get("columns"):
            catalog_df = pd.DataFrame(catalog_details.get("columns"))
            if self.migration:
                catalog_df = catalog_df.rename(columns={"Name": "name", "Type": "type"})

            # Filter out all the records that has commented as true -- deleted columns
            # filtered_cols = list(
            #     filter(lambda x: not x["commented"], hql_details.get("columns"))
            # )
            hql_df = pd.DataFrame(hql_details.get("columns"))
            # Add is_renamed, is_deleted, after, first flags
            hql_df["is_renamed"] = hql_df["renamed_from"].notna() & (
                hql_df["renamed_from"] != ""
            )
            # TODO: how to identify if after has already been applied ?
            # If doesn't exist, not an issue. goes to new_columns with "after" key.
            # if exists, check if the previous if the previous id column matching in the metadata.json
            # TODO: add is_backfilled flag here.
            hql_df["is_after"] = hql_df["after"].notna() & (hql_df["after"] != "")
            hql_df["is_first"] = (
                hql_df["first"].notna() & (hql_df["first"]) & (hql_df["first"] != "")
            )
            hql_df["is_deleted"] = hql_df["commented"].notna() & hql_df["commented"]
            hql_df["is_backfilled"] = (
                hql_df["backfilled_from"].notna() & hql_df["backfilled_from"]
            )

            # Making columns comparison case insensitive
            hql_df["name"] = hql_df["name"].str.lower()
            catalog_df["name"] = catalog_df["name"].str.lower()

            # For identifying new columns + extended deletes(future implementation).
            merged_df = pd.merge(
                catalog_df, hql_df, on=["name"], how="outer", suffixes=["_old", "_new"]
            )

            # For checking valid renames and deletes.
            common_df = pd.merge(
                catalog_df, hql_df, on=["name"], how="inner", suffixes=["_old", "_new"]
            )

            # Only commented columns are marked for deletion.
            # S1. Need to identify columns present in catalog but not in DDL.
            # S2. Missing columns from DDL are not part of renamed columns.
            deleted_cols = [
                d["name"]
                for d in common_df.loc[lambda df: (df["is_deleted"]), ["name"]].to_dict(
                    "records"
                )
            ]

            # Non commented deletes:
            # Columns missing in user provided DDL but present in Catalog metadata
            # These columns might also be getting renamed so need to be made sure these columns are not part of renamed_from
            non_commented_deletes = [
                d["name"]
                for d in merged_df.loc[
                    lambda df: (
                        df["type_new"].isna()
                        & df["type_old"].notna()
                        & ~df["name"].isin(df["renamed_from"])
                    ),
                    ["name"],
                ].to_dict("records")
            ]

            # extend the deleted columns list
            deleted_cols.extend(non_commented_deletes)

            # identify Renamed Columns:
            # only existing columns in catalog can be renamed.
            renamed_cols = (
                pd.merge(
                    catalog_df,
                    hql_df,
                    left_on="name",
                    right_on="renamed_from",
                    how="inner",
                    suffixes=["_old", "_new"],
                )[["renamed_from", "name_new"]]
                .rename(columns={"name_new": "new_name", "renamed_from": "old_name"})
                .to_dict("records")
            )

            # Identify Updated Columns
            updated_cols = {}
            # Only that are present in catalog can be updated.
            types_updated_df = common_df.loc[
                lambda df: df["type_old"] != df["type_new"],
                ["name", "type_old", "type_new", "after", "first"],
            ].rename(columns={"type_old": "old_type", "type_new": "new_type"})

            # Check for compatible and incompatible data type changes
            if not types_updated_df.empty:
                (
                    all_compatible,
                    compatible_df,
                    incompatible_df,
                ) = check_dtype_compatibility(
                    types_updated_df,
                    query_engine="iceberg",
                    name_alias="name",
                    old_type_alias="old_type",
                    new_type_alias="new_type",
                )

                # TODO: inner join incompatible_df with hql_df on name == backfilled_from
                # to get the backfilled_from column included in incompatible records
                final_incompatible_df = incompatible_df.merge(
                    hql_df, on=["name"], how="left"
                )[["name", "old_type", "new_type", "backfilled_from"]]

                # incompatible column must have a backfilled_from column
                if not final_incompatible_df[
                    final_incompatible_df["backfilled_from"].isna()
                ].empty:
                    raise ValueError(
                        f"backfilled_from is missing for column in DDL file for: {comparison_results['table_name']} \n {final_incompatible_df.to_dict('records')}"
                    )

                updated_cols = {"compatible": compatible_df.to_dict("records")}
                if not all_compatible:
                    updated_cols["incompatible"] = final_incompatible_df.to_dict(
                        "records"
                    )

            # Check position based updates
            # Position can only be UPDATED for the columns that are already existing.
            cols_with_after_update = common_df.loc[
                lambda df: (df["type_old"] == df["type_new"])
                & df["is_after"]
                & (~df["is_deleted"])
            ]
            after_position_cols = None
            # Changes for identifying ccascading position changes
            if not cols_with_after_update.empty:
                validated_after_pos = self._get_valid_after_updates(
                    cols_with_after_update, catalog_df
                )
                if validated_after_pos:
                    after_position_cols = validated_after_pos
                # if (
                #     validated_after_pos_df is not None
                #     and not validated_after_pos_df.empty
                # ):
                #     after_position_cols = (
                #         validated_after_pos_df[["name_expected", "after"]]
                #         .rename(columns={"name_expected": "name"})
                #         .to_dict("records")
                #     )

            # TODO: 1. There can't be more than once column as first for a table.
            first_position_col = common_df.loc[
                lambda df: (df["type_old"] == df["type_new"]) & df["is_first"], ["name"]
            ].to_dict("records")

            # TODO: Validate if first is valid.
            # 2. Check if the position wise first column is already the marked one.
            # In this no update required

            # Add Positional Updates in updated cols list
            if after_position_cols or first_position_col:
                updated_cols["position_changes"] = []
                if after_position_cols:
                    updated_cols["position_changes"].extend(after_position_cols)
                if first_position_col:
                    updated_cols["position_changes"].append(
                        {"first": first_position_col[0]["name"]}
                    )

            self.logger.debug("Updated Columns: %s", updated_cols)

            # New columns identications
            # Added a filter to remove any columns present in backfilled_from
            # Because actual column during schema updates will be renamed to name
            # present in backfilled_from.
            new_cols = (
                merged_df.loc[
                    lambda df: (df["id"].isna())
                    & (df["is_renamed"].isna() | ~(df["is_renamed"].fillna(False)))
                    & (~(df["is_deleted"].fillna(False)))
                    & (~df["name"].isin(df["backfilled_from"])),
                    ["name", "type_new", "after", "first"],
                ]
                .rename(columns={"type_new": "type"})
                .to_dict("records")
            )

            comparison_results["columns"] = {
                "new": new_cols,
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
            # Making columns comparison case insensitive
            hql_part_df["name"] = hql_part_df["name"].str.lower()
            catalog_part_df["name"] = catalog_part_df["name"].str.lower()
            # Compare partition columns here.
            merged_part_df = pd.merge(
                catalog_part_df,
                hql_part_df,
                on=["field-id"],
                how="outer",
                suffixes=["_old", "_new"],
            )

            # print("#### MERGED DF ####")
            # print(merged_part_df)
            # new partitions cols
            new_part_cols = (
                merged_part_df.loc[
                    lambda df: (df["name_old"].isna()), ["field-id", "name_new"]
                ]
                .rename(columns={"name_new": "name", "field-id": "field_id"})
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
            "new": sorted(new_part_cols, key=itemgetter("field_id")),
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
                # Exclude keys that are default mentioned in ICEBERG_DEFAULT_PROP
                catalog_filtered_props = list(
                    filter(
                        lambda x: x not in ICEBERG_DEFAULT_PROP, catalog_tblprops.keys()
                    )
                )
                removed_props: List[str] = list(
                    set(catalog_filtered_props) - set(hql_tblprops.keys())
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
                # FIXME: Check if the common keys properties are matching or not.
                # before assigning that to update_props
                updated_props: Union[Dict[str, str], dict] = (
                    {
                        ckey: hql_tblprops[ckey]
                        for ckey in common_keys
                        if hql_tblprops[ckey] != catalog_tblprops[ckey]
                    }
                    if common_keys
                    else {}
                )
            else:
                updated_props = {}
                # TODO: needs revisiting for handling more complex cases:
                # What happens to the default properties that are not in HQL but
                # added while table creation ? Can't be filtered simply because
                # in that case, what happens if those default properties are updated in HQL?
                removed_props = (
                    {}
                )  # (list(catalog_tblprops.keys()) if catalog_tblprops else [])
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
                    'table_name': str,  # Table name
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
                (
                    ic_c_columns,
                    c_partition_cols,
                    c_tblprop,
                ) = self._get_schema_details_metadata(metadata_path)
                c_columns = map_iceberg_to_spark_dtype(ic_c_columns)
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

        # in case migration is required, check if columns are in order
        # TODO: Maybe add this behavior based on some config ??
        if not self.migration:
            results = self._compare_schemas(catalog_details, hql_details)
        else:
            print(
                "Check if sequence is same. If yes then compare schema, else mark it as mismatched sequence."
            )
            # Remove commented from hql_column_details and required from catalog_column_details
            hql_cols = [
                {k: v for k, v in hql_cdetails.items() if k != "commented"}
                for hql_cdetails in hql_details["columns"]
            ]
            catalog_cols = [
                {k.lower(): v for k, v in cdetails.items() if k != "required"}
                for cdetails in catalog_details["columns"]
            ]
            if self._same_order(hql_cols, catalog_cols):
                results = self._compare_schemas(catalog_details, hql_details)
            else:
                results = {
                    "table_name": f"{self.ic_catalog}.{self.table}",
                    "sequenceMismatch": "True",
                }
        results["migration"] = str(self.migration)
        self.logger.info("Schema comparison results: \n %s", results)

        # Filter out all the keys that has no data from results nested dict
        cleaned_result = self.clean_results(results)

        return (
            cleaned_result
            if cleaned_result.get("columns")
            or cleaned_result.get("partition_columns")
            or cleaned_result.get("tblprops")
            or cleaned_result.get("sequenceMismatch")
            else {}
        )

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

    def _same_order(
        self, list1: List[Dict[str, Any]], list2: List[Dict[str, Any]]
    ) -> bool:
        """Checks the sequence of elements in dictionary."""
        return len(list1) == len(list2) and all(
            d1 == d2 for d1, d2 in zip(list1, list2)
        )
