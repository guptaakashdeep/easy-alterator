"""Main Class for getting the Iceberg Table Schema Changes."""
from typing import Dict, Any, Tuple, List
import json
from utils.s3_utils import read_s3_file
from utils.glue_utils import get_table_details
import logging

class IcebergSchemaHandler:
    def __init__(self, table: str, hql_str: str, iceberg_catalog: str="spark_catalog",
                requires_migration: bool = False, catalog: str="glue"):
        self.table = table
        self._db, self._table = table.split(".")
        self.hql = hql_str
        self.ic_catalog = iceberg_catalog
        self.catalog = catalog
        self.migration = requires_migration
        self.logger = logging.getLogger('EA.handler.iceberg_handler')
        self.col_rgx = r"""`(\w+)`\s+(\w+((\(\d+,\d+\))|(\(\d+\)))?),*"""
        self.partition_col_rgx = r"""PARTITIONED BY \(\s*(`[^`]+`(?:\s*,\s*`[^`]+`)*)\s*\)"""
        self.tblprop_rgx = r"""TBLPROPERTIES\s*\(\s*((?:'[\w.-]+'='[\w.-]+'\s*,?\s*)+)\)"""


    #TODO: Fetch columns, partition details, TBLPROPERTIES,  with sequence from HQL file
    def _get_schema_details_hql(self):
        print("Fetch all the required details from HQL string using regex.")

    #TODO: Get metadata JSON locaiton from Glue Catalog -- if migration=False
    def _get_metadata_location(self, tbl_dict: Dict[str, Any]) -> str:
        if 'metadata_location' in tbl_dict.get("Parameters", {}):
            return tbl_dict["Parameters"]["metadata_location"]
        else:
            raise Exception(f"{self.table} is not an ICEBERG table. Please check.")

    #TODO: Get Schema Details: migration=False from JSON, migration=True from catalog
    def _get_schema_details_metadata(self, metadata_json_path: str) -> Tuple[List, List, Dict[str, str]]:
        metadata_str = read_s3_file(metadata_json_path)
        metadata = json.loads(metadata_str)
        column_details = metadata["schemas"][metadata["current-schema-id"]]["fields"]
        partition_details = metadata["partition-specs"][metadata["default-spec-id"]]["fields"]
        tblprop_details = metadata["properties"]
        return column_details, partition_details, tblprop_details

    def _get_schema_details(self, tbl_dict: Dict[str, Any]) -> Tuple[List, List, Dict]:
        """Get Table Schema details from Glue Catalog. Should only be called
            when migration is True, i.e. Glue Catalog still has this table as Parquet
            and will be migrated to ICEBERG."""
        columns = tbl_dict['StorageDescriptor']['Columns']
        partition_cols = tbl_dict['PartitionKeys']
        all_columns: Dict[str, str] = columns + partition_cols
        if all_columns:
            # id column added to match the spec same as ICEBERG table.
            column_details = [{"id": i, **d} for i, d in enumerate(all_columns, start=1)]
            # only name and field-id is requried from this.
            partition_details = [{"field-id": i, **d} for i, d in enumerate(partition_cols, start=1000)]
            # TBLPROPERTIES dict doesn't matter as table is parquet currently.
            return column_details, partition_details, {}
        else:
            raise Exception("No columns extracted from Glue Catalog.")

    def _compare_schemas(self, catalog_details, hql_details):
        print("Use pandas to get the changes here...")


    #TODO: Compare Schema -- get the change details.
    def get_schema_updates(self):
        if self.catalog == "glue":
            tbl_details = get_table_details(self._db, self._table)['Table']
            if not self.migration:
                metadata_path = self._get_metadata_location(tbl_details)
                # c_ is to identify catalog columns, i.e. table details in catalog right now
                c_columns, c_partition_cols, c_tblprop = self._get_schema_details_metadata(metadata_path)
            else:
                c_columns, c_partition_cols, c_tblprop = self._get_schema_details(tbl_details)

    ## Comparison need to happen twice in case of migration = True
    # TODO: :thinking: comparison should be done before and after of the comparison ?