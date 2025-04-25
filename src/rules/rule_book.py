"""Module for Validation Rules."""

import re
import pandas as pd
import logging

logger = logging.getLogger('EA.rule_book')


def external_table_check(table_obj):
    """
    Checks if the table is EXTERNAL Table or not.
    :param table_obj: str or dict instance
    :return: bool
    """
    if isinstance(table_obj, dict):
        glue_tbl_type = table_obj["Table"]["TableType"]
        return True if glue_tbl_type.lower() == "external_table" else False
    elif isinstance(table_obj, str):
        ext_regex = r"CREATE\s*(EXTERNAL)\s*table"
        match = re.search(ext_regex, table_obj, flags=re.IGNORECASE)
        if match:
            return True if match.group(1).lower() == "external" else False
        else:
            return False
    else:
        raise Exception("Passed object for validation is neither string nor dict.")


def parquet_check(table_obj):
    """
    Checks if the table is Parquet table or not
    :param table_obj: str or dict instance
    :return: bool
    """
    PARQUET_ROW_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    INPUT_SERDE = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    OUTPUT_SERDE = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    def check_dict_format(table_format_detail):
        if "SerdeInfo" in table_format_detail:
            return (
                table_format_detail["SerdeInfo"]["SerializationLibrary"] == PARQUET_ROW_FORMAT
                and table_format_detail["InputFormat"] == INPUT_SERDE
                and table_format_detail["OutputFormat"] == OUTPUT_SERDE
            )
        else:
            return False

    def check_str_format(table_str):
        store_regex = r"STORED\s+AS\s+(\w+)"
        match = re.search(store_regex, table_str, flags=re.IGNORECASE)
        if not match:
            return False
        stored_as = match.group(1).lower()
        if stored_as == "parquet":
            return True
        if stored_as != "inputformat":
            return False

        row_fmt_regex = r"ROW\s+FORMAT\s+SERDE\s+'([\w\.]+)'"
        row_fmt_match = re.search(row_fmt_regex, table_str, flags=re.IGNORECASE)
        if not row_fmt_match or row_fmt_match.group(1).lower() != PARQUET_ROW_FORMAT.lower():
            return False

        input_serde_regex = r"INPUTFORMAT\s+'([\w\.]+)'"
        output_serde_regex = r"OUTPUTFORMAT\s+'([\w\.]+)'"
        input_serde_match = re.search(input_serde_regex, table_str, flags=re.IGNORECASE)
        output_serde_match = re.search(output_serde_regex, table_str, flags=re.IGNORECASE)
        return (
            input_serde_match
            and output_serde_match
            and input_serde_match.group(1).lower() == INPUT_SERDE.lower()
            and output_serde_match.group(1).lower() == OUTPUT_SERDE.lower()
        )

    if isinstance(table_obj, dict):
        return check_dict_format(table_obj["Table"]["StorageDescriptor"])
    if isinstance(table_obj, str):
        return check_str_format(table_obj)


def partition_col_check(hql_str_dict, catalog_partn_cols):
    """
    Check if the partition columns are changed in provided table HQL.
    Compares the partition columns from the already existing column.
    :param hql_str_dict: hql string or partition cols list of dict
    :param catalog_partn_cols: already existing partition columns list
    :return: bool
    """
    def parse_hql(hql_str):
        partition_regex = r"PARTITIONED\s+BY\s+\(([\w`\s,]+)\)"
        match = re.search(partition_regex, hql_str, flags=re.IGNORECASE)
        if match:
            partition_cols = re.sub(r"\s+", " ", match.group(1).lower().strip().replace("`", "")).split(",")
            return [{"Name": col.split()[0], "Type": col.split()[1]} for col in partition_cols]
        return []

    hql_df = pd.DataFrame(hql_str_dict if isinstance(hql_str_dict, list) else parse_hql(hql_str_dict))
    catalog_df = pd.DataFrame(catalog_partn_cols)

    if hql_df.shape[0] != catalog_df.shape[0]:
        logger.error("=> Partitions column mismatch")
        return False

    if hql_df.empty or catalog_df.empty:
        logger.info("=> No partitions found.")
        return True

    merged_df = pd.merge(hql_df, catalog_df, on="Name", how="outer", suffixes=("_new", "_old"))
    if merged_df["Type_new"].isna().any() or merged_df["Type_old"].isna().any():
        logger.error("Partition column mismatch.")
        return False

    if (merged_df["Type_new"] != merged_df["Type_old"]).any():
        logger.error("=> Partition column data type mismatch.")
        return False

    logger.debug("=> Partition columns match")
    return True


def check_dtype_compatibility(df, query_engine="athena", name_alias="Name",
            new_type_alias="Type_new", old_type_alias="Type_old"):
    """
    Checks if the changed data type of the column is compatible with the 
    new data type for the mentioned query engine
    :param df: pandas dataframe
    :param query_engine: str, query engine name. Default is "athena"
    :return: bool
    """
    compatibility_dict = QUERY_ENG_DTYPE_COMPATIBILITY[query_engine]
    # TODO: Add logic to handle decimal to decimal conversion, Decimal(Scale, Precision)
    ## No backfill required for Precision increased, Backfill required for Precision reduced. 
    df["compatible"] = df.apply(
        lambda x: 1
        if (x[new_type_alias].upper() in compatibility_dict.get(x[old_type_alias].upper(), [])
            or ("decimal" in x[new_type_alias].lower() and "decimal" in x[old_type_alias].lower()
                and _is_decimal_compatible(x[old_type_alias], x[new_type_alias])
                )
        )
        else 0,
        axis=1,
    )
    incompatible_cols = df[df["compatible"] == 0]
    compatible_cols = df[df["compatible"] == 1]
    if not incompatible_cols.empty:
        logger.info("==> Incompatible data type change found in the DDL: ")
        for row in incompatible_cols.to_dict(orient="records"):
            logger.warning(
                '%s data type changed from %s to %s',
                row[name_alias], row[old_type_alias], row[new_type_alias]
            )
        logger.warning(
            "==> Please change the data type of the following columns to the compatible data type."
        )
        return False, compatible_cols, incompatible_cols
    return True, compatible_cols, incompatible_cols


def iceberg_check(table_obj) -> bool:
    # TODO: Implement code with regex to check from HQL.
    if isinstance(table_obj, str):
        FMT_RGX = r"USING\s+(\w+)"
        fmt_match = re.search(FMT_RGX, table_obj, flags=re.IGNORECASE)
        if not fmt_match or fmt_match.group(1).upper() != "ICEBERG":
            return False
        else:
            return True
    if isinstance(table_obj, dict):
        table_format = table_obj.get('Table')\
            .get('Parameters', {})\
            .get('table_type','').upper()
        return True if table_format == "ICEBERG" else False


def convert_varchar(data_type):
    """Converts any 'varchar' string to string"""
    return re.sub(r'varchar\(\d+\)', 'string', data_type, flags=re.IGNORECASE)

def _process_decimal_type(column_type):
    decimal_pattern = r'decimal\((\d+),\s*(\d+)\)'
    return re.sub(decimal_pattern, r'decimal(\1, \2)', column_type)


def convert_data_type(column_type):
    if column_type.lower().startswith('varchar'):
        return convert_varchar(column_type)
    if column_type.lower().startswith('decimal'):
        return _process_decimal_type(column_type)
    return SPARK_DTYPE_MAP.get(column_type, column_type)


def _is_decimal_compatible(old_type: str, new_type: str) -> bool:
    """
    Check if decimal type change is compatible.
    Compatible: decimal(P,S) to decimal(P2,S) when P2 > P (scale must remain the same)
    """

    # Use compiled regex patterns for better performance
    decimal_pattern = re.compile(r'decimal\((\d+),\s*(\d+)\)')
    
    old_decimal_match = decimal_pattern.match(old_type)
    new_decimal_match = decimal_pattern.match(new_type)
    
    if old_decimal_match and new_decimal_match:
        old_precision, old_scale = int(old_decimal_match.group(1)), int(old_decimal_match.group(2))
        new_precision, new_scale = int(new_decimal_match.group(1)), int(new_decimal_match.group(2))
        
        # Compatible only if scale is the same and new precision is greater
        return old_scale == new_scale and new_precision > old_precision
    
    return True



INITIAL_RULE_DICT = {
    "EXTERNAL_TABLE": external_table_check,
    "PARQUET_CHECK": parquet_check,
    "ICEBERG_CHECK": iceberg_check
}

QUERY_ENG_DTYPE_COMPATIBILITY = {
    "athena": {
        "STRING": ["BYTE", "TINYINT", "SMALLINT", "INT", "BIGINT", "VARCHAR"],
        "BYTE": ["TINYINT", "SMALLINT", "INT", "BIGINT"],
        "TINYINT": ["SMALLINT", "INT", "BIGINT"],
        "SMALLINT": ["INT", "BIGINT"],
        "INT": ["BIGINT"],
        "FLOAT": ["DOUBLE"],
        "DECIMAL": ["DECIMAL"],
        "VARCHAR": ["VARCHAR"]
    },
    "iceberg": {
        "STRING": [],
        "BYTE": [],
        "TINYINT": ["SMALLINT", "INT", "BIGINT"],
        "SMALLINT": ["INT", "BIGINT"],
        "INT": ["BIGINT"],
        "FLOAT": ["DOUBLE"],
        "DECIMAL": ["DECIMAL"],
        "VARCHAR": ["VARCHAR"]
    }
}

# HQL DTYPE to SPARK DTYPE
SPARK_DTYPE_MAP = {
    "bigint": "long"
}

# ICEBERG DEFAULT PROP EXCLUSION
ICEBERG_DEFAULT_PROP = [
    "write.parquet.compression-codec",
    "schema.name-mapping.default"
]
