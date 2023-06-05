import re
import pandas as pd


def external_table_check(table_obj):
    """
    Checks if the table is EXTERNAL Table or not.
    :param table_obj: str or dict instance
    :return: bool
    """
    if isinstance(table_obj, dict):
        glue_tbl_type = table_obj['Table']['TableType']
        return True if glue_tbl_type.lower() == "external_table" else False
    elif isinstance(table_obj, str):
        ext_regex = "CREATE\s*(EXTERNAL)\s*table"
        match = re.search(ext_regex, table_obj, flags=re.IGNORECASE)
        return True if match.group(1).lower() == "external" else False
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

    if isinstance(table_obj, dict):
        table_format_detail = table_obj['Table']['StorageDescriptor']
        input_format = table_format_detail['InputFormat']
        output_format = table_format_detail['OutputFormat']
        serde_library = table_format_detail['SerdeInfo']['SerializationLibrary']
        if serde_library == PARQUET_ROW_FORMAT:
            return True if input_format == INPUT_SERDE and output_format == OUTPUT_SERDE else False
        else:
            return False
    elif isinstance(table_obj, str):
        print("inside string PV")
        store_regex = "STORED\s+AS\s+(\w+)"
        match = re.search(store_regex, table_obj, flags=re.IGNORECASE)
        if match:
            stored_as = match.group(1).lower()
            if stored_as == "parquet":
                return True
            elif stored_as == "inputformat":
                print("check for serde's here")
                row_fmt_regex = "ROW\s+FORMAT\s+SERDE\s+'([\w\.]+)'"
                row_fmt_match = re.search(row_fmt_regex, table_obj, flags=re.IGNORECASE)
                if row_fmt_match:
                    print("ROW FORMAT MATCHES..!!")
                    if row_fmt_match.group(1) == PARQUET_ROW_FORMAT.lower():
                        input_serde_regex = "INPUTFORMAT\s+'([\w\.]+)'"
                        input_serde_match = re.search(input_serde_regex, table_obj, flags=re.IGNORECASE)
                        output_serde_regex = "OUTPUTFORMAT\s+'([\w\.]+)'"
                        output_serde_match = re.search(output_serde_regex, table_obj, flags=re.IGNORECASE)
                        if input_serde_match and output_serde_match:
                            return True if input_serde_match.group(1) == INPUT_SERDE.lower() and output_serde_match.group(
                                1) == OUTPUT_SERDE.lower() else False
                        else:
                            print("INPUT/OUTPUT SERDE isn't Parquet SERDE ==>", input_serde_match.group(1), "\n",
                                  output_serde_match.group(1))
                            return False
                    else:
                        return False
                else:
                    return False
            else:
                return False
        else:
            return False
    else:
        return False


def partition_col_check(hql_str, catalog_partn_cols):
    """
    Check if the partition columns are changed in provided table HQL.
    Compares the partition columns from the already existing column.
    :param hql_str: hql string
    :param catalog_partn_cols: already existing partition columns list
    :return:
    """
    partition_regex = "PARTITIONED\s+BY\s+\(([\w`\s,]+)\)"
    match = re.search(partition_regex, hql_str, flags=re.IGNORECASE)
    if match:
        parition_cols = re.sub('\s+', ' ', match.group(1).lower().strip().replace('`','')).split(',')
        hql_pcols = [{"Name": col.strip().split(' ')[0], "Type": col.strip().split(' ')[1]} for col in parition_cols]
        hql_df = pd.DataFrame(hql_pcols)
        catalog_df = pd.DataFrame(catalog_partn_cols)
        diff_cols = pd.concat([hql_df, catalog_df]).drop_duplicates(keep=False)
        if diff_cols.empty:
            return True
        else:
            return False


INITIAL_RULE_DICT = {
    "EXTERNAL_TABLE": external_table_check,
    "PARQUET_CHECK": parquet_check
}
