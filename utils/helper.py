"""Module for helper functions."""

from os import popen
import pandas as pd
from rules import rule_book as rbook


def intial_checks(table_info):
    """
    Runs the initial validation rules
    :param table_info: dict
    :return: bool
    """
    # RULES:
    # 1. TABLE_TYPE is EXTERNAL
    # 2. TABLE IS A PARQUET TABLE => check serde info
    for key, value in rbook.INITIAL_RULE_DICT.items():
        vresult = value(table_info)
        if not vresult:
            print(f"{key} validation failed.")
            # TODO: MAYBE update this later to something more meaningful ? A failure event ?
            return False
    return True


def compare_schema(new_col_list, old_col_list):
    """Compares the schema for provided column lists.
    Args:
        new_col_list (list of dict): _description_
        old_col_list (list of dict): _description_

    Returns:
        tuple (list of dict, list of dict, pandas Dataframe):
        (new columns, deleted columns, data type changed columns)
    """
    # Schema comparison logic ==>
    new_df = pd.DataFrame(new_col_list)
    old_df = pd.DataFrame(old_col_list)

    new_df["From"] = "new"
    old_df["From"] = "old"

    # includes added, deleted and data type changed columns
    combined_cols_df = pd.merge(
        new_df, old_df, on=["Name"], how="outer", suffixes=("_new", "_old")
    )[["Name", "Type_new", "Type_old"]]

    # new columns df
    new_cols_df = combined_cols_df[combined_cols_df["Type_old"].isna()]
    # print("new df \n", new_cols_df)

    # deleted columns df
    deleted_cols_df = combined_cols_df[combined_cols_df["Type_new"].isna()]
    # print("deleted df \n",deleted_cols_df)

    # getting columns with data type change
    datatype_changes = combined_cols_df[
        (~combined_cols_df["Type_old"].isna())
        & (~combined_cols_df["Type_new"].isna())
        & (combined_cols_df["Type_old"] != combined_cols_df["Type_new"])
    ]
    # print("datatype changes \n", datatype_changes)

    # new columns
    added_cols = new_cols_df.rename(columns={"Type_new": "Type"})[
        ["Name", "Type"]
    ].to_dict("records")
    # deleted columns
    deleted_cols = deleted_cols_df.rename(columns={"Type_old": "Type"})[
        ["Name", "Type"]
    ].to_dict("records")

    print("++++ Newly Added columns ==>", added_cols)
    print("---- Deleted columns ===>", deleted_cols)
    print("++++ New columns count ==>", len(added_cols))
    print("---- Deleted columns count ===>", len(deleted_cols))

    if not datatype_changes.empty:
        print(
            "+-+- data type changes records for: ", datatype_changes["Name"].to_list()
        )
    return added_cols, deleted_cols, datatype_changes


def get_account_id():
    return str(
        popen(
            "curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .accountId"
        )
        .read()
        .strip()
    )
