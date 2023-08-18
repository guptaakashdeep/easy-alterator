"""Module to handle AWS Glue Catalog related operations"""

from copy import deepcopy
import boto3
from botocore.exceptions import ClientError
from utils.helper import get_aws_region

REGION = get_aws_region() 


def get_table_details(database, table):
    """
    Gets the table details from the AWS Glue catalog.
    :param client: boto3 client
    :param database: str
    :param table: str
    :return: dict
    """
    try:
        client = boto3.client("glue", region_name=REGION)
        response = client.get_table(DatabaseName=database, Name=table)
        return response
    except ClientError as error:
        err_response = error.response
        if err_response["Error"]["Code"] == "EntityNotFoundException":
            print(err_response["Message"])
        return err_response
    except Exception as ex:
        print("Error occured while getting table from catalog.")
        raise ex


def update_table_schema(table, new_cols, del_cols):
    """
    Update the table schema in AWS Glue catalog.
    Returns the update status as True, False along with db.table_name
    :param client: boto3 glue client
    :param table: dict
    :param new_cols: list of dict
    :param del_cols: list of dict
    :return: tuple: (Bool, string, dict)
    """
    glue_client = boto3.client("glue", region_name=REGION)
    updated_table = deepcopy(table)
    db_name = table["Table"]["DatabaseName"]
    table_name = table["Table"]["Name"]
    # removing unecessary keys
    # can be implemented better with @dataclass
    extra_keys = [
        "DatabaseName",
        "CreateTime",
        "UpdateTime",
        "CreatedBy",
        "IsRegisteredWithLakeFormation",
        "CatalogId",
        "VersionId",
        "FederatedTable",
    ]
    for key in extra_keys:
        updated_table["Table"].pop(key, None)

    # add new cols:
    existing_columns = table["Table"]["StorageDescriptor"]["Columns"]
    new_cols_list = existing_columns + new_cols

    # remove deleted columns
    if del_cols:
        updated_columns = list(filter(lambda d: d not in del_cols, new_cols_list))
    else:
        updated_columns = new_cols_list

    print("Final cols list ==>", updated_columns)
    updated_table["Table"]["StorageDescriptor"]["Columns"] = updated_columns

    up_response = glue_client.update_table(
        DatabaseName=db_name, TableInput=updated_table["Table"]
    )

    # Check if the update is successful or not.
    if up_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print(f"Update successful for {db_name}.{table_name}")
        return True, f"{db_name}.{table_name}", None
    else:
        print(f"Update failure for {db_name}.{table_name}")
        return False, f"{db_name}.{table_name}", up_response["Error"]
