"""Entry point for Easy Alterator functionality."""
import argparse
import sys
from bin.process import sync_tables, alterator


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--path",
        nargs="*",
        required="--config" not in sys.argv
        and "-c" not in sys.argv
        and "--sync" not in sys.argv,
        help="Paths to DDL folder separated by space.",
    )
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        required="--path" not in sys.argv
        and "-p" not in sys.argv
        and "--sync" not in sys.argv,
        help="DDL config yaml.",
    )
    parser.add_argument(
        "-cp",
        "--key_for_path",
        type=str,
        required=("--path" not in sys.argv and "-p" not in sys.argv)
        and ("--config" in sys.argv or "-c" in sys.argv),
        help="Key in DDL config file for reading path to DDL folder.",
    )
    parser.add_argument(
        "-fs",
        "--file_suffix",
        type=str,
        required=False,
        default="hql",
        choices=["hql", "txt"],
        help="Suffix for DDL files to be picked from path.",
    )
    parser.add_argument(
        "-fp",
        "--file_prefix",
        type=str,
        required=False,
        help="Prefix for DDL files to be picked from path.",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        required=False,
        help="To check how the actual run will impact the tables. Doesn't update anything in tables.",
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        required=False,
        help="syncing schema for 2 tables.",
    )
    parser.add_argument(
        "-src",
        "--source_table",
        type=str,
        required="--sync" in sys.argv,
        help="source table for sync option. Reference table for updating target table schema.",
    )
    parser.add_argument(
        "-tgt",
        "--target_table",
        type=str,
        required="--sync" in sys.argv,
        help="target table for sync option. Table whose schema needs to be updated.",
    )
    parser.add_argument(
        "-pcheck",
        "--partition_check",
        type=int,
        required=False,
        choices=[0, 1],
        help="Specifies if partition check is required during table syncing. Used with --sync. default is 1",
    )

    # print(sys.argv)
    args = parser.parse_args()
    paths = args.path
    ddl_config_path = args.config
    path_key = args.key_for_path
    ddl_file_suffix = args.file_suffix
    ddl_file_prefix = args.file_prefix
    validate = args.validate
    # sync functionality parameters
    sync = args.sync
    src = args.source_table
    tgt = args.target_table
    part_check = args.partition_check
    if part_check is None:
        part_check = 1

    if sync:
        try:
            sync_tables(src, tgt, part_check=part_check, validate=validate)
            print("Sync completed successfully.")
            # exit and send success
            sys.exit(0)
        except Exception as ex:
            print("Error occured while running sync.")
            print(ex)
            raise ex

    try:
        response = alterator(
            paths=paths,
            ddl_config_path=ddl_config_path,
            path_key=path_key,
            ddl_file_prefix=ddl_file_prefix,
            ddl_file_suffix=ddl_file_suffix,
            validate=validate,
        )
        print(response)
    except Exception as ex:
        print("Error occured while running alterator.")
        print(ex)
        raise ex
