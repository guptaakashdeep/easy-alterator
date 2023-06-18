# EASY ALTERATOR
- This utility is developed to update the schema for `EXTERNAL` `PARQUET` format v1 tables (i.e. non open source format tables like delta, iceberg etc.)  in your data pipelines that uses `AWS Glue Catalog` as hive metastore for your spark jobs.
- This utility will help in saving time by manually comparing the table schemas and need of backfill your table data incase if there is any deletion and addition of column. [Check FAQ for more details.]
- This utility compares the `hql`/`DDL` provided for the table with the already existing ones, identified the newly added/removed columns along with data type update for any column if there is any, and updates the table schema in AWS Glue catalog automatically.
- Incase of **data type mismatch** with the already existing tables, at present at the time of writing this, it skips the table update and mentions the same as the reason of skipping table. [upcoming version: will include data type compatibility of existing and provided data type and update action will be taken accordingly.]

## Checks before updating a table
There are some checks implemented in place that checks if the table being updated safely and won't break any query engine which is used to query the data after the udate. Below are the mentioned checks that are in place:
1. DDL Statement is a `CREATE` table statement.
2. Table is an `EXTERNAL` table at both the places i.e. in provided DDL file and in glue catalog.
3. Table is a `PARQUET` table as per the provided DDL and in glue catalog.
4. `Partition Columns` are not changed.

## Easy Alterator Functionality
Idea behind creating Easy Alterator is to provide ability to ADD/DROP/REPLACE/CHANGE column for v1 parquet external table so it can be achieved without dropping and backfilling your table.

| Use Case | Spark 2.4 | Spak 3.x | Easy Alterator | Additional Comments
| --- | ----------- | -------- | ------- | ------- | 
| Adding Columns into table | using `ALTER TABLE ADD COLUMN` | using `ALTER TABLE ADD COLUMN` | Automatically detects from provided DDL and updates the Glue catalog schema for table |
| `ALTER TABLE REPLACE/DROP/CHANGE COLUMN` command | Not Supported | Works only for `v2 tables` (i.e. open source format tables like delta, iceberg, hudi) | Provides ability for v1 `EXTERNAL` `PARQUET` tables. |
|Adding new partitions in non-partitioned table|Not Supported|Not Supported| Not Supported | Requires backfill of data after partitions columns are added in schema
|Adding a new partition column|Not Supported|Not Supported| Not Supported | Requires backfill of data after partitions columns are added in schema
|Replacing a partition column with new partition|Not Supported|Not Supported|Not Supported|Requires backfill of data after partitions columns are added in schema


----
# Usage

```bash
python3 easy_alterator.py -p /home/hadoop/ddl/,/home/hadoop/hql/xyz.hql 
```

## Pre-requisites
The only pre-requisite is to provide the DDL file and config yaml file in a specific format.

### DDL Format
It's case-insensitive. It should follow the format of 
1. keeping the `table name` and `column name` within ticks (`)
2. TABLE FORMAT can be mentioned using `STORED AS PARQUET` or combination of `ROW FORMAT SERDE`, `INPUTFORMAT` and `OUTPUTFORMAT`
```
CREATE EXTERNAL TABLE IF EXISTS `db_name.table_name`(
    `column1` string,
    `column2` double,
    `column3` bigint,
    ....
)
PARTITIONED BY (
    `column5` date,
    `column6` int,
    ....
)
STORED AS PARQUET
or
ROW FORMAT SERDE org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe,
INPUTFORMAT org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat,
OUTPUTFORMAT org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat,
LOCATION "s3://<bucket>/<key>";
```

### YAML CONFIG
YAML configuration file format:
1. if `--path` is defined with the path for the table DDLs, 
```
path: <s3-path> or <local-fs like emr local>
tables:
    - table1
    - table2
    - table3
    - table4
```

## Parameters
| Parameters | Usage | Is Mandatory ? | Any Additional condition | 
|----------|--------|------|-------|
|`-h or --help` | To get the details of all the parameters | No |
|`-p or --path`| To provide the paths to DDL files folder separated by `comma (,)` or Path to DDL files separated by `comma (,)`.|Yes if `-c` is not used.|
|`-c or --config`|DDL config yaml|Yes if `-p` is not used| In case of selective multiple tables needs to be updated, all such table names can be provided as a `YAML` config file.
|`-cp or --key_for_path`|Key in DDL config file for reading path to DDL folder.|Yes if `-c` is used and `-p` is not used| In case of `--config` is used, it is required to provide the path of the folder from where these table DDL's need to be read, this path can either be provided via `--path or -p` or a key in `YAML` config file. In later case that key needs to be provided as `-cp <key-name>`
|`-fs or --file_suffix`|Suffix for DDL files to be picked from path. Default is `hql`|No| For filtering type of DDL files based on prefix. Possible values: `hql`, `txt`
|`-fp or --file_prefix`|Prefix for DDL files to be picked from path.|No| For filtering type of DDL files based on suffix.

# FAQs

1. Why not just use `ALTER TABLE REPLACE/DROP/CHANGE COLUMN` statements directly instead of this utility ?
- ALTER TABLE REPLACE/DROP/CHANGE column works only for `v2 tables` (i.e. open source table format tables.)

2. Why it only works for `PARQUET` tables ?
- By default `parquet` tables data mapping is name to name mapping and not index based, so when you add or drop a column the sequence of data being read/displayed from table is independent of the sequence. As of now this utility supports only PARQUET Tables.

3. Why can't the same thing be done for `INTERNAL` tables ?
- While reading from Internal table, Spark doesn't compare the schema that is being read from the file with the once in metastore. So while reading from table into spark dataframe it will still read all the columns that are present in the data even if those are not present in the metastore. This behavior of Spark can easily be seen by using `.explain` command while reading an INTERNAL and an EXTERNAL table.
