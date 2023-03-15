/* @blast
name: some-sql-task
description: some description goes here
type: bq.sql
depends:
  - task1
  - task2
  - task3
  - task4
  - task5
  - task3
parameters:
    param1: first-parameter
    param2: second-parameter
    s3_file_path: s3://bucket/path
connections:
    conn1: first-connection
    conn2: second-connection
materialization:
    type: table
    partition_by: dt
    cluster_by:
        - event_name
    strategy: delete+insert
    incremental_key: dt

@blast */

select *
from foo;
