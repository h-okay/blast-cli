-- @blast.name: some-sql-task
-- @blast.description: some description goes here
-- @blast.type: bq.sql
-- @blast.depends: task1, task2
-- @blast.depends: task3,task4
-- @blast.depends: task5, task3
-- @blast.parameters.param1: first-parameter
-- @blast.parameters.param2: second-parameter
-- @blast.parameters.s3_file_path: s3://bucket/path
-- @blast.connection: conn2
-- @blast.materialization.type: table
-- @blast.materialization.partition_by: dt
-- @blast.materialization.cluster_by: event_name
-- @blast.materialization.strategy: delete+insert
-- @blast.materialization.incremental_key: dt

select *
from foo;
