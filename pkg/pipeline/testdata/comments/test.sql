-- @blast.name: some-sql-task
-- @blast.description: some description goes here
-- @blast.type: bq.sql
-- @blast.depends: task1, task2
-- @blast.depends: task3,task4
-- @blast.depends: task5, task3
-- @blast.parameters.param1: first-parameter
-- @blast.parameters.param2: second-parameter
-- @blast.parameters.s3_file_path: s3://bucket/path
-- @blast.connections.conn1: first-connection
-- @blast.connections.conn2: second-connection

select *
from foo;
