-- @blast.name: some-sql-task
-- @blast.description: some description goes here
-- @blast.type: bq.sql
-- @blast.depends: task1, task2
-- @blast.depends: task3,task4
-- @blast.depends: task5, task3
-- @blast.parameters.param1: first-parameter
-- @blast.parameters.param2: second-parameter
-- @blast.connection: conn2

select *
from foo;
