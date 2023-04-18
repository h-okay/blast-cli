# @blast.name: some-python-task
# @blast.description: some description goes here
# @blast.type: bq.sql
# @blast.depends: task1, task2
# @blast.depends: task3,task4
# @blast.depends: task5, task3
# @blast.parameters.param1: first-parameter
# @blast.parameters.param2: second-parameter
# @blast.parameters.param3: third-parameter
# @blast.connection: conn1
# @blast.schedule.days: SUNDAY, MONDAY
# @blast.schedule.days: TUESDAY


print('hello world')
