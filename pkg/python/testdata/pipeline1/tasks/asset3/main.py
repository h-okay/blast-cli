# @blast.name: asset3
# @blast.type: python
# @blast.depends: asset1

import requests
print("imported requests library")

import pandas
print("imported pandas library")

print("hello from asset2 task")

import os
print(os.environ)
# print("hello from asset2 task, will sleep for 1 second")
# time.sleep(1)
# print("done sleeping, my job here is completed")
