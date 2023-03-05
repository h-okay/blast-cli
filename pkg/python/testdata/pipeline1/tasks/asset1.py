# @blast.name: asset1
# @blast.type: python
import time

print("hello from asset1 task, will sleep for 2 seconds")

time.sleep(2)
print("done sleeping, my job here is completed, gonna throw an exception now")
raise Exception("I am a dummy exception")
