import json
from time import sleep
from broadcast.publisher import Publisher

publisher = Publisher()
for i in range(10):
    j = {"id": f"{i}", "msg": f"Message {i}"}
    msg = json.dumps(j)
    publisher.publish(msg)
    sleep(1)

publisher = Publisher()
for i in range(10):
    j = {"id": f"{i}", "msg": f"Another message {i}"}
    msg = json.dumps(j)
    publisher.broadcast(msg)
    sleep(1)
