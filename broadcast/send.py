import json
# from time import sleep
import pika
from broadcast import settings

queue = settings.IN_QUEUE


for i in range(10):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queue)

    j = {"id": f"{i}", "msg": f"Message {i}"}
    msg = json.dumps(j)
    channel.basic_publish(exchange='', routing_key=queue, body=msg)
    print(f" [x] Sent {msg}")
    # sleep(1)
