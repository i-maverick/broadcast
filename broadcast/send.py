import json
from time import sleep
import pika
from broadcast import settings

queue = settings.IN_QUEUE


for id in range(20):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queue)

    j = {"id": f"{id}", "msg": f"Message {id}"}
    msg = json.dumps(j)
    channel.basic_publish(exchange='', routing_key=queue, body=msg)
    print(f" [x] Sent {msg}")
    sleep(1)
