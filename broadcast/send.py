import json
from time import sleep
import pika
from broadcast import settings


for id in range(10):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=settings.IN_QUEUE)

    j = {"id": f"{id}", "msg": f"Message for {id}"}
    msg = json.dumps(j)
    channel.basic_publish(exchange='', routing_key=settings.IN_QUEUE, body=msg)
    print(f" [x] Sent {msg}")
    sleep(1)
