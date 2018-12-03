import pika
from broadcast import settings

queue = settings.OUT_QUEUES['ETALON']

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue=queue)


def callback(ch, method, properties, body):
    print(f' [x] Received {body}')


channel.basic_consume(callback, queue=queue, no_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
