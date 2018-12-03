import time
import pika


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    time.sleep(body.count(b'.'))
    print(" [x] Done")


def worker():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare('queue')
    channel.basic_consume(callback, queue='queue', no_ack=True)
    channel.start_consuming()


worker()
