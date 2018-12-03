import pika
import sys

message = ' '.join(sys.argv[1:]) or "Hello World!"


def producer():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare('queue')
    channel.basic_publish(exchange='', routing_key='queue', body=message,
                          properties=pika.BasicProperties(
                              delivery_mode=2,  # make message persistent
                          ))
    connection.close()


producer()
