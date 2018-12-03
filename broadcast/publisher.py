import json
import pika
from broadcast import settings


def comparator_message(message):
    j = json.loads(message)
    j['target'] = 'COMPARATOR'
    out_message = json.dumps(j)
    return out_message


def etalon_message(message):
    j = json.loads(message)
    j['target'] = 'ETALON'
    out_message = json.dumps(j)
    return out_message


def create_out_message_for_app(app, message):
    # create outgoing message for specific app
    if app == 'COMPARATOR':
        return comparator_message(message)
    elif app == 'ETALON':
        return etalon_message(message)
    return message


class ExchangeNotDefined(Exception):
    pass


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(
                *args, **kwargs)
        return cls._instances[cls]


class Publisher(metaclass=Singleton):
    def __init__(self):
        print('Creating MQ connection...')
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters())
        self.channel = self.connection.channel()

        # Define exchange for broadcasting
        if settings.EXCHANGE:
            self.channel.exchange_declare(
                exchange=settings.EXCHANGE, exchange_type="fanout")

        for out_queue in settings.OUT_QUEUES.values():
            # Create output queues
            self.channel.queue_declare(queue=out_queue)

            # Bind queues to exchange for broadcasting
            if settings.EXCHANGE:
                self.channel.queue_bind(
                    exchange=settings.EXCHANGE, queue=out_queue)

    def publish(self, message: str):
        for app, out_queue in settings.OUT_QUEUES.items():

            msg = create_out_message_for_app(app, message)
            print(f'Sent message to {app}: {msg}')

            self.channel.basic_publish(
                exchange='', routing_key=out_queue, body=msg)

    def broadcast(self, message: str):
        if settings.EXCHANGE:
            print(f'Broadcast message: {message}')
            self.channel.basic_publish(
                exchange=settings.EXCHANGE, routing_key='', body=message)
        else:
            raise ExchangeNotDefined()
