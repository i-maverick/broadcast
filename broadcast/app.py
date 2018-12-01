import asyncio
import json

import aio_pika

from broadcast import settings


async def send(channel, out_queue, message):
    # send messages to out_queue
    await channel.default_exchange.publish(
        aio_pika.Message(message.encode()),
        routing_key=out_queue)


async def create_out_message_for_app(app, message):
    # create outgoing message for specific app
    out_message = json.loads(message)
    out_message['target'] = app
    return json.dumps(out_message)


async def generate_out_messages(message):
    # generate output messages for different queues
    out_messages = dict()
    for app in settings.OUT_QUEUES:
        out_message = await create_out_message_for_app(app, message)
        out_queue = settings.OUT_QUEUES[app]
        out_messages[out_queue] = out_message
    return out_messages


async def save_message_to_db(message):
    # save message to database
    pass


async def broadcast(in_queue, out_queues, loop):
    connection = await aio_pika.connect_robust(
        settings.CONNECTION, loop=loop)

    async with connection:
        channel = await connection.channel()

        # create outgoing queues
        for out_queue in out_queues.values():
            await channel.declare_queue(out_queue)

            # create incoming queue
            queue = await channel.declare_queue(in_queue)
            print(f'receiver: {in_queue}')

            # read messages from in_queue
            async for message in queue:
                with message.process():
                    print(message.body)

                    await save_message_to_db(message.body)

                    out_messages = await generate_out_messages(message.body)
                    for out_q, msg in out_messages.items():
                        await send(channel, out_q, msg)

                    if in_queue in message.body.decode():
                        break


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        broadcast(settings.IN_QUEUE, settings.OUT_QUEUES, loop))
    loop.close()


if __name__ == '__main__':
    main()
