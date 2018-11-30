import asyncio
import json

import aio_pika

from broadcast import settings


async def broadcast(in_queue, out_queues, loop):
    print(f'receiver: {in_queue}')

    connection = await aio_pika.connect_robust(
        settings.CONNECTION, loop=loop)

    async with connection:
        channel = await connection.channel()

        # create in/out queues
        queue = await channel.declare_queue(in_queue)
        for out_queue in out_queues:
            await channel.declare_queue(out_queue)

        # read messages from in_queue
        async for message in queue:
            with message.process():
                print(message.body)
                j = json.loads(message.body)
                num = int(j["id"])

                # send messages to out_queues
                await channel.default_exchange.publish(
                    aio_pika.Message(message.body),
                    routing_key=out_queues[num % 2])

                if in_queue in message.body.decode():
                    break


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        broadcast(settings.IN_QUEUE, settings.OUT_QUEUES, loop))
    loop.close()


if __name__ == '__main__':
    main()
