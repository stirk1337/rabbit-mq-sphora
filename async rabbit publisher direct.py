import asyncio

import aio_pika
from aio_pika.abc import AbstractRobustConnection
from aio_pika.pool import Pool
from aio_pika import ExchangeType


async def main() -> None:
    loop = asyncio.get_event_loop()

    async def get_connection() -> AbstractRobustConnection:
        return await aio_pika.connect_robust("amqp://guest:guest@localhost/")

    connection_pool: Pool = Pool(get_connection, max_size=2, loop=loop)

    async def get_channel() -> aio_pika.Channel:
        async with connection_pool.acquire() as connection:
            return await connection.channel()

    channel_pool: Pool = Pool(get_channel, max_size=10, loop=loop)
    # queue_name = "pool_queue"

    async def publish() -> None:
        async with channel_pool.acquire() as channel:  # type: aio_pika.Channel
            # await channel.default_exchange.publish(
            #     aio_pika.Message(("Channel: %r" % channel).encode()),
            #     queue_name,
            # )
            exchange = await channel.declare_exchange(name='direct_xd', type=ExchangeType.DIRECT)
            queue = await channel.declare_queue(
                'error', durable=False, auto_delete=False,
            )
            await queue.bind(exchange='direct_xd', routing_key='error')
            await exchange.publish(message=aio_pika.Message('xd'.encode()), routing_key='error')
            print('Published message')

    async with connection_pool, channel_pool:
        async with asyncio.TaskGroup() as tg:
            for _ in range(50):
                tg.create_task(publish())



if __name__ == "__main__":
    asyncio.run(main())
