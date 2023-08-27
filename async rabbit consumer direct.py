import asyncio

import aio_pika
from aio_pika.abc import AbstractRobustConnection
from aio_pika.pool import Pool


async def main() -> None:
    loop = asyncio.get_event_loop()

    async def get_connection() -> AbstractRobustConnection:
        return await aio_pika.connect_robust("amqp://guest:guest@localhost/")

    connection_pool: Pool = Pool(get_connection, max_size=2, loop=loop)

    async def get_channel() -> aio_pika.Channel:
        async with connection_pool.acquire() as connection:
            return await connection.channel()

    channel_pool: Pool = Pool(get_channel, max_size=10, loop=loop)
    queue_name = "error"

    async def consume() -> None:
        async with channel_pool.acquire() as channel:  # type: aio_pika.Channel
            await channel.set_qos(10)
            ######### SIMPLE QUEUE
            queue = await channel.declare_queue(
                queue_name, durable=False, auto_delete=False,
            )

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    print(message.body.decode())
                    await message.ack()

    async with connection_pool, channel_pool:
        task = loop.create_task(consume())
        await task


if __name__ == "__main__":
    asyncio.run(main())
