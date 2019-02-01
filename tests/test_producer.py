from aiokafka import AIOKafkaConsumer
import asyncio
import tarantool


def test_producer():
    server = tarantool.Connection(
        "127.0.0.1", 3301,
        user="guest",
        password=None,
        socket_timeout=30,
        reconnect_max_attempts=3,
        reconnect_delay=1,
        connect_now=True
    )

    server.call("producer", (
        (
            "1",
            "2",
            "3",
        ),
    ))

    loop = asyncio.get_event_loop()

    async def test():
        kafka_output = []
        async def consume():
            consumer = AIOKafkaConsumer(
                'test_producer',
                group_id="test_group",
                loop=loop,
                bootstrap_servers='localhost:9092',
                auto_offset_reset="earliest",
            )
            # Get cluster layout
            await consumer.start()

            try:
                # Consume messages
                async for msg in consumer:
                    kafka_output.append({
                        'key': msg.key if msg.key is None else msg.key.decode('utf8'),
                        'value': msg.value if msg.value is None else msg.value.decode('utf8')
                    })

            finally:
                # Will leave consumer group; perform autocommit if enabled.
                await consumer.stop()

        try:
            await asyncio.wait_for(consume(), 10, loop=loop)
        except asyncio.TimeoutError:
            pass

        assert kafka_output == [
            {
                "key": "1",
                "value": "1"
            },
            {
                "key": "2",
                "value": "2"
            },
            {
                "key": "3",
                "value": "3"
            },
        ]

    loop.run_until_complete(test())
