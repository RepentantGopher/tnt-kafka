from aiokafka import AIOKafkaProducer
import asyncio
import tarantool
import socket


def test_consumer():
    message1 = {
        "key": "test1",
        "value": "test1"
    }

    message2 = {
        "key": "test1",
        "value": "test2"
    }

    message3 = {
        "key": "test1",
        "value": "test3"
    }

    loop = asyncio.get_event_loop()

    async def send():
        producer = AIOKafkaProducer(
            loop=loop, bootstrap_servers='localhost:9092')
        # Get cluster layout and initial topic/partition leadership information
        await producer.start()
        try:
            # Produce message
            for msg in (message1, message2, message3):
                await producer.send_and_wait(
                    "test_consumer",
                    value=msg['value'].encode('utf-8'),
                    key=msg['key'].encode('utf-8')
                )

        finally:
            # Wait for all pending messages to be delivered or expire.
            await producer.stop()

    loop.run_until_complete(send())

    server = tarantool.Connection("127.0.0.1", 3301,
                                  user="guest",
                                  password=None,
                                  socket_timeout=20,
                                  reconnect_max_attempts=3,
                                  reconnect_delay=1,
                                  connect_now=True)

    attempts = 0
    while True:
        try:
            response = server.call("consumer.consume", ())
        # tarantool in docker sometimes stacks
        except:
            attempts += 1
            if attempts < 3:
                continue
            else:
                assert True is False
        else:
            break

    assert set(*response) == {
        "test1",
        "test2",
        "test3"
    }
