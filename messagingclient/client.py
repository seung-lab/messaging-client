from typing import Any
from typing import Union
from typing import Callable
from os import getenv

from google.cloud import pubsub_v1

PROJECT_NAME = getenv("PROJECT_NAME", "neuromancer-seung-import")


class MessagingClient:
    def __init__(self):
        pass

    def publish(
        self,
        exchange: str,
        payload: bytes,
        attributes: dict = {},
        timeout: Union[int, float] = None,
    ) -> Any:
        publisher = pubsub_v1.PublisherClient()
        topic_name = f"projects/{PROJECT_NAME}/topics/{exchange}"
        future = publisher.publish(topic_name, payload, **attributes)
        return future.result(timeout=timeout)

    def consume(self, queue: str, callback: Callable = None):
        if callback is None:
            callback = _print

        def _print(payload):
            print(payload.data)

        def callback_wrapper(payload):
            """Call user callback and send acknowledge."""
            callback(payload)
            payload.ack()

        subscription_name = f"projects/{PROJECT_NAME}/subscriptions/{queue}"
        with pubsub_v1.SubscriberClient() as subscriber:
            future = subscriber.subscribe(subscription_name, callback_wrapper)
            try:
                future.result()
            except KeyboardInterrupt:
                future.cancel()
                print("stopped listening")
