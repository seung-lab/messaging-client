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

    def consume(self, queue: str, callback: Callable = None, max_messages: int = 1):
        if callback is None:
            callback = _print

        def _print(payload):
            print(payload.data)

        def callback_wrapper(payload):
            """Call user callback and send acknowledge."""
            callback(payload)
            payload.ack()

        flow_control = pubsub_v1.types.FlowControl(max_messages=max_messages)
        subscription_name = f"projects/{PROJECT_NAME}/subscriptions/{queue}"
        with pubsub_v1.SubscriberClient() as subscriber:
            future = subscriber.subscribe(
                subscription_name, callback_wrapper, flow_control=flow_control
            )
            try:
                future.result()
            except Exception:
                # terminate on any exception so that the worker isn't hung.
                future.cancel()
                print("stopped listening")
