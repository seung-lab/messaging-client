import logging
import time
from typing import Any
from typing import Union, List
from typing import Callable
from os import getenv

from google.api_core import retry
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
            logging.info(
                f"MessagingClient.consume().callback_wrapper() Received message: {payload}."
            )
            payload.message.attributes["__subscription_name"] = queue
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
            except Exception as exc:
                # terminate on any exception so that the worker isn't hung.
                future.cancel()
                logging.info(
                    f"MessagingClient.consume() Exception (will stop listening now): {exc}"
                )

    def _consume_round_robin(
        self,
        subscribers: List[pubsub_v1.SubscriberClient],
        subscription_names: List[str],
        callback: Callable,
    ):
        queue_index = 0
        quit = False
        while not quit:
            subscriber, subscription_name = (
                subscribers[queue_index],
                subscription_names[queue_index],
            )
            queue_index = (queue_index + 1) % len(subscribers)

            response = subscriber.pull(
                request={"subscription": subscription_name, "max_messages": 1},
                retry=retry.Retry(deadline=300),
            )
            if len(response.received_messages) == 0:
                # TODO: Do I need to add a brief sleep here before looping around and trying again
                # or is it relatively self-throttling, say via the retry/deadline arguments to subscriber.pull()?
                time.sleep(0.1)
                continue

            ack_ids = []
            # This for-loop is overkill. There really should only be only one message,
            # as per the request.max_messages argument to subscriber.pull().
            for received_message in response.received_messages:
                try:
                    logging.info(
                        f"MessagingClient.consume_multiple() Received message on subscription '{subscription_name}': {received_message.message.data}."
                    )
                    received_message.message.attributes["__subscription_name"] = subscription_name
                    callback(received_message.message)
                    ack_ids.append(received_message.ack_id)
                except Exception as exc:
                    # terminate on any exception so that the worker isn't hung.
                    logging.info(
                        f"MessagingClient.consume_multiple() Exception (will stop listening now): {exc}"
                    )
                    quit = True
                    break
            if quit:
                break

            subscriber.acknowledge(
                request={"subscription": subscription_name, "ack_ids": ack_ids}
            )

    def consume_multiple(
        self, queues: Union[str, List], callback: Callable = None, max_messages: int = 1
    ):
        """
        TODO: This function can completely replace consume() (i.e. be renamed consume(), deleting the older version),
        but for development, I left the original consume() untouched and added this as a new function instead.
        To avoid altering any code that currently calls consume(), the existing version could eventually be deleted
        and this version could be renamed in its place.

        If one queue is provided, it will be consumed until the process is terminated.
        If multiple queues are provided, they will be consumed in round robin fashion.

        Refs:
        https://cloud.google.com/pubsub/docs/pull
        """

        if isinstance(queues, str):
            return self.consume(queues, callback, max_messages)
        if len(queues) == 1:
            return self.consume(queues[0], callback, max_messages)

        if callback is None:
            callback = _print

        def _print(payload):
            print(payload.data)

        subscription_names = [
            f"projects/{PROJECT_NAME}/subscriptions/{queue}" for queue in queues
        ]
        subscribers = [pubsub_v1.SubscriberClient() for _ in subscription_names]

        try:
            self._consume_round_robin(subscribers, subscription_names, callback)
        except Exception as exc:
            logging.info(f"stopped listening: {exc}")
