from concurrent import futures
import logging
import time
from typing import Any
from typing import Union, List
from typing import Callable
from os import getenv

from google.api_core import retry
from google.cloud import pubsub_v1

PROJECT_NAME = getenv("PROJECT_NAME", "neuromancer-seung-import")


class MessagingClientPublisher:
    """
    This class is a wrapper around the Google Cloud Pub/Sub PublisherClient.
    It allows for both single and batch message publishing.
    The batch publisher is used by default, but if the batch size is set to 1,
    the single message publisher is used instead, which avoids the futures complexity
    and perfectly replicates the older single-message design.
    """

    class MessagingClientSinglePublisher:
        """
        Strictly speaking, this class is unnecessary since the batch publisher can be used for single messages,
        but it is used by the batch publisher to perfectly replicate the older single-message design.
        """
        def publish(
            self,
            exchange: str,
            payload: bytes,
            attributes: dict = {},
            timeout: Union[int, float] = None,
        ) -> Any:
            logging.info(
                f"MessagingClientPublisher.MessagingClientSinglePublisher.publish() Publishing message to exchange '{exchange}' with attributes {attributes}."
            )
            publisher = pubsub_v1.PublisherClient()
            topic_name = f"projects/{PROJECT_NAME}/topics/{exchange}"
            future = publisher.publish(topic_name, payload, **attributes)
            return future.result(timeout=timeout)

    def __init__(self, batch_size: int=1):
        """
        Args:
            batch_size (int, optional): Number of messages to send in a single batch. Defaults to 1.
        Ref:
            https://cloud.google.com/pubsub/docs/batch-messaging
        """
        if not isinstance(batch_size, int) or batch_size < 0:
            raise ValueError(
                f"Invalid batch size: {batch_size}. Must be an int >= 0 (where 0 implies no batch handling at all)."
            )
        elif batch_size == 0:
            # Single-message publishing could readily be supported by the batch publisher,
            # but it is a bit more efficient to use the single message publisher since it avoids all the futures complexity,
            # and furthermore enhances backward compatilibity by perfectly preserving the original design.
            self.publisher = MessagingClientPublisher.MessagingClientSinglePublisher()
            logging.info(
                "MessagingClientPublisher.__init__() Initialized single-message publisher."
            )
        elif batch_size >= 1:
            # We are in a multi-message, batch-publishing scenario
            self.batch_settings = pubsub_v1.types.BatchSettings(
                max_messages=batch_size,  # Messages, default 100
                max_bytes=1024,  # Bytes, default 1 MB (1000000)
                max_latency=1,  # Seconds, default 10 ms (.01)
            )
            self.publisher = pubsub_v1.PublisherClient(self.batch_settings)
            self.publish_future_timeouts = {}
            logging.info(
                f"MessagingClientPublisher.__init__() Initialized batch-message publisher for batches of size {batch_size}."
            )
    
    def callback(self, future):
        """Callback to handle the result of the publish operation."""
        try:
            return future.result(timeout=self.publish_future_timeouts[future])
        except Exception as exc:
            logging.error(f"MessagingClient.callback() Publishing message failed: {exc}")
        logging.info("MessagingClient.callback() Message published successfully.")
        return None

    def close(self, timeout: Union[int, float]=None):
        """
        Wait for all publish futures to complete.
        """
        if not self.publisher:
            logging.warning("MessagingClientPublisher.close() Publisher client has already been closed.")
            return
        
        if isinstance(self.publisher, MessagingClientPublisher.MessagingClientSinglePublisher):
            # If the publisher is a single-message publisher, there is nothing to close
            self.publisher = None
            return
        
        if self.publish_future_timeouts:
            logging.info("MessagingClientPublisher.close() Waiting for publish futures to complete...")
            futures.wait(
                [future for future in self.publish_future_timeouts],
                return_when=futures.ALL_COMPLETED, timeout=timeout)
            for future in self.publish_future_timeouts:
                try:
                    future.result(timeout=timeout)
                except Exception as exc:
                    logging.error(f"MessagingClientPublisher.close() Publishing message failed: {exc}")
                else:
                    logging.info("MessagingClientPublisher.close() Message published successfully.")
            self.publish_future_timeouts = {}
        
        self.publisher = None

    def publish(
        self,
        exchange: str,
        payload: bytes,
        attributes: dict={},
        timeout: Union[int, float]=None,
    ) -> Any:
        logging.info(
            f"MessagingClientPublisher.publish() Publishing message to exchange '{exchange}' with attributes {attributes}."
        )
        
        if isinstance(self.publisher, MessagingClientPublisher.MessagingClientSinglePublisher):
            # If the publisher is a single-message publisher, fall through to the older design
            return self.publisher.publish(exchange, payload, attributes, timeout)

        # We are in a multi-message, batch-publishing scenario
        if not self.publisher:
            # If the publisher was previously closed, recreate it.
            # This is an unlikely use case, as it shouldn't have been closed until it was no longer needed,
            # but there is no harm in supporting such use if it arises.
            logging.warning("MessagingClientPublisher.publish() Publisher client was previously closed. It will be recreated now.")
            self.publisher = pubsub_v1.PublisherClient(self.batch_settings)
            self.publish_future_timeouts = {}
        
        topic_name = f"projects/{PROJECT_NAME}/topics/{exchange}"
        future = self.publisher.publish(topic_name, payload, **attributes)
        future.add_done_callback(self.callback)
        self.publish_future_timeouts[future] = timeout

class MessagingClientConsumer:
    def consume(self, queue: str, callback: Callable=None, max_messages: int=1):
        if callback is None:
            callback = _print

        def _print(payload):
            print(payload.data)

        def callback_wrapper(payload):
            """Call user callback and send acknowledge."""
            logging.info(
                f"MessagingClientConsumer.consume().callback_wrapper() Received message: {payload}."
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
                    f"MessagingClientConsumer.consume() Exception (will stop listening now): {exc}"
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
                        f"MessagingClientConsumer._consume_round_robin() Received message on subscription '{subscription_name}': {received_message.message.data}."
                    )
                    received_message.message.attributes["__subscription_name"] = subscription_name
                    callback(received_message.message)
                    ack_ids.append(received_message.ack_id)
                except Exception as exc:
                    # terminate on any exception so that the worker isn't hung.
                    logging.info(
                        f"MessagingClientConsumer._consume_round_robin() Exception (will stop listening now): {exc}"
                    )
                    quit = True
                    break
            if quit:
                break

            subscriber.acknowledge(
                request={"subscription": subscription_name, "ack_ids": ack_ids}
            )

    def consume_multiple(
        self, queues: Union[str, List], callback: Callable = None, max_messages: int=1
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
            logging.info(f"MessagingClientConsumer.consume_multiple() stopped listening: {exc}")

class MessagingClient:
    """
    This class merely exists for back compatibility with existing code that creates a "MessagingClient" object.
    Future code should use the MessagingClientPublisher and MessagingClientConsumer classes directly.
    """

    def __init__(self):
        # Since we don't know if this client will be used as a publisher or a consumer,
        # we will lazily generate them when needed instead of wastefully front-loading both types in the ctor.
        self.publisher = None
        self.consumer = None

    def publish(
        self,
        exchange: str,
        payload: bytes,
        attributes: dict={},
        timeout: Union[int, float]=None,
    ):
        if not self.publisher:
            # Hard code the publisher to single-message mode since that is the older design
            self.publisher = MessagingClientPublisher(1)
        return self.publisher.publish(exchange, payload, attributes, timeout)

    def consume(self, queue: str, callback: Callable = None, max_messages: int=1):
        if not self.consumer:
            self.consumer = MessagingClientConsumer()
        return self.consumer.consume(queue, callback, max_messages)
    
    def consume_multiple(   
        self, queues: Union[str, List], callback: Callable = None, max_messages: int=1
    ):
        if not self.consumer:
            self.consumer = MessagingClientConsumer()
        return self.consumer.consume_multiple(queues, callback, max_messages)
