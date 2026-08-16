"""RetryableError returns a message to the subscription without stopping the consumer.

The loop acks iff the callback returns. Before RetryableError existed a callback had only two
options: return (ack, losing the work) or raise (the worker stops consuming entirely). Callbacks
facing transient downstream failures therefore swallowed their errors and dropped messages.
"""

from unittest import mock

import pytest

from messagingclient import RetryableError
from messagingclient.client import MessagingClientConsumer


class FakeMessage:
    def __init__(self, data=b""):
        self.data = data
        self.attributes = {}


class FakeReceived:
    def __init__(self, ack_id, data=b""):
        self.ack_id = ack_id
        self.message = FakeMessage(data)


class FakeResponse:
    def __init__(self, received):
        self.received_messages = received


class FakeSubscriber:
    """Serves a fixed script of pulls, recording acks and nacks."""

    def __init__(self, script):
        self._script = list(script)
        self.acked = []
        self.nacked = []

    def pull(self, request=None, retry=None, **kw):
        if self._script:
            return FakeResponse(self._script.pop(0))
        return FakeResponse([])

    def acknowledge(self, request=None, **kw):
        self.acked.extend(request["ack_ids"])

    def modify_ack_deadline(self, request=None, **kw):
        assert request["ack_deadline_seconds"] == 0, request
        self.nacked.extend(request["ack_ids"])


def _run(subscriber, callback, message_limit=0, idle_timeout=0.3):
    """Drive the loop over a scripted subscriber.

    idle_timeout is the exit condition, not message_limit: a nacked message is never acked, so
    `processed` does not advance and a message_limit alone would spin forever once the script is
    exhausted. Real (short) sleeps are used so the idle clock actually advances.
    """
    consumer = MessagingClientConsumer.__new__(MessagingClientConsumer)
    return consumer._consume_round_robin(
        [subscriber], ["projects/p/subscriptions/s"], callback,
        message_limit=message_limit, idle_timeout=idle_timeout,
    )


class TestRetryableError:
    def test_retryable_nacks_and_does_not_ack(self):
        sub = FakeSubscriber([[FakeReceived("ack-1")]])

        def cb(_msg):
            raise RetryableError("HTTP 429")

        _run(sub, cb)

        assert sub.nacked == ["ack-1"], "message should be returned for redelivery"
        assert sub.acked == [], "a failed message must not be acked"

    def test_consumer_keeps_going_after_a_retryable_failure(self):
        """The whole point: one 429 must not take the worker out of service."""
        sub = FakeSubscriber([[FakeReceived("ack-1")], [FakeReceived("ack-2")]])
        seen = []

        def cb(msg):
            seen.append(len(seen))
            if len(seen) == 1:
                raise RetryableError("HTTP 429")

        _run(sub, cb)

        assert len(seen) == 2, "consumer stopped after the retryable failure"
        assert sub.nacked == ["ack-1"]
        assert sub.acked == ["ack-2"], "the following message should still be acked"

    def test_success_acks_as_before(self):
        sub = FakeSubscriber([[FakeReceived("ack-1")]])

        _run(sub, lambda _m: None)

        assert sub.acked == ["ack-1"]
        assert sub.nacked == []

    def test_non_retryable_still_stops_and_does_not_ack(self):
        """Unchanged fatal path: stop consuming, leave the message unacked."""
        sub = FakeSubscriber([[FakeReceived("ack-1")], [FakeReceived("ack-2")]])
        seen = []

        def cb(_msg):
            seen.append(1)
            raise ValueError("structural")

        _run(sub, cb)

        assert len(seen) == 1, "should have stopped after the fatal error"
        assert sub.acked == []
        assert sub.nacked == []

    def test_a_failing_nack_does_not_break_the_loop(self):
        """If the nack RPC fails the message still is not acked; we just wait out the deadline."""
        sub = FakeSubscriber([[FakeReceived("ack-1")], [FakeReceived("ack-2")]])
        sub.modify_ack_deadline = mock.Mock(side_effect=RuntimeError("transport down"))
        seen = []

        def cb(_msg):
            seen.append(len(seen))
            if len(seen) == 1:
                raise RetryableError("HTTP 503")

        _run(sub, cb)

        assert len(seen) == 2, "a failed nack must not stop the consumer"
        assert sub.acked == ["ack-2"]
