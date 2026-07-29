"""Tests for MessagingClientConsumer.consume_bounded (bounded/idle-exit pull consumer)."""

import os
import signal
from unittest.mock import MagicMock, patch

from messagingclient.client import MessagingClientConsumer


class FakeMessage:
    def __init__(self, data=b"payload", attributes=None):
        self.data = data
        self.attributes = attributes if attributes is not None else {}


class FakeReceived:
    def __init__(self, message, ack_id):
        self.message = message
        self.ack_id = ack_id


class FakePullResponse:
    def __init__(self, received_messages):
        self.received_messages = received_messages


def _patched_subscriber(pull_side_effect):
    """Return (patcher, subscriber_mock) where SubscriberClient() context-manages the mock."""
    subscriber = MagicMock()
    subscriber.pull.side_effect = pull_side_effect
    client_cls = MagicMock()
    client_cls.return_value.__enter__.return_value = subscriber
    patcher = patch("messagingclient.client.pubsub_v1.SubscriberClient", client_cls)
    return patcher, subscriber


def test_processes_exactly_n_then_returns():
    # Always a message available; bounded by max_messages.
    def pull(request, retry):
        return FakePullResponse([FakeReceived(FakeMessage(), "ack")])

    patcher, subscriber = _patched_subscriber(pull)
    calls = []
    with patcher:
        processed = MessagingClientConsumer().consume_bounded(
            "q", callback=lambda m: calls.append(m), message_limit=3
        )

    assert processed == 3
    assert len(calls) == 3
    assert subscriber.acknowledge.call_count == 3
    # subscription name is injected into the message attributes for the callback
    assert calls[0].attributes["__subscription_name"].endswith("/subscriptions/q")


def test_idle_timeout_returns_zero_on_empty_queue():
    def pull(request, retry):
        return FakePullResponse([])  # queue always empty

    patcher, subscriber = _patched_subscriber(pull)
    calls = []
    with patcher:
        processed = MessagingClientConsumer().consume_bounded(
            "q", callback=lambda m: calls.append(m), message_limit=0, idle_timeout=0.05
        )

    assert processed == 0
    assert calls == []
    subscriber.acknowledge.assert_not_called()


def test_sigterm_breaks_after_current_message():
    # A few messages then empty (bounded so the test can't hang if SIGTERM is ignored).
    remaining = [FakeReceived(FakeMessage(), f"ack{i}") for i in range(3)]

    def pull(request, retry):
        if remaining:
            return FakePullResponse([remaining.pop(0)])
        return FakePullResponse([])

    patcher, subscriber = _patched_subscriber(pull)
    calls = []

    def callback(msg):
        calls.append(msg)
        os.kill(os.getpid(), signal.SIGTERM)  # ask the worker to stop

    with patcher:
        processed = MessagingClientConsumer().consume_bounded(
            "q", callback=callback, message_limit=0, idle_timeout=0.5
        )

    # It finished the in-flight message and then exited (did not drain all 3).
    assert processed == 1
    assert len(calls) == 1
    assert subscriber.acknowledge.call_count == 1


def test_multiple_queues_round_robin_with_message_limit():
    # One subscriber per queue; round-robins across both. message_limit is total.
    subscribers = {}

    def make_pull(name):
        def pull(request, retry):
            return FakePullResponse([FakeReceived(FakeMessage(), f"{name}-ack")])
        return pull

    # Patch SubscriberClient so each constructed client is a distinct mock whose
    # __enter__ returns itself; ExitStack.enter_context(client) -> client.
    created = []

    def new_client():
        sub = MagicMock()
        sub.__enter__.return_value = sub
        idx = len(created)
        sub.pull.side_effect = make_pull(f"q{idx}")
        created.append(sub)
        return sub

    with patch("messagingclient.client.pubsub_v1.SubscriberClient", side_effect=new_client):
        calls = []
        processed = MessagingClientConsumer().consume_bounded(
            ["qa", "qb"], callback=lambda m: calls.append(m), message_limit=4
        )

    assert processed == 4
    assert len(calls) == 4
    assert len(created) == 2  # one subscriber per queue
    # round-robin: work split across both subscribers
    assert created[0].acknowledge.call_count == 2
    assert created[1].acknowledge.call_count == 2
    # subscription names carry through to each message
    subs = {m.attributes["__subscription_name"] for m in calls}
    assert subs == {
        "projects/neuromancer-seung-import/subscriptions/qa",
        "projects/neuromancer-seung-import/subscriptions/qb",
    }


def test_callback_exception_does_not_ack_and_exits():
    def pull(request, retry):
        return FakePullResponse([FakeReceived(FakeMessage(), "ack")])

    patcher, subscriber = _patched_subscriber(pull)

    def callback(_msg):
        raise RuntimeError("boom")

    with patcher:
        processed = MessagingClientConsumer().consume_bounded(
            "q", callback=callback, message_limit=5
        )

    assert processed == 0
    subscriber.acknowledge.assert_not_called()  # unacked -> Pub/Sub redelivers
