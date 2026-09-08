import asyncio

import pytest

from collector.ws_client import PersistentWSClient


class FakeWebSocket:
    def __init__(self, messages, keep_alive=False):
        self._messages = messages
        self.sent = []
        self.keep_alive = keep_alive
        self.closed_by_test = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def __aiter__(self):
        return self._iter()

    async def _iter(self):
        for message in self._messages:
            yield message
        if self.keep_alive:
            while not self.closed_by_test:
                await asyncio.sleep(0)
            return
        raise ConnectionResetError("fake connection closed")

    async def send(self, message):
        self.sent.append(message)

    async def close(self):
        self.closed_by_test = True


class FakeConnector:
    def __init__(self, connections):
        self._connections = list(connections)
        self.calls = 0

    def __call__(self, url):
        ws = self._connections[self.calls]
        self.calls += 1
        return ws


async def _instant_sleep(_seconds):
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_on_message_called_for_each_message():
    received = []

    async def on_message(raw):
        received.append(raw)

    ws = FakeWebSocket(["a", "b"])
    connector = FakeConnector([ws])
    client = PersistentWSClient(
        "wss://fake", on_message=on_message, connect_fn=connector, sleep_fn=_instant_sleep
    )

    async def stop_soon():
        while len(received) < 2:
            await asyncio.sleep(0)
        client.stop()

    await asyncio.gather(client.run(), stop_soon())
    assert received == ["a", "b"]


@pytest.mark.asyncio
async def test_on_open_called_with_ws():
    opened = []

    async def on_open(ws):
        opened.append(ws)

    async def on_message(raw):
        pass

    ws = FakeWebSocket(["x"])
    connector = FakeConnector([ws])
    client = PersistentWSClient(
        "wss://fake", on_message=on_message, on_open=on_open, connect_fn=connector,
        sleep_fn=_instant_sleep,
    )

    async def stop_soon():
        while not opened:
            await asyncio.sleep(0)
        await asyncio.sleep(0)
        client.stop()

    await asyncio.gather(client.run(), stop_soon())
    assert opened == [ws]


@pytest.mark.asyncio
async def test_ping_sent_to_socket():
    ws = FakeWebSocket(["x"], keep_alive=True)
    connector = FakeConnector([ws])

    async def on_message(raw):
        pass

    client = PersistentWSClient(
        "wss://fake",
        on_message=on_message,
        connect_fn=connector,
        ping_message="PING",
        ping_interval_sec=0,
        sleep_fn=_instant_sleep,
    )

    async def stop_soon():
        for _ in range(200):
            if ws.sent:
                break
            await asyncio.sleep(0)
        ws.closed_by_test = True
        client.stop()

    await asyncio.wait_for(asyncio.gather(client.run(), stop_soon()), timeout=5)
    assert "PING" in ws.sent


@pytest.mark.asyncio
async def test_reconnects_after_disconnect_and_reports_duration():
    ws1 = FakeWebSocket(["msg1"])
    ws2 = FakeWebSocket(["msg2"])
    connector = FakeConnector([ws1, ws2])

    received = []
    disconnects = []

    async def on_message(raw):
        received.append(raw)
        if raw == "msg2":
            client.stop()

    async def on_disconnect(duration_ms, error):
        disconnects.append((duration_ms, error))

    import itertools

    clock = itertools.count(1000, 1500)

    def fake_now_ms():
        return next(clock)

    client = PersistentWSClient(
        "wss://fake",
        on_message=on_message,
        on_disconnect=on_disconnect,
        connect_fn=connector,
        sleep_fn=_instant_sleep,
        now_ms_fn=fake_now_ms,
    )

    await client.run()

    assert received == ["msg1", "msg2"]
    assert connector.calls == 2
    assert len(disconnects) == 1
    duration_ms, error = disconnects[0]
    assert duration_ms > 0
    assert error is None


@pytest.mark.asyncio
async def test_force_reconnect_closes_and_reports_reason():
    ws1 = FakeWebSocket([], keep_alive=True)
    ws2 = FakeWebSocket(["after-reconnect"])
    connector = FakeConnector([ws1, ws2])

    received = []
    disconnects = []

    async def on_message(raw):
        received.append(raw)
        if raw == "after-reconnect":
            client.stop()

    async def on_disconnect(duration_ms, error):
        disconnects.append((duration_ms, error))

    client = PersistentWSClient(
        "wss://fake",
        on_message=on_message,
        on_disconnect=on_disconnect,
        connect_fn=connector,
        sleep_fn=_instant_sleep,
    )

    async def force_reconnect_soon():
        while client._ws is None:
            await asyncio.sleep(0)
        await client.force_reconnect("test_reason")

    await asyncio.wait_for(asyncio.gather(client.run(), force_reconnect_soon()), timeout=5)

    assert connector.calls == 2
    assert received == ["after-reconnect"]
    assert len(disconnects) == 1
    duration_ms, error = disconnects[0]
    assert error == "test_reason"


@pytest.mark.asyncio
async def test_set_on_disconnect_overrides_callback():
    ws1 = FakeWebSocket(["msg1"])
    ws2 = FakeWebSocket(["msg2"])
    connector = FakeConnector([ws1, ws2])

    original_calls = []
    replaced_calls = []

    async def on_message(raw):
        if raw == "msg2":
            client.stop()

    async def original_on_disconnect(duration_ms, error):
        original_calls.append((duration_ms, error))

    async def replaced_on_disconnect(duration_ms, error):
        replaced_calls.append((duration_ms, error))

    client = PersistentWSClient(
        "wss://fake",
        on_message=on_message,
        on_disconnect=original_on_disconnect,
        connect_fn=connector,
        sleep_fn=_instant_sleep,
    )
    client.set_on_disconnect(replaced_on_disconnect)

    await client.run()

    assert original_calls == []
    assert len(replaced_calls) == 1


@pytest.mark.asyncio
async def test_stop_before_run_exits_immediately():
    async def on_message(raw):
        pass

    ws = FakeWebSocket([])
    connector = FakeConnector([ws])
    client = PersistentWSClient(
        "wss://fake", on_message=on_message, connect_fn=connector, sleep_fn=_instant_sleep
    )
    client.stop()
    await asyncio.wait_for(client.run(), timeout=1)
