import sys
import pytest
import gzip
import json
from unittest.mock import patch, Mock
from binance.ws.reconnecting_websocket import ReconnectingWebsocket
from binance.ws.constants import WSListenerState
from binance.exceptions import BinanceWebsocketUnableToConnect, ReadLoopClosed
import picows
import asyncio

try:
    from unittest.mock import AsyncMock  # Python 3.8+
except ImportError:
    from asynctest import CoroutineMock as AsyncMock  # Python 3.7


@pytest.mark.asyncio
async def test_init():
    ws = ReconnectingWebsocket(url="wss://test.url", path="/test")
    assert ws._url == "wss://test.url"
    assert ws._path == "/test"
    assert ws.ws_state == WSListenerState.INITIALISING


@pytest.mark.asyncio
async def test_json_dumps():
    ws = ReconnectingWebsocket(url="wss://test.url")
    data = {"key": "value"}
    dumped = ws.json_dumps(data)
    assert isinstance(dumped, (str, bytes))


@pytest.mark.asyncio
async def test_json_loads():
    ws = ReconnectingWebsocket(url="wss://test.url")
    data_str = '{"key": "value"}'
    loaded = ws.json_loads(data_str)
    assert loaded == {"key": "value"}


@pytest.mark.asyncio
async def test_json_loads_invalid():
    ws = ReconnectingWebsocket(url="wss://test.url")
    data_str = "invalid json"
    with pytest.raises(json.JSONDecodeError):
        ws.json_loads(data_str)


@pytest.mark.asyncio
async def test_handle_message():
    ws = ReconnectingWebsocket(url="wss://test.url")
    message = '{"key": "value"}'
    result = ws._handle_message(message)
    assert result == {"key": "value"}


@pytest.mark.asyncio
async def test_handle_message_binary():
    ws = ReconnectingWebsocket(url="wss://test.url", is_binary=True)
    data = b'{"key": "value"}'
    compressed = gzip.compress(data)
    result = ws._handle_message(compressed)
    assert result == {"key": "value"}


@pytest.mark.asyncio
async def test_handle_message_invalid_json():
    ws = ReconnectingWebsocket(url="wss://test.url")
    message = "invalid json"
    with pytest.raises(Exception):
        ws._handle_message(message)


@pytest.mark.asyncio
async def test_recv_message():
    ws = ReconnectingWebsocket(url="wss://test.url")
    await ws._queue.put({"test": "data"})
    # Simulate the read loop being active
    ws._handle_read_loop = Mock()
    result = await ws.recv()
    assert result == {"test": "data"}


class MockFrame:
    def __init__(self, msg_type, payload):
        self.msg_type = msg_type
        self._payload = payload

    def get_payload_as_utf8_text(self):
        return self._payload

    def get_payload_as_bytes(self):
        return self._payload


class MockTransport:
    def __init__(self, listener):
        self.listener = listener
        self.sent = []
        self._disconnected = asyncio.Event()

    def send(self, msg_type, payload):
        self.sent.append((msg_type, payload))

    def send_close(self):
        self.listener.on_ws_disconnected(self)
        self._disconnected.set()

    def disconnect(self, graceful=False):
        self._disconnected.set()

    async def wait_disconnected(self):
        await self._disconnected.wait()

    def emit_text(self, payload: str):
        frame = MockFrame(picows.WSMsgType.TEXT, payload)
        self.listener.on_ws_frame(self, frame)


@pytest.mark.skipif(sys.version_info < (3, 8), reason="Requires Python 3.8+")
@pytest.mark.asyncio
async def test_before_reconnect():
    ws = ReconnectingWebsocket(url="wss://test.url")
    ws.ws = AsyncMock()
    ws_connection = ws.ws
    ws._reconnects = 0
    await ws.before_reconnect()
    ws_connection.close.assert_awaited_once()
    assert ws.ws is None
    assert ws._reconnects == 1


def test_get_reconnect_wait():
    ws = ReconnectingWebsocket(url="wss://test.url")
    wait_time = ws._get_reconnect_wait(2)
    assert 1 <= wait_time <= ws.MAX_RECONNECT_SECONDS


@pytest.mark.skipif(sys.version_info < (3, 8), reason="Requires Python 3.8+")
@pytest.mark.asyncio
async def test_connect_max_reconnects_exceeded():
    """Test ws.connect exceeds maximum reconnect attempts."""
    ws = ReconnectingWebsocket(url="wss://test.url")
    ws.MAX_RECONNECTS = 2  # type: ignore # Set max reconnects to a low number for testing
    ws._before_connect = AsyncMock()
    ws._after_connect = AsyncMock()
    with patch.object(ws._log, "error") as mock_log:
        with patch(
            "binance.ws.reconnecting_websocket.picows.ws_connect",
            side_effect=Exception("Connection failed"),
        ):
            with pytest.raises(BinanceWebsocketUnableToConnect):
                for _ in range(3):  # Exceed MAX_RECONNECTS
                    await ws._run_reconnect()
        mock_log.assert_called_with(f"Max reconnections {ws.MAX_RECONNECTS} reached:")

    assert ws._reconnects == ws.MAX_RECONNECTS


@pytest.mark.skipif(sys.version_info < (3, 8), reason="Requires Python 3.8+")
@pytest.mark.asyncio
async def test_recieve_invalid_json():
    transport = None

    async def _mock_connect(listener_factory, *_args, **_kwargs):
        nonlocal transport
        listener = listener_factory()
        transport = MockTransport(listener)
        listener.on_ws_connected(transport)
        return transport, listener

    with patch(
        "binance.ws.reconnecting_websocket.picows.ws_connect",
        side_effect=_mock_connect,
    ):
        ws = ReconnectingWebsocket(url="wss://test.url")
        async with ws:
            assert transport is not None
            transport.emit_text("invalid json{")
            msg = await ws.recv()
            assert msg["e"] == "error"
            assert msg["type"] == "JSONDecodeError"  # JSON parsing error


@pytest.mark.skipif(sys.version_info < (3, 8), reason="Requires Python 3.8+")
@pytest.mark.asyncio
async def test_receive_valid_json():
    msgRecv = '{"e": "value"}'
    transport = None

    async def _mock_connect(listener_factory, *_args, **_kwargs):
        nonlocal transport
        listener = listener_factory()
        transport = MockTransport(listener)
        listener.on_ws_connected(transport)
        return transport, listener

    with patch(
        "binance.ws.reconnecting_websocket.picows.ws_connect",
        side_effect=_mock_connect,
    ):
        ws = ReconnectingWebsocket(url="wss://test.url")
        async with ws:
            assert transport is not None
            transport.emit_text(msgRecv)
            msg = await ws.recv()
            assert msg == json.loads(msgRecv)


@pytest.mark.skipif(sys.version_info < (3, 8), reason="Requires Python 3.8+")
@pytest.mark.asyncio
async def test_connect_fails_to_connect_on_enter_context():
    """Test ws.connect raises a ConnectionClosedError."""
    ws = ReconnectingWebsocket(url="wss://test.url")
    with patch(
        "binance.ws.reconnecting_websocket.picows.ws_connect",
        side_effect=Exception("Connection closed"),
    ):
        with pytest.raises(Exception):
            await ws.__aenter__()


@pytest.mark.skipif(sys.version_info < (3, 8), reason="Requires Python 3.8+")
@pytest.mark.asyncio
async def test_connect_fails_to_connect_after_disconnect():
    connect_calls = 0

    async def _mock_connect(listener_factory, *_args, **_kwargs):
        nonlocal connect_calls
        connect_calls += 1
        if connect_calls > 1:
            raise Exception("Connection failed")
        listener = listener_factory()
        transport = MockTransport(listener)
        listener.on_ws_connected(transport)
        return transport, listener

    with patch(
        "binance.ws.reconnecting_websocket.picows.ws_connect",
        side_effect=_mock_connect,
    ):
        ws = ReconnectingWebsocket(url="wss://test.url")
        async with ws as ws:
            assert ws.ws is not None
            ws.ws._transport.emit_text('{"e":"value"}')
            _ = await ws.recv()
            await ws.ws.close()
            msg = await ws.recv()
            while msg["type"] in {"ConnectionError", "BinanceWebsocketClosed"}:
                msg = await ws.recv()
            assert msg["e"] == "error"
            assert msg["type"] == "BinanceWebsocketUnableToConnect"


@pytest.mark.skipif(sys.version_info < (3, 8), reason="Requires Python 3.8+")
@pytest.mark.asyncio
async def test_recv_read_loop_closed():
    """Test that recv() raises ReadLoopClosed when read loop is closed."""
    ws = ReconnectingWebsocket(url="wss://test.url")
    
    # Simulate read loop being closed by setting _handle_read_loop to None
    ws._handle_read_loop = None
    
    with pytest.raises(ReadLoopClosed) as exc_info:
        await ws.recv()
    
    assert "Read loop has been closed" in str(exc_info.value)
    assert "please reset the websocket connection" in str(exc_info.value)
