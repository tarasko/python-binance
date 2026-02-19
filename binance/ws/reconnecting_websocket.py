import asyncio
import gzip
import json
import logging
from socket import gaierror
from typing import Optional, Union
from asyncio import sleep
from random import random

# load orjson if available, otherwise default to json
orjson = None
try:
    import orjson as orjson
except ImportError:
    pass

import picows

from binance.exceptions import (
    BinanceWebsocketClosed,
    BinanceWebsocketUnableToConnect,
    BinanceWebsocketQueueOverflow,
    ReadLoopClosed,
)
from binance.helpers import get_loop
from binance.ws.constants import WSListenerState


_DISCONNECT_SENTINEL = object()


class _PicowsWebSocket(picows.WSListener):
    def __init__(self):
        self._transport = None
        self._queue = asyncio.Queue()
        self.closed = False

    def on_ws_connected(self, transport):
        self._transport = transport

    def on_ws_frame(self, transport: picows.WSTransport, frame: picows.WSFrame) -> None:
        if frame.msg_type == picows.WSMsgType.TEXT:
            payload: Union[str, bytes] = frame.get_payload_as_utf8_text()
        elif frame.msg_type == picows.WSMsgType.BINARY:
            payload = frame.get_payload_as_bytes()
        else:
            return
        self._queue.put_nowait(payload)

    def on_ws_disconnected(self, transport: picows.WSTransport) -> None:
        self.closed = True
        self._queue.put_nowait(_DISCONNECT_SENTINEL)

    async def recv(self):
        if self.closed:
            raise ConnectionError("WebSocket is closed")
        msg = await self._queue.get()
        self._queue.task_done()
        if msg is _DISCONNECT_SENTINEL:
            self.closed = True
            raise ConnectionError("WebSocket disconnected")
        return msg

    async def send(self, payload: Union[str, bytes]) -> None:
        if self.closed:
            raise ConnectionError("WebSocket is closed")
        if isinstance(payload, bytes):
            self._transport.send(picows.WSMsgType.BINARY, payload)
        else:
            self._transport.send(picows.WSMsgType.TEXT, payload.encode("utf-8"))

    async def close(self) -> None:
        if self.closed:
            return
        self._transport.send_close()
        self._transport.disconnect()
        await self._transport.wait_disconnected()


class ReconnectingWebsocket:
    MAX_RECONNECTS = 5
    MAX_RECONNECT_SECONDS = 60
    MIN_RECONNECT_WAIT = 0.1
    TIMEOUT = 10
    NO_MESSAGE_RECONNECT_TIMEOUT = 60

    def __init__(
        self,
        url: str,
        path: Optional[str] = None,
        prefix: str = "ws/",
        is_binary: bool = False,
        exit_coro=None,
        https_proxy: Optional[str] = None,
        max_queue_size: int = 100,
        **kwargs,
    ):
        self._loop = get_loop()
        self._log = logging.getLogger(__name__)
        self._path = path
        self._url = url
        self._exit_coro = exit_coro
        self._prefix = prefix
        self._reconnects = 0
        self._is_binary = is_binary
        self._conn = None
        self._socket = None
        self.ws: Optional[_PicowsWebSocket] = None
        self.ws_state = WSListenerState.INITIALISING
        self._queue = asyncio.Queue()
        self._handle_read_loop = None
        self._https_proxy = https_proxy
        self._ws_kwargs = kwargs
        self.max_queue_size = max_queue_size

    def json_dumps(self, msg) -> str:
        if orjson:
            return orjson.dumps(msg).decode("utf-8")
        return json.dumps(msg)

    def json_loads(self, msg):
        if orjson:
            return orjson.loads(msg)
        return json.loads(msg)

    async def __aenter__(self):
        await self.connect()
        return self

    async def close(self):
        await self.__aexit__(None, None, None)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._log.debug(f"Closing Websocket {self._url}{self._prefix}{self._path}")
        if self._handle_read_loop:
            await self._kill_read_loop()
        if self._exit_coro:
            await self._exit_coro(self._path)
        if self.ws:
            await self.ws.close()
        self.ws = None

    async def connect(self):
        self._log.debug("Establishing new WebSocket connection")
        self.ws_state = WSListenerState.RECONNECTING
        await self._before_connect()

        ws_url = (
            f"{self._url}{getattr(self, '_prefix', '')}{getattr(self, '_path', '')}"
        )

        try:
            if self._https_proxy and self._https_proxy.lower().startswith("https://"):
                raise ValueError(
                    "picows does not support https:// proxy URLs; use http://, socks4://, or socks5://"
                )
            _, self.ws = await picows.ws_connect(
                _PicowsWebSocket,
                ws_url,
                proxy=self._https_proxy,
                **self._ws_kwargs,
            )
        except Exception as e:  # noqa
            self._log.error(f"Failed to connect to websocket: {e}")
            self.ws_state = WSListenerState.RECONNECTING
            raise e
        self.ws_state = WSListenerState.STREAMING
        self._reconnects = 0
        await self._after_connect()
        if not self._handle_read_loop:
            self._handle_read_loop = self._loop.call_soon_threadsafe(
                asyncio.create_task, self._read_loop()
            )

    async def _kill_read_loop(self):
        self.ws_state = WSListenerState.EXITING
        while self._handle_read_loop:
            await sleep(0.1)
        self._log.debug("Finished killing read_loop")

    async def _before_connect(self):
        pass

    async def _after_connect(self):
        pass

    def _handle_message(self, evt):
        if self._is_binary:
            try:
                evt = gzip.decompress(evt)
            except (ValueError, OSError) as e:
                self._log.error(f"Failed to decompress message: {(e)}")
                raise
            except Exception as e:
                self._log.error(f"Unexpected decompression error: {(e)}")
                raise
        try:
            return self.json_loads(evt)
        except ValueError as e:
            self._log.error(f"JSON Value Error parsing message: Error: {(e)}")
            raise
        except TypeError as e:
            self._log.error(f"JSON Type Error parsing message. Error: {(e)}")
            raise
        except Exception as e:
            self._log.error(f"Unexpected error parsing message. Error: {(e)}")
            raise

    async def _read_loop(self):
        try:
            while True:
                try:
                    while self.ws_state == WSListenerState.RECONNECTING:
                        await self._run_reconnect()

                    if self.ws_state == WSListenerState.EXITING:
                        self._log.debug(
                            f"_read_loop {self._path} break for {self.ws_state}"
                        )
                        break
                    elif self.ws and self.ws.closed:
                        self._reconnect()
                        raise BinanceWebsocketClosed(
                            "Connection closed. Reconnecting..."
                        )
                    elif self.ws_state == WSListenerState.STREAMING:
                        assert self.ws
                        res = await asyncio.wait_for(
                            self.ws.recv(), timeout=self.TIMEOUT
                        )
                        res = self._handle_message(res)
                        self._log.debug(f"Received message: {res}")
                        if res:
                            if self._queue.qsize() < self.max_queue_size:
                                await self._queue.put(res)
                            else:
                                raise BinanceWebsocketQueueOverflow(
                                    f"Message queue size {self._queue.qsize()} exceeded maximum {self.max_queue_size}"
                                )
                except asyncio.TimeoutError:
                    self._log.debug(f"no message in {self.TIMEOUT} seconds")
                    # _no_message_received_reconnect
                except asyncio.CancelledError as e:
                    self._log.debug(f"_read_loop cancelled error {e}")
                    await self._queue.put({
                        "e": "error",
                        "type": f"{e.__class__.__name__}",
                        "m": f"{e}",
                    })
                    break
                except (
                    asyncio.IncompleteReadError,
                    gaierror,
                    ConnectionError,
                    BinanceWebsocketClosed,
                ) as e:
                    # reports errors and continue loop
                    self._log.error(f"{e.__class__.__name__} ({e})")
                    await self._queue.put({
                        "e": "error",
                        "type": f"{e.__class__.__name__}",
                        "m": f"{e}",
                    })
                except (
                    BinanceWebsocketUnableToConnect,
                    BinanceWebsocketQueueOverflow,
                    Exception,
                ) as e:
                    # reports errors and break the loop
                    self._log.error(f"Unknown exception: {e.__class__.__name__} ({e})")
                    await self._queue.put({
                        "e": "error",
                        "type": e.__class__.__name__,
                        "m": f"{e}",
                    })
                    break
        except Exception as e:
            self._log.error(f"Unknown exception: {e.__class__.__name__} ({e})")
        finally:
            self._handle_read_loop = None  # Signal the coro is stopped
            self._reconnects = 0

    async def _run_reconnect(self):
        await self.before_reconnect()
        if self._reconnects < self.MAX_RECONNECTS:
            reconnect_wait = self._get_reconnect_wait(self._reconnects)
            self._log.debug(
                f"websocket reconnecting. {self.MAX_RECONNECTS - self._reconnects} reconnects left - "
                f"waiting {reconnect_wait}"
            )
            await asyncio.sleep(reconnect_wait)
            try:
                await self.connect()
            except Exception as e:
                pass
        else:
            self._log.error(f"Max reconnections {self.MAX_RECONNECTS} reached:")
            # Signal the error
            raise BinanceWebsocketUnableToConnect

    async def recv(self):
        res = None
        while not res:
            if not self._handle_read_loop:
                raise ReadLoopClosed(
                    "Read loop has been closed, please reset the websocket connection and listen to the message error."
                )
            try:
                res = await asyncio.wait_for(self._queue.get(), timeout=self.TIMEOUT)
            except asyncio.TimeoutError:
                self._log.debug(f"no message in {self.TIMEOUT} seconds")
        return res

    async def _wait_for_reconnect(self):
        while (
            self.ws_state != WSListenerState.STREAMING
            and self.ws_state != WSListenerState.EXITING
        ):
            await sleep(0.1)

    def _get_reconnect_wait(self, attempts: int) -> int:
        expo = 2**attempts
        return round(random() * min(self.MAX_RECONNECT_SECONDS, expo - 1) + 1)

    async def before_reconnect(self):
        if self.ws:
            await self.ws.close()
            self.ws = None

        self._reconnects += 1

    def _reconnect(self):
        self.ws_state = WSListenerState.RECONNECTING
