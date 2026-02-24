# src/pgmq/notify_listener.py
"""
Notification Listeners for PGMQ.
"""

import asyncio
import logging
import threading
import json
from typing import Callable, Optional, Any, Dict

# Sync imports
try:
    import psycopg
    from psycopg import Notify

    SYNC_AVAILABLE = True
except ImportError:
    SYNC_AVAILABLE = False

# Async imports
try:
    import asyncpg

    ASYNC_AVAILABLE = True
except ImportError:
    ASYNC_AVAILABLE = False

from pgmq.logger import log_with_context


class SyncNotificationListener:
    """
    Synchronous listener for PGMQ queue notifications.
    """

    def __init__(self, queue_client: Any):
        if not SYNC_AVAILABLE:
            raise RuntimeError("psycopg is required for SyncNotificationListener")

        self.queue_client = queue_client
        self.dsn = queue_client.config.dsn
        self.logger = queue_client.logger
        self._stop_event = threading.Event()
        self._conn: Optional[psycopg.Connection] = None

    def listen(
        self,
        queue_name: str,
        callback: Callable[[Dict[str, Any]], None],
        timeout: float = 5.0,
    ) -> None:
        """
        Start listening for notifications on the specified queue.
        """
        # Standard PGMQ channel naming convention
        channel = f"pgmq_insert_{queue_name}"

        log_with_context(
            self.logger,
            logging.INFO,
            "Starting notification listener",
            queue_name=queue_name,
            channel=channel,
        )

        try:
            # Autocommit is required for LISTEN to take effect immediately
            self._conn = psycopg.connect(self.dsn, autocommit=True)
            self._conn.execute(f"LISTEN {channel};")

            # psycopg3: notifies() returns a generator that blocks until a notification arrives.
            # We iterate over it. To allow stopping, we rely on closing the connection
            # in the stop() method, which will break this loop.
            for notify in self._conn.notifies():
                if self._stop_event.is_set():
                    break
                self._handle_notify(notify, callback, queue_name)

        except (psycopg.OperationalError, psycopg.InterfaceError):
            # These are expected when the connection is closed externally via stop()
            if not self._stop_event.is_set():
                log_with_context(
                    self.logger,
                    logging.ERROR,
                    "Listener connection lost unexpectedly",
                    queue_name=queue_name,
                )
        except Exception as e:
            log_with_context(
                self.logger,
                logging.ERROR,
                f"Listener error: {e}",
                queue_name=queue_name,
                error_type=type(e).__name__,
            )
            raise
        finally:
            if self._conn:
                self._conn.close()
                log_with_context(
                    self.logger,
                    logging.INFO,
                    "Listener connection closed",
                    queue_name=queue_name,
                )

    def _handle_notify(self, notify: Notify, callback: Callable, queue_name: str):
        try:
            payload = json.loads(notify.payload)
            log_with_context(
                self.logger,
                logging.DEBUG,
                "Received notification",
                queue_name=queue_name,
                payload=payload,
            )
            callback(payload)
        except Exception as e:
            log_with_context(
                self.logger,
                logging.ERROR,
                f"Error handling notification: {e}",
                queue_name=queue_name,
            )

    def stop(self) -> None:
        """Signal the listener to stop and close the connection."""
        log_with_context(self.logger, logging.INFO, "Stop signal received")
        self._stop_event.set()
        # Closing the connection interrupts the blocking `for notify in conn.notifies()` loop
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass


class AsyncNotificationListener:
    """
    Asynchronous listener for PGMQ queue notifications.
    """

    def __init__(self, queue_client: Any):
        if not ASYNC_AVAILABLE:
            raise RuntimeError("asyncpg is required for AsyncNotificationListener")

        self.queue_client = queue_client
        self.dsn = queue_client.config.async_dsn
        self.logger = queue_client.logger
        self._stop_event = asyncio.Event()
        self._conn: Optional[asyncpg.Connection] = None
        self._queue: asyncio.Queue = asyncio.Queue()

    async def listen(
        self, queue_name: str, callback: Callable[[Dict[str, Any]], None]
    ) -> None:
        channel = f"pgmq_insert_{queue_name}"

        log_with_context(
            self.logger,
            logging.INFO,
            "Starting async notification listener",
            queue_name=queue_name,
            channel=channel,
        )

        try:
            self._conn = await asyncpg.connect(self.dsn)
            await self._conn.add_listener(channel, self._on_notification)

            while not self._stop_event.is_set():
                try:
                    payload = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                    log_with_context(
                        self.logger,
                        logging.DEBUG,
                        "Processing notification payload",
                        queue_name=queue_name,
                        payload=payload,
                    )
                    await callback(payload)
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    log_with_context(
                        self.logger,
                        logging.ERROR,
                        f"Error in listener loop: {e}",
                        queue_name=queue_name,
                    )

        except Exception as e:
            log_with_context(
                self.logger,
                logging.ERROR,
                f"Async listener error: {e}",
                queue_name=queue_name,
            )
            raise
        finally:
            if self._conn:
                await self._conn.close()
                log_with_context(
                    self.logger,
                    logging.INFO,
                    "Async listener connection closed",
                    queue_name=queue_name,
                )

    def _on_notification(self, connection, pid, channel, payload):
        try:
            self._queue.put_nowait(json.loads(payload))
        except Exception as e:
            self.logger.error(f"Failed to parse notification payload: {e}")

    def stop(self) -> None:
        log_with_context(self.logger, logging.INFO, "Stop signal received")
        self._stop_event.set()
