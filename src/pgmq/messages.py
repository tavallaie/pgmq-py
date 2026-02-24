# src/pgmq/messages.py
"""
Dataclasses representing PGMQ database types.

These classes map directly to PostgreSQL composite types defined by the PGMQ
extension, ensuring type safety and IDE autocomplete support.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any, Callable


@dataclass
class Message:
    """
    Complete message record matching pgmq.message_record type.

    Attributes:
        msg_id: Unique ID of the message
        read_ct: Number of times the message has been read
        enqueued_at: Timestamp when the message was inserted
        last_read_at: Timestamp when the message was last read (None if never read)
        vt: Timestamp when the message will become available for reading
        message: The message payload as a dictionary
        headers: Optional message headers/metadata
    """

    msg_id: int
    read_ct: int
    enqueued_at: datetime
    last_read_at: Optional[datetime]
    vt: datetime
    message: Dict[str, Any]
    headers: Optional[Dict[str, Any]]

    @classmethod
    def from_row(
        cls, row: tuple, json_parser: Optional[Callable[[Any], Dict[str, Any]]] = None
    ) -> "Message":
        """
        Factory method to create Message from a database row tuple.

        Args:
            row: Database result row (msg_id, read_ct, enqueued_at, last_read_at,
                vt, message, headers)
            json_parser: Optional function to parse JSONB. If None, assumes
                        already parsed (for psycopg) or uses identity.
        """

        def _identity(x: Any) -> Any:
            return x

        if json_parser is None:
            json_parser = _identity

        return cls(
            msg_id=row[0],
            read_ct=row[1],
            enqueued_at=row[2],
            last_read_at=row[3],
            vt=row[4],
            message=json_parser(row[5]),
            headers=json_parser(row[6]) if row[6] is not None else None,
        )

    def __repr__(self) -> str:
        return (
            f"Message(msg_id={self.msg_id}, read_ct={self.read_ct}, "
            f"enqueued_at={self.enqueued_at.isoformat()}, "
            f"message={self.message!r})"
        )


@dataclass
class QueueRecord:
    """
    Queue metadata matching pgmq.queue_record type.

    Attributes:
        queue_name: Name of the queue
        is_partitioned: Whether the queue uses table partitioning
        is_unlogged: Whether the queue uses unlogged tables (faster but not crash-safe)
        created_at: When the queue was created
    """

    queue_name: str
    is_partitioned: bool
    is_unlogged: bool
    created_at: datetime

    @classmethod
    def from_row(cls, row: tuple) -> "QueueRecord":
        return cls(
            queue_name=row[0],
            created_at=row[1],
            is_partitioned=row[2],
            is_unlogged=row[3],
        )

    def __str__(self) -> str:
        """Return queue name for backward compatibility printing."""
        return self.queue_name


@dataclass
class QueueMetrics:
    """
    Queue statistics matching pgmq.metrics_result type.

    Attributes:
        queue_name: Name of the queue
        queue_length: Total messages currently in queue
        newest_msg_age_sec: Age of newest message in seconds (None if empty)
        oldest_msg_age_sec: Age of oldest message in seconds (None if empty)
        total_messages: Total messages ever processed through this queue
        scrape_time: When these metrics were collected
        queue_visible_length: Number of messages currently visible (vt <= now)
    """

    queue_name: str
    queue_length: int
    newest_msg_age_sec: Optional[int]
    oldest_msg_age_sec: Optional[int]
    total_messages: int
    scrape_time: datetime
    queue_visible_length: int

    @classmethod
    def from_row(cls, row: tuple) -> "QueueMetrics":
        """Create QueueMetrics from database row."""
        # Handle both old (6 columns) and new (7 columns) schema versions
        if len(row) >= 7:
            visible_length = row[6]
        else:
            visible_length = row[1]  # Fallback to queue_length for older versions

        return cls(
            queue_name=row[0],
            queue_length=row[1],
            newest_msg_age_sec=row[2],
            oldest_msg_age_sec=row[3],
            total_messages=row[4],
            scrape_time=row[5],
            queue_visible_length=visible_length,
        )


@dataclass
class TopicBinding:
    """
    Topic routing binding record.

    Attributes:
        pattern: The wildcard pattern (e.g., 'logs.*.error')
        queue_name: Queue that receives matching messages
        bound_at: When this binding was created
        compiled_regex: Internal regex used for matching
    """

    pattern: str
    queue_name: str
    bound_at: datetime
    compiled_regex: str

    @classmethod
    def from_row(cls, row: tuple) -> "TopicBinding":
        """Create TopicBinding from database row."""
        return cls(
            pattern=row[0],
            queue_name=row[1],
            bound_at=row[2],
            compiled_regex=row[3],
        )


@dataclass
class RoutingResult:
    """
    Result from test_routing function.

    Shows which queues would receive a message with a given routing key.
    """

    pattern: str
    queue_name: str
    compiled_regex: str

    @classmethod
    def from_row(cls, row: tuple) -> "RoutingResult":
        """Create RoutingResult from database row."""
        return cls(
            pattern=row[0],
            queue_name=row[1],
            compiled_regex=row[2],
        )


@dataclass
class BatchTopicResult:
    """
    Result from send_batch_topic function.

    Maps which queue received which message ID.
    """

    queue_name: str
    msg_id: int

    @classmethod
    def from_row(cls, row: tuple) -> "BatchTopicResult":
        """Create BatchTopicResult from database row."""
        return cls(
            queue_name=row[0],
            msg_id=row[1],
        )


@dataclass
class NotificationThrottle:
    """
    Notification throttle configuration.

    Attributes:
        queue_name: Queue with notifications enabled
        throttle_interval_ms: Minimum milliseconds between notifications
        last_notified_at: Timestamp of last notification (epoch if never)
    """

    queue_name: str
    throttle_interval_ms: int
    last_notified_at: datetime

    @classmethod
    def from_row(cls, row: tuple) -> "NotificationThrottle":
        """Create NotificationThrottle from database row."""
        return cls(
            queue_name=row[0],
            throttle_interval_ms=row[1],
            last_notified_at=row[2],
        )
