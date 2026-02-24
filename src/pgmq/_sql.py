# src/pgmq/_sql.py
"""
Centralized SQL templates for PGMQ operations.

This module contains all SQL queries used by the PGMQ client, ensuring
consistency between sync and async implementations and making the code
easier to maintain and audit.
"""


# ============================================================================
# Queue Management
# ============================================================================

CREATE_QUEUE = "SELECT pgmq.create(%s);"
CREATE_UNLOGGED_QUEUE = "SELECT pgmq.create_unlogged(%s);"
CREATE_PARTITIONED_QUEUE = "SELECT pgmq.create_partitioned(%s, %s::text, %s::text);"
CREATE_NON_PARTITIONED = "SELECT pgmq.create_non_partitioned(%s);"
DROP_QUEUE = "SELECT pgmq.drop_queue(%s);"
LIST_QUEUES = "SELECT queue_name, created_at, is_partitioned, is_unlogged FROM pgmq.list_queues();"
VALIDATE_QUEUE_NAME = "SELECT pgmq.validate_queue_name(%s);"

# ============================================================================
# Sending Messages
# ============================================================================

SEND = "SELECT * FROM pgmq.send(queue_name=>%s::text, msg=>%s::jsonb);"
SEND_WITH_DELAY_INT = (
    "SELECT * FROM pgmq.send(queue_name=>%s::text, msg=>%s::jsonb, delay=>%s::integer);"
)
SEND_WITH_DELAY_TZ = "SELECT * FROM pgmq.send(queue_name=>%s::text, msg=>%s::jsonb, delay=>%s::timestamptz);"
SEND_WITH_HEADERS = (
    "SELECT * FROM pgmq.send(queue_name=>%s::text, msg=>%s::jsonb, headers=>%s::jsonb);"
)
SEND_WITH_HEADERS_DELAY_INT = "SELECT * FROM pgmq.send(queue_name=>%s::text, msg=>%s::jsonb, headers=>%s::jsonb, delay=>%s::integer);"
SEND_WITH_HEADERS_DELAY_TZ = "SELECT * FROM pgmq.send(queue_name=>%s::text, msg=>%s::jsonb, headers=>%s::jsonb, delay=>%s::timestamptz);"

SEND_BATCH = "SELECT * FROM pgmq.send_batch(queue_name=>%s::text, msgs=>%s::jsonb[]);"
SEND_BATCH_WITH_DELAY_INT = "SELECT * FROM pgmq.send_batch(queue_name=>%s::text, msgs=>%s::jsonb[], delay=>%s::integer);"
SEND_BATCH_WITH_DELAY_TZ = "SELECT * FROM pgmq.send_batch(queue_name=>%s::text, msgs=>%s::jsonb[], delay=>%s::timestamptz);"
SEND_BATCH_WITH_HEADERS = "SELECT * FROM pgmq.send_batch(queue_name=>%s::text, msgs=>%s::jsonb[], headers=>%s::jsonb[]);"
SEND_BATCH_WITH_HEADERS_DELAY_INT = "SELECT * FROM pgmq.send_batch(queue_name=>%s::text, msgs=>%s::jsonb[], headers=>%s::jsonb[], delay=>%s::integer);"
SEND_BATCH_WITH_HEADERS_DELAY_TZ = "SELECT * FROM pgmq.send_batch(queue_name=>%s::text, msgs=>%s::jsonb[], headers=>%s::jsonb[], delay=>%s::timestamptz);"

# ============================================================================
# Topic-Based Routing
# ============================================================================

SEND_TOPIC = "SELECT pgmq.send_topic(%s::text, %s::jsonb);"
SEND_TOPIC_WITH_HEADERS = "SELECT pgmq.send_topic(%s::text, %s::jsonb, %s::jsonb);"
SEND_TOPIC_WITH_DELAY_INT = "SELECT pgmq.send_topic(%s::text, %s::jsonb, %s::integer);"
SEND_TOPIC_WITH_HEADERS_DELAY_INT = (
    "SELECT pgmq.send_topic(%s::text, %s::jsonb, %s::jsonb, %s::integer);"
)

SEND_BATCH_TOPIC = "SELECT * FROM pgmq.send_batch_topic(%s::text, %s::jsonb[]);"
SEND_BATCH_TOPIC_WITH_HEADERS = (
    "SELECT * FROM pgmq.send_batch_topic(%s::text, %s::jsonb[], %s::jsonb[]);"
)
SEND_BATCH_TOPIC_WITH_DELAY_INT = (
    "SELECT * FROM pgmq.send_batch_topic(%s::text, %s::jsonb[], %s::integer);"
)
SEND_BATCH_TOPIC_WITH_DELAY_TZ = (
    "SELECT * FROM pgmq.send_batch_topic(%s::text, %s::jsonb[], %s::timestamptz);"
)
SEND_BATCH_TOPIC_WITH_HEADERS_DELAY_INT = "SELECT * FROM pgmq.send_batch_topic(%s::text, %s::jsonb[], %s::jsonb[], %s::integer);"
SEND_BATCH_TOPIC_WITH_HEADERS_DELAY_TZ = "SELECT * FROM pgmq.send_batch_topic(%s::text, %s::jsonb[], %s::jsonb[], %s::timestamptz);"

BIND_TOPIC = "SELECT pgmq.bind_topic(%s::text, %s::text);"
UNBIND_TOPIC = "SELECT pgmq.unbind_topic(%s::text, %s::text);"
LIST_TOPIC_BINDINGS = "SELECT pattern, queue_name, bound_at, compiled_regex FROM pgmq.list_topic_bindings();"
LIST_TOPIC_BINDINGS_FOR_QUEUE = "SELECT pattern, queue_name, bound_at, compiled_regex FROM pgmq.list_topic_bindings(%s);"
TEST_ROUTING = "SELECT pattern, queue_name, compiled_regex FROM pgmq.test_routing(%s);"

# ============================================================================
# Reading Messages
# ============================================================================

READ = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
          FROM pgmq.read(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer);"""

READ_WITH_POLL = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
                    FROM pgmq.read_with_poll(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer, 
                    max_poll_seconds=>%s::integer, poll_interval_ms=>%s::integer);"""

READ_CONDITIONAL = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
                      FROM pgmq.read(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer, conditional=>%s::jsonb);"""

READ_WITH_POLL_CONDITIONAL = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
                                FROM pgmq.read_with_poll(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer, 
                                max_poll_seconds=>%s::integer, poll_interval_ms=>%s::integer, conditional=>%s::jsonb);"""

# ============================================================================
# FIFO Reading
# ============================================================================

READ_GROUPED = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
                  FROM pgmq.read_grouped(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer);"""

READ_GROUPED_WITH_POLL = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
                            FROM pgmq.read_grouped_with_poll(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer,
                            max_poll_seconds=>%s::integer, poll_interval_ms=>%s::integer);"""

READ_GROUPED_RR = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
                     FROM pgmq.read_grouped_rr(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer);"""

READ_GROUPED_RR_WITH_POLL = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
                               FROM pgmq.read_grouped_rr_with_poll(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer,
                               max_poll_seconds=>%s::integer, poll_interval_ms=>%s::integer);"""

# ============================================================================
# Pop
# ============================================================================

POP = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
         FROM pgmq.pop(queue_name=>%s::text, qty=>%s::integer);"""

# ============================================================================
# Deleting/Archiving
# ============================================================================

DELETE = "SELECT pgmq.delete(queue_name=>%s::text, msg_id=>%s::bigint);"
DELETE_BATCH = "SELECT * FROM pgmq.delete(queue_name=>%s::text, msg_ids=>%s::bigint[]);"
ARCHIVE = "SELECT pgmq.archive(queue_name=>%s::text, msg_id=>%s::bigint);"
ARCHIVE_BATCH = (
    "SELECT * FROM pgmq.archive(queue_name=>%s::text, msg_ids=>%s::bigint[]);"
)
PURGE_QUEUE = "SELECT pgmq.purge_queue(queue_name=>%s);"

# ============================================================================
# Visibility Timeout
# ============================================================================

SET_VT = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
            FROM pgmq.set_vt(queue_name=>%s::text, msg_id=>%s::bigint, vt=>%s);"""

SET_VT_BATCH = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
                  FROM pgmq.set_vt(queue_name=>%s::text, msg_ids=>%s::bigint[], vt=>%s);"""

# ============================================================================
# Metrics
# ============================================================================

METRICS = "SELECT * FROM pgmq.metrics(queue_name=>%s);"
METRICS_ALL = "SELECT * FROM pgmq.metrics_all();"

# ============================================================================
# Notifications
# ============================================================================

ENABLE_NOTIFY = "SELECT pgmq.enable_notify_insert(%s::text, %s::integer);"
DISABLE_NOTIFY = "SELECT pgmq.disable_notify_insert(%s::text);"
UPDATE_NOTIFY = "SELECT pgmq.update_notify_insert(%s::text, %s::integer);"
LIST_NOTIFY_THROTTLES = "SELECT queue_name, throttle_interval_ms, last_notified_at FROM pgmq.list_notify_insert_throttles();"

# ============================================================================
# Utilities
# ============================================================================

VALIDATE_ROUTING_KEY = "SELECT pgmq.validate_routing_key(%s);"
VALIDATE_TOPIC_PATTERN = "SELECT pgmq.validate_topic_pattern(%s);"
CREATE_FIFO_INDEX = "SELECT pgmq.create_fifo_index(%s);"
CREATE_FIFO_INDEXES_ALL = "SELECT pgmq.create_fifo_indexes_all();"
CONVERT_ARCHIVE_PARTITIONED = "SELECT pgmq.convert_archive_partitioned(%s, %s, %s, %s);"
DETACH_ARCHIVE = "SELECT pgmq.detach_archive(%s);"


def get_send_sql(
    headers: bool = False,
    delay: bool = False,
    delay_is_timestamp: bool = False,
) -> str:
    """Get appropriate send SQL based on parameters."""
    if headers and delay:
        return (
            SEND_WITH_HEADERS_DELAY_TZ
            if delay_is_timestamp
            else SEND_WITH_HEADERS_DELAY_INT
        )
    elif headers:
        return SEND_WITH_HEADERS
    elif delay:
        return SEND_WITH_DELAY_TZ if delay_is_timestamp else SEND_WITH_DELAY_INT
    return SEND


def get_send_batch_sql(
    headers: bool = False,
    delay: bool = False,
    delay_is_timestamp: bool = False,
) -> str:
    """Get appropriate send_batch SQL based on parameters."""
    if headers and delay:
        return (
            SEND_BATCH_WITH_HEADERS_DELAY_TZ
            if delay_is_timestamp
            else SEND_BATCH_WITH_HEADERS_DELAY_INT
        )
    elif headers:
        return SEND_BATCH_WITH_HEADERS
    elif delay:
        return (
            SEND_BATCH_WITH_DELAY_TZ
            if delay_is_timestamp
            else SEND_BATCH_WITH_DELAY_INT
        )
    return SEND_BATCH


def get_send_topic_sql(
    headers: bool = False,
    delay: bool = False,
) -> str:
    """Get appropriate send_topic SQL based on parameters."""
    if headers and delay:
        return SEND_TOPIC_WITH_HEADERS_DELAY_INT
    elif headers:
        return SEND_TOPIC_WITH_HEADERS
    elif delay:
        return SEND_TOPIC_WITH_DELAY_INT
    return SEND_TOPIC


def get_send_batch_topic_sql(
    headers: bool = False,
    delay: bool = False,
    delay_is_timestamp: bool = False,
) -> str:
    """Get appropriate send_batch_topic SQL based on parameters."""
    if headers and delay:
        return (
            SEND_BATCH_TOPIC_WITH_HEADERS_DELAY_TZ
            if delay_is_timestamp
            else SEND_BATCH_TOPIC_WITH_HEADERS_DELAY_INT
        )
    elif headers:
        return SEND_BATCH_TOPIC_WITH_HEADERS
    elif delay:
        return (
            SEND_BATCH_TOPIC_WITH_DELAY_TZ
            if delay_is_timestamp
            else SEND_BATCH_TOPIC_WITH_DELAY_INT
        )
    return SEND_BATCH_TOPIC
