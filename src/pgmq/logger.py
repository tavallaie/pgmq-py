"""
Centralized logging management for PGMQ.

Provides a unified interface for both standard library logging and loguru,
with automatic backend detection and structured logging support.
"""

import logging
import logging.handlers
import os
import sys
import functools
import inspect
import time
from datetime import datetime
from typing import Optional, Dict, Any, Union, Set

# Optional loguru support
try:
    from loguru import logger as loguru_logger

    LOGURU_AVAILABLE = True
except ImportError:
    LOGURU_AVAILABLE = False


class LoggingManager:
    """
    Manages logger configuration with idempotent setup.

    Prevents duplicate handlers and provides consistent configuration
    across both standard library and loguru backends.
    """

    _configured_loggers: Dict[str, Any] = {}
    _loguru_handler_ids: Set[int] = set()
    _use_loguru: bool = LOGURU_AVAILABLE

    @classmethod
    def get_logger(
        cls,
        name: str,
        verbose: bool = False,
        log_filename: Optional[str] = None,
        structured: bool = False,
        rotation: Optional[str] = None,
        retention: Optional[str] = None,
    ) -> Union[logging.Logger, Any]:
        """
        Get or create a configured logger.

        Uses caching to prevent duplicate configuration of the same logger.
        """
        cache_key = (
            f"{name}:{verbose}:{log_filename}:{structured}:{rotation}:{retention}"
        )

        if cache_key in cls._configured_loggers:
            return cls._configured_loggers[cache_key]

        needs_enhanced = structured or rotation or retention

        if needs_enhanced and cls._use_loguru:
            logger = cls._configure_loguru(
                name, verbose, log_filename, structured, rotation, retention
            )
        else:
            logger = cls._configure_stdlib(
                name, verbose, log_filename, structured, bool(rotation)
            )

        cls._configured_loggers[cache_key] = logger
        return logger

    @classmethod
    def _configure_stdlib(
        cls,
        name: str,
        verbose: bool,
        log_filename: Optional[str],
        structured: bool,
        enable_rotation: bool,
    ) -> logging.Logger:
        """Configure standard library logger."""
        logger = logging.getLogger(name)

        # Return existing if already configured
        if logger.handlers:
            return logger

        level = logging.DEBUG if verbose else logging.WARNING
        logger.setLevel(level)

        # Format setup
        if structured:
            fmt = '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
        else:
            fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

        formatter = logging.Formatter(fmt)

        # Console handler
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(formatter)
        logger.addHandler(console)

        # File handler if requested
        if verbose or log_filename:
            filename = log_filename or datetime.now().strftime("pgmq_%Y%m%d_%H%M%S.log")
            filepath = os.path.join(os.getcwd(), filename)

            if enable_rotation:
                file_handler = logging.handlers.RotatingFileHandler(
                    filepath, maxBytes=10 * 1024 * 1024, backupCount=5
                )
            else:
                file_handler = logging.FileHandler(filepath)

            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger

    @classmethod
    def _configure_loguru(
        cls,
        name: str,
        verbose: bool,
        log_filename: Optional[str],
        structured: bool,
        rotation: Optional[str],
        retention: Optional[str],
    ) -> Any:
        """Configure loguru logger."""
        level = "DEBUG" if verbose else "WARNING"

        if structured:
            fmt = '{{"timestamp": "{time:YYYY-MM-DD HH:mm:ss.SSS}", "level": "{level}", "logger": "{extra[logger]}", "message": "{message}"}}'
        else:
            fmt = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"

        # Only add handlers if we need file output or verbose mode
        if verbose or log_filename:
            # Console handler
            handler_id = loguru_logger.add(
                sys.stderr,
                format=fmt,
                level=level,
                enqueue=True,
            )
            cls._loguru_handler_ids.add(handler_id)

            # File handler
            filename = log_filename or datetime.now().strftime("pgmq_%Y%m%d_%H%M%S.log")
            filepath = os.path.join(os.getcwd(), filename)

            file_id = loguru_logger.add(
                filepath,
                format=fmt,
                level=level,
                rotation=rotation or "10 MB",
                retention=retention or "1 week",
                enqueue=True,
            )
            cls._loguru_handler_ids.add(file_id)

        return loguru_logger.bind(logger=name)


def create_logger(
    name: str,
    verbose: bool = False,
    log_filename: Optional[str] = None,
) -> Union[logging.Logger, Any]:
    """
    Backward-compatible logger factory.

    Simple interface matching legacy PGMQ API.
    """
    return LoggingManager.get_logger(
        name=name,
        verbose=verbose,
        log_filename=log_filename,
    )


def log_with_context(
    logger: Union[logging.Logger, Any], level: int, message: str, **context: Any
) -> None:
    """
    Emit log entry with structured context.

    Automatically adapts to logger type (stdlib vs loguru).
    """
    # Check if loguru (has bind method)
    if hasattr(logger, "bind") and callable(getattr(logger, "bind")):
        # Loguru style
        bound = logger.bind(**context) if context else logger

        # Convert int level to string
        if isinstance(level, int):
            level_map = {
                logging.DEBUG: "DEBUG",
                logging.INFO: "INFO",
                logging.WARNING: "WARNING",
                logging.ERROR: "ERROR",
                logging.CRITICAL: "CRITICAL",
            }
            level = level_map.get(level, "INFO")

        bound.log(level, message)
    else:
        # Standard library style
        if context:
            context_str = " | ".join(f"{k}={v}" for k, v in context.items())
            message = f"{message} | {context_str}"

        logger.log(level, message)


def log_performance(logger: Union[logging.Logger, Any]):
    """
    Decorator factory for timing function execution.

    Works with both sync and async functions.
    """

    def decorator(func):
        is_async = inspect.iscoroutinefunction(func)

        if is_async:

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                start = time.time()
                try:
                    result = await func(*args, **kwargs)
                    elapsed = (time.time() - start) * 1000
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        f"Completed {func.__name__}",
                        function=func.__name__,
                        elapsed_ms=round(elapsed, 2),
                        success=True,
                    )
                    return result
                except Exception as e:
                    elapsed = (time.time() - start) * 1000
                    log_with_context(
                        logger,
                        logging.ERROR,
                        f"Failed {func.__name__}: {e}",
                        function=func.__name__,
                        elapsed_ms=round(elapsed, 2),
                        success=False,
                        error=str(e),
                    )
                    raise

            return async_wrapper
        else:

            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                start = time.time()
                try:
                    result = func(*args, **kwargs)
                    elapsed = (time.time() - start) * 1000
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        f"Completed {func.__name__}",
                        function=func.__name__,
                        elapsed_ms=round(elapsed, 2),
                        success=True,
                    )
                    return result
                except Exception as e:
                    elapsed = (time.time() - start) * 1000
                    log_with_context(
                        logger,
                        logging.ERROR,
                        f"Failed {func.__name__}: {e}",
                        function=func.__name__,
                        elapsed_ms=round(elapsed, 2),
                        success=False,
                        error=str(e),
                    )
                    raise

            return sync_wrapper

    return decorator


# Backward compatibility alias
PGMQLogger = LoggingManager
