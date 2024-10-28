import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import polars as pl
from rich.console import Console
from rich.logging import RichHandler
from rich.traceback import install

# Install rich traceback handler
install(show_locals=True)

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class ServiceLogger:
    """Enhanced logging functionality for services"""

    def __init__(self, name: str, log_dir: Path) -> None:
        self.name = name
        self.log_dir = log_dir
        self.console = Console()

        # Create log directory
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Create timestamped log file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = self.log_dir / f"{name}_{timestamp}.log"

        # Configure logging
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        # Console handler with rich formatting
        console_handler = RichHandler(
            console=self.console,
            show_path=False,
            rich_tracebacks=True,
            tracebacks_show_locals=True,
        )
        console_handler.setLevel(logging.INFO)

        # File handler for detailed logging
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(logging.DEBUG)

        # Formatting
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)

        # Add handlers
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

    def log_method_call(self, method_name: str, **kwargs: Any) -> None:
        """Log method calls with parameters"""
        self.logger.debug(f"Method call: {method_name} - Parameters: {kwargs}")

    def log_data_operation(self, operation: str, df: pl.LazyFrame, details: dict[str, Any] | None = None) -> None:
        """Log details about data operations"""
        info = {
            "operation": operation,
            "schema": str(df.collect_schema()),
            "columns": df.columns,
        }
        if details:
            info.update(details)
        self.logger.debug(f"Data operation: {info}")

    def log_service_event(self, event_type: str, details: dict[str, Any]) -> None:
        """Log service-specific events"""
        self.logger.info(f"Service event - {event_type}: {details}")

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log debug message"""
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log info message"""
        self.logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log warning message"""
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log error message"""
        self.logger.error(msg, *args, **kwargs)

    def critical(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log critical message"""
        self.logger.critical(msg, *args, **kwargs)

    def setLevel(self, level: str) -> None:
        """Set logging level"""
        self.logger.setLevel(level)


def create_logger(name: str) -> ServiceLogger:
    """Create a new service logger"""
    log_dir = Path("logs") / name
    return ServiceLogger(name, log_dir)


def log_dataframe_info(df: pl.LazyFrame, name: str) -> None:
    """Log information about a DataFrame"""
    logger.debug(f"DataFrame info for {name}:")
    logger.debug(f"Schema: {df.collect_schema()}")
    logger.debug(f"Columns: {df.collect_schema().names()}")

    # Get non-null counts and sample with string cache enabled
    try:
        null_counts = df.null_count().collect()
        logger.debug(f"Null counts:\n{null_counts}")

        sample = df.limit(5).collect()
        logger.debug(f"Sample data:\n{sample}")
    except Exception as e:
        logger.warning(f"Could not get detailed info for {name}: {str(e)}")


# Create default logger instance
logger = create_logger("cdef_cohort")

# Export symbols
__all__ = ["logger", "create_logger", "ServiceLogger", "log_dataframe_info", "LogLevel"]
