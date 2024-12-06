import json
import logging
from dataclasses import asdict, dataclass, field
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


@dataclass
class LogSummary:
    """Structure for maintaining log summary"""

    timestamp: str
    service_name: str
    total_operations: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[dict[str, Any]] = field(default_factory=list)
    data_operations: list[dict[str, Any]] = field(default_factory=list)
    service_events: list[dict[str, Any]] = field(default_factory=list)
    performance_metrics: dict[str, Any] = field(default_factory=dict)


# Add validation for log levels
def validate_log_level(level: str) -> LogLevel:
    """Validate and convert string to LogLevel"""
    valid_levels: set[LogLevel] = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    upper_level = level.upper()
    if upper_level not in valid_levels:
        raise ValueError(f"Invalid log level: {level}. Must be one of {valid_levels}")
    return upper_level  # type: ignore


def calculate_dataframe_size(df: pl.LazyFrame) -> float:
    """Calculate approximate size of DataFrame in MB"""
    try:
        collected = df.collect()
        # Get size in bytes and convert to MB
        size_mb = collected.estimated_size("mb")
        return size_mb
    except Exception:
        return 0.0


class ServiceLogger:
    """Enhanced logging functionality for services"""

    def __init__(self, name: str, log_dir: Path) -> None:
        self.name = name
        self.log_dir = log_dir
        self.console = Console()

        # Initialize log summary
        self.log_summary = LogSummary(timestamp=datetime.now().isoformat(), service_name=name)

        # Create log directory
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Create timestamped log files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = self.log_dir / f"{name}_{timestamp}.log"
        self.summary_file = self.log_dir / f"{name}_{timestamp}_summary.json"

        # Configure logging
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        # Console handler with rich formatting
        console_handler = RichHandler(
            console=self.console,
            show_path=True,
            rich_tracebacks=True,
            tracebacks_show_locals=False,
        )
        console_handler.setLevel(logging.INFO)

        # File handler for detailed logging
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(logging.DEBUG)

        # Enhanced formatting
        detailed_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(pathname)s:%(lineno)d - %(funcName)s - %(message)s"
        )
        file_handler.setFormatter(detailed_formatter)

        # Add handlers
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

    def _update_summary(self, level: str, message: str, details: dict[str, Any] | None = None) -> None:
        """Update log summary with new entries"""
        self.log_summary.total_operations += 1
        entry = {"timestamp": datetime.now().isoformat(), "message": message, "level": level}
        if details:
            entry.update(details)

        if level == "ERROR":
            self.log_summary.errors.append(entry)
        elif level == "WARNING":
            self.log_summary.warnings.append(entry)
        elif level == "DATA":
            self.log_summary.data_operations.append(entry)
        elif level == "SERVICE":
            self.log_summary.service_events.append(entry)

    def save_summary(self) -> None:
        """Save the log summary to a JSON file"""
        with open(self.summary_file, "w") as f:
            json.dump(asdict(self.log_summary), f, indent=2, default=str)

    def log_method_call(self, method_name: str, **kwargs: Any) -> None:
        """Log method calls with parameters"""
        message = f"Method call: {method_name} - Parameters: {kwargs}"
        self.logger.debug(message)
        self._update_summary("DATA", message, {"method": method_name, "parameters": kwargs, "type": "method_call"})

    def log_data_operation(self, operation: str, df: pl.LazyFrame, details: dict[str, Any] | None = None) -> None:
        """Log details about data operations"""
        try:
            schema = str(df.collect_schema())
            columns = df.columns
            # Calculate size safely
            size_mb = calculate_dataframe_size(df)
            estimated_size = f"{size_mb:.2f} MB"
        except Exception as e:
            schema = "Error getting schema"
            columns = "Error getting columns"
            estimated_size = "Unknown"
            self.warning(f"Error collecting DataFrame info: {str(e)}")

        info = {
            "operation": operation,
            "schema": schema,
            "columns": columns,
            "estimated_size": estimated_size,
            "type": "data_operation",
        }
        if details:
            info.update(details)

        message = f"Data operation: {info}"
        self.logger.debug(message)
        self._update_summary("DATA", message, info)

    def log_service_event(self, event_type: str, details: dict[str, Any]) -> None:
        """Log service-specific events"""
        message = f"Service event - {event_type}: {details}"
        self.logger.info(message)
        self._update_summary(
            "SERVICE", message, {"event_type": event_type, "details": details, "type": "service_event"}
        )

    def log_performance_metric(self, metric_name: str, value: Any, unit: str | None = None) -> None:
        """Log performance metrics"""
        metric_info = {"value": value}
        if unit:
            metric_info["unit"] = unit

        self.log_summary.performance_metrics[metric_name] = metric_info
        self.logger.info(f"Performance metric - {metric_name}: {value} {unit if unit else ''}")

    def _log_with_summary(self, level: str, msg: str, *args: Any, **kwargs: Any) -> None:
        """Internal method to log messages and update summary"""
        getattr(self.logger, level)(msg, *args, **kwargs)
        self._update_summary(level.upper(), msg, kwargs.get("extra"))

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log debug message"""
        self._log_with_summary("debug", msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log info message"""
        self._log_with_summary("info", msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log warning message"""
        self._log_with_summary("warning", msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log error message"""
        self._log_with_summary("error", msg, *args, **kwargs)

    def critical(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log critical message"""
        self._log_with_summary("critical", msg, *args, **kwargs)

    def setLevel(self, level: str) -> None:
        """Set logging level"""
        validated_level = validate_log_level(level)
        self.logger.setLevel(validated_level)

    def __del__(self):
        """Ensure summary is saved when logger is destroyed"""
        try:
            self.save_summary()
        except Exception:
            pass

    def log_file_operation_error(
        self, operation: str, file_path: Path, error: Exception, context: dict[str, Any] | None = None
    ) -> None:
        """Special handling for file operation errors"""
        error_details = {
            "operation": operation,
            "file_path": str(file_path),
            "error_type": type(error).__name__,
            "error_message": str(error),
        }
        if context:
            # Convert context dictionary to string representation
            error_details["context"] = json.dumps(context, default=str)

        message = (
            f"File operation failed:\n"
            f"Operation: {operation}\n"
            f"Path: {file_path}\n"
            f"Error: {type(error).__name__}: {str(error)}"
        )
        if context:
            message += f"\nContext: {json.dumps(context, indent=2, default=str)}"

        self.error(message, extra=error_details)

    def log_operation_chain(
        self, main_operation: str, sub_operations: list[str], error: Exception | None = None
    ) -> None:
        """Log a chain of operations, optionally with an error"""
        chain_details = {
            "main_operation": main_operation,
            "operation_chain": sub_operations,
        }

        if error:
            chain_details["error"] = {"type": type(error).__name__, "message": str(error)}

        message = f"Operation chain: {main_operation}\n" f"Steps:\n" + "\n".join(
            f"  {i+1}. {op}" for i, op in enumerate(sub_operations)
        )

        if error:
            message += f"\nFailed with: {type(error).__name__}: {str(error)}"
            self.error(message, extra=chain_details)
        else:
            self.info(message, extra=chain_details)


def create_logger(name: str) -> ServiceLogger:
    """Create a new service logger"""
    log_dir = Path("logs") / name
    return ServiceLogger(name, log_dir)


def log_dataframe_info(df: pl.LazyFrame, name: str) -> None:
    """Log information about a DataFrame"""
    logger.debug(f"DataFrame info for {name}:")

    try:
        schema = df.collect_schema()
        logger.debug(f"Schema: {schema}")
        logger.debug(f"Columns: {schema.names()}")

        # Get null counts
        null_counts = df.null_count().collect()
        logger.debug(f"Null counts:\n{null_counts}")

        # Get sample data
        sample = df.limit(5).collect()
        logger.debug(f"Sample data:\n{sample}")

        # Log additional DataFrame metrics
        size_mb = calculate_dataframe_size(df)
        logger.log_performance_metric(f"dataframe_{name}_estimated_size", size_mb, "MB")
    except Exception as e:
        logger.error(f"Error analyzing DataFrame {name}: {str(e)}")


# Create default logger instance
logger = create_logger("cdef_cohort")

# Export symbols
__all__ = ["logger", "create_logger", "ServiceLogger", "log_dataframe_info", "LogLevel", "LogSummary"]
