import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import polars as pl
from rich.console import Console
from rich.logging import RichHandler

LogLevel = Literal["debug", "info", "warning", "error", "critical"]


class RichLogger:
    def __init__(self, name: str, log_file: Path) -> None:
        self.console = Console()
        self.log_file = log_file
        self.name = name

        # Setup logging
        logging.basicConfig(
            level="NOTSET",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="[%Y-%m-%d %H:%M:%S]",
            handlers=[
                RichHandler(
                    console=self.console,
                    rich_tracebacks=True,
                    show_time=False,
                    show_path=False,
                ),
                logging.FileHandler(log_file, mode="w", encoding="utf-8"),
            ],
        )

        self.logger = logging.getLogger(name)

    def log(self, message: str, level: LogLevel = "info", **kwargs: Any) -> None:
        getattr(self.logger, level)(message, extra={"markup": True}, **kwargs)

    def debug(self, message: str, **kwargs: Any) -> None:
        self.log(message, "debug", **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        self.log(message, "info", **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        self.log(message, "warning", **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        self.log(message, "error", **kwargs)

    def critical(self, message: str, **kwargs: Any) -> None:
        self.log(message, "critical", **kwargs)

    def setLevel(self, level: str) -> None:
        numeric_level = getattr(logging, level.upper())
        self.logger.setLevel(numeric_level)
        for handler in self.logger.handlers:
            handler.setLevel(numeric_level)


def setup_logging(log_level: LogLevel = "info") -> RichLogger:
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"cdef_cohort_builder_{timestamp}.log"

    logger = RichLogger("cdef_cohort_builder", log_file)

    # Convert string log level to logging module constant
    numeric_level = getattr(logging, str(log_level.upper()))

    # Set the level for both handlers
    for handler in logger.logger.handlers:
        handler.setLevel(numeric_level)

    logger.logger.setLevel(numeric_level)  # Set the logger's level as well

    logger.info(f"Logging session started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Log file: {log_file}")
    logger.info(f"Log level set to: {log_level.upper()}")

    return logger


logger = setup_logging()


def log(message: str, level: LogLevel = "info", **kwargs: Any) -> None:
    logger.log(message, level, **kwargs)


def log_non_null_counts(df: pl.LazyFrame, name: str) -> None:
    counts = df.select(pl.all().is_not_null().sum()).collect().to_dict()
    logger.debug(f"Non-null counts for {name}:")
    for col, count in counts.items():
        logger.debug(f"  {col}: {count[0]}")


def log_dataframe_info(df: pl.LazyFrame | pl.DataFrame, name: str) -> None:
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    logger.debug(f"{name} shape: {df.shape}")
    logger.debug(f"{name} schema: {df.schema}")
    logger.debug(f"{name} null counts:\n{df.null_count()}")
    logger.debug(f"{name} sample (5 rows):\n{df.head(5)}")
