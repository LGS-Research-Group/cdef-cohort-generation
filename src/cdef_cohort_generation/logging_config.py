import logging
from pathlib import Path
from typing import Any

from rich import print as rprint
from rich.console import Console
from rich.logging import RichHandler


def setup_logging() -> tuple[logging.Logger, Console]:
    console = Console()

    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "profile_data.log"

    logger = logging.getLogger("profile_data")
    logger.setLevel(logging.DEBUG)

    console_handler = RichHandler(
        console=console, rich_tracebacks=True, show_time=False, show_path=False
    )
    file_handler = logging.FileHandler(log_file, encoding="utf-8")

    console_format = logging.Formatter("%(message)s")
    file_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(console_format)
    file_handler.setFormatter(file_format)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger, console


logger, console = setup_logging()


def log(message: str, level: str = "info", **kwargs: Any) -> None:
    getattr(logger, level)(message)
    rprint(f"[{level.upper()}] {message}", **kwargs)
