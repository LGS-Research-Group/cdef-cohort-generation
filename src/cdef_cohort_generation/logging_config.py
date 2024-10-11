import logging
from pathlib import Path
from typing import Any, Literal

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
            format="%(message)s",
            datefmt="[%X]",
            handlers=[
                RichHandler(
                    console=self.console,
                    rich_tracebacks=True,
                    show_time=False,
                    show_path=False,
                ),
                RichHandler(
                    console=Console(file=Path.open(log_file, "a", encoding="utf-8")),
                    show_time=True,
                    show_path=True,
                ),
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


def setup_logging() -> RichLogger:
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "profile_data.log"
    return RichLogger("profile_data", log_file)


logger = setup_logging()


def log(message: str, level: LogLevel = "info", **kwargs: Any) -> None:
    logger.log(message, level, **kwargs)
