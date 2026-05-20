"""Rich-formatted logger used across all pipeline stages."""
import logging
from rich.console import Console
from rich.logging import RichHandler

console = Console()

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(console=console, rich_tracebacks=True, markup=True)],
)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
