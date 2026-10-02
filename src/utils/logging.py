import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict
from rich.logging import RichHandler
from rich.console import Console

console = Console()

class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_obj: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_obj["exc_info"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key not in {"name", "msg", "args", "created", "filename", "funcName", "levelname", "levelno", "lineno", "module", "msecs", "message", "exc_info", "exc_text", "stack_info", "pathname", "process", "processName", "relativeCreated", "thread", "threadName", "taskName"}:
                log_obj[key] = value
        return json.dumps(log_obj, ensure_ascii=False)

def setup_logging(log_file: Path, level: int = logging.INFO) -> logging.Logger:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(JSONFormatter())
    file_handler.setLevel(level)
    
    rich_handler = RichHandler(console=console, rich_tracebacks=True, markup=True)
    rich_handler.setLevel(level)
    
    logger = logging.getLogger("pipeline")
    logger.setLevel(level)
    logger.handlers = [file_handler, rich_handler]
    logger.propagate = False
    
    return logger

def get_logger(name: str = "pipeline") -> logging.Logger:
    return logging.getLogger(name)