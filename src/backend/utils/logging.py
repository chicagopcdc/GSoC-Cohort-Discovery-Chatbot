"""
Unified logging configuration for PCDC Chatbot Backend

This module provides centralized logging setup with:
- Structured logging with JSON formatting
- Different log levels for different components
- File and console output configuration
- Request/response logging for API endpoints
"""

import logging
import logging.config
import json
import sys
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

from core.config import config


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging"""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON"""
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        # Add extra fields if they exist
        if hasattr(record, 'trace_id'):
            log_data['trace_id'] = record.trace_id
        if hasattr(record, 'session_id'):
            log_data['session_id'] = record.session_id
        if hasattr(record, 'step'):
            log_data['step'] = record.step
        if hasattr(record, 'duration'):
            log_data['duration'] = record.duration
        
        # Add exception information if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_data, ensure_ascii=False)


def setup_logging(
    log_level: str = "INFO",
    log_to_file: bool = True,
    log_to_console: bool = True,
    log_file_path: Optional[str] = None
) -> None:
    """
    Setup centralized logging configuration
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file: Whether to log to file
        log_to_console: Whether to log to console
        log_file_path: Custom log file path (optional)
    """
    
    # Ensure log directory exists
    config.ensure_directories()
    
    # Default log file path
    if log_file_path is None:
        log_file_path = str(Path(config.LOG_DIR) / "pcdc_chatbot.log")
    
    # Logging configuration
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {
                "()": JSONFormatter
            },
            "simple": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            }
        },
        "handlers": {},
        "loggers": {
            "": {  # Root logger
                "level": log_level,
                "handlers": []
            },
            "pcdc_chatbot": {
                "level": log_level,
                "handlers": [],
                "propagate": False
            },
            "uvicorn": {
                "level": "INFO",
                "handlers": [],
                "propagate": False
            }
        }
    }
    
    # Add console handler if requested
    if log_to_console:
        logging_config["handlers"]["console"] = {
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
            "formatter": "simple",
            "level": log_level
        }
        logging_config["loggers"][""]["handlers"].append("console")
        logging_config["loggers"]["pcdc_chatbot"]["handlers"].append("console")
        logging_config["loggers"]["uvicorn"]["handlers"].append("console")
    
    # Add file handler if requested
    if log_to_file:
        logging_config["handlers"]["file"] = {
            "class": "logging.FileHandler",
            "filename": log_file_path,
            "formatter": "json",
            "level": log_level,
            "encoding": "utf-8"
        }
        logging_config["loggers"][""]["handlers"].append("file")
        logging_config["loggers"]["pcdc_chatbot"]["handlers"].append("file")
        logging_config["loggers"]["uvicorn"]["handlers"].append("file")
    
    # Apply configuration
    logging.config.dictConfig(logging_config)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the specified name
    
    Args:
        name: Logger name (usually __name__)
    
    Returns:
        Configured logger instance
    """
    return logging.getLogger(f"pcdc_chatbot.{name}")


class PipelineLogger:
    """Specialized logger for pipeline step tracking"""
    
    def __init__(self, trace_id: Optional[str] = None, session_id: Optional[str] = None):
        self.logger = get_logger("pipeline")
        self.trace_id = trace_id
        self.session_id = session_id
    
    def log_step_start(self, step: str, inputs: Dict[str, Any]) -> None:
        """Log the start of a pipeline step"""
        self.logger.info(
            f"Starting step: {step}",
            extra={
                "trace_id": self.trace_id,
                "session_id": self.session_id,
                "step": step,
                "inputs": inputs,
                "event": "step_start"
            }
        )
    
    def log_step_end(self, step: str, outputs: Dict[str, Any], duration: float) -> None:
        """Log the end of a pipeline step"""
        self.logger.info(
            f"Completed step: {step}",
            extra={
                "trace_id": self.trace_id,
                "session_id": self.session_id,
                "step": step,
                "outputs": outputs,
                "duration": duration,
                "event": "step_end"
            }
        )
    
    def log_step_error(self, step: str, error: Exception, inputs: Dict[str, Any]) -> None:
        """Log an error in a pipeline step"""
        self.logger.error(
            f"Error in step: {step}",
            extra={
                "trace_id": self.trace_id,
                "session_id": self.session_id,
                "step": step,
                "inputs": inputs,
                "error": str(error),
                "event": "step_error"
            },
            exc_info=True
        )


# Initialize logging on module import
setup_logging() 