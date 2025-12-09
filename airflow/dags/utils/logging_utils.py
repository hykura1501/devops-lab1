import logging
from datetime import datetime


class DataOpsLogger:
    """Logger for DataOps pipeline events and metrics"""

    def __init__(self, pipeline_name, component_name):
        self.pipeline_name = pipeline_name
        self.component_name = component_name
        self.logger = logging.getLogger(f"{pipeline_name}.{component_name}")

    def log_event(self, event_type, message, level="info"):
        """Log a pipeline event"""
        log_message = f"[{self.pipeline_name}] [{self.component_name}] {event_type}: {message}"

        if level == "info":
            self.logger.info(log_message)
        elif level == "warning":
            self.logger.warning(log_message)
        elif level == "error":
            self.logger.error(log_message)
        else:
            self.logger.debug(log_message)

        return {
            "timestamp": datetime.now().isoformat(),
            "pipeline": self.pipeline_name,
            "component": self.component_name,
            "event_type": event_type,
            "message": message,
            "level": level,
        }

    def log_metric(self, metric_name, value, unit=None):
        """Log a pipeline metric"""
        unit_str = f" {unit}" if unit else ""
        message = f"Metric: {metric_name} = {value}{unit_str}"
        self.logger.info(f"[{self.pipeline_name}] [{self.component_name}] {message}")

        return {
            "timestamp": datetime.now().isoformat(),
            "pipeline": self.pipeline_name,
            "component": self.component_name,
            "metric_name": metric_name,
            "value": value,
            "unit": unit,
        }


def setup_logger(name, log_file=None, level=logging.INFO):
    """Setup a logger with console and optional file handler"""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (optional)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def log_task_execution(task_id, status, duration=None, error=None):
    """Log task execution details"""
    logger = logging.getLogger("airflow.task")
    log_data = {
        "task_id": task_id,
        "status": status,
        "timestamp": datetime.now().isoformat(),
        "duration": duration,
        "error": str(error) if error else None,
    }

    if status == "success":
        logger.info(f"Task completed: {log_data}")
    elif status == "failed":
        logger.error(f"Task failed: {log_data}")
    else:
        logger.warning(f"Task status: {log_data}")

    return log_data