import logging
from typing import Optional


class VerbosityAdapter:
    """
    A logging adapter that supports three verbosity levels.

    This adapter wraps Python's logging module and filters messages based on
    the specified verbosity level:
    - "high": All messages (INFO, WARNING, ERROR)
    - "moderate": WARNING and ERROR, plus key lifecycle INFO messages
    - "low": No messages (silent operation)

    Parameters
    ----------
    verbosity : str
        Verbosity level: "high", "moderate", or "low"
    logger_name : str, optional
        Name of the underlying logger. Default is "opstrat_backtester"
    """

    def __init__(self, verbosity: str = "high", logger_name: str = "opstrat_backtester"):
        self.verbosity = verbosity.lower()
        self.logger = logging.getLogger(logger_name)

        # Set the underlying logger level to INFO so all messages reach the adapter
        self.logger.setLevel(logging.INFO)

        # Create console handler if none exists
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(levelname)s: %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def info(self, message: str, always_show: bool = False) -> None:
        """
        Log an info message.

        Parameters
        ----------
        message : str
            The message to log
        always_show : bool, optional
            If True, show this message even in moderate/low verbosity
        """
        if self.verbosity == "high":
            self.logger.info(message)
        elif self.verbosity == "moderate" and always_show:
            self.logger.info(message)
        # low verbosity: no info messages

    def warning(self, message: str) -> None:
        """
        Log a warning message.

        Always shown in high and moderate verbosity, suppressed in low.
        """
        if self.verbosity in ["high", "moderate"]:
            self.logger.warning(message)
        # low verbosity: no warning messages

    def error(self, message: str) -> None:
        """
        Log an error message.

        Always shown regardless of verbosity level.
        """
        self.logger.error(message)

    def debug(self, message: str) -> None:
        """
        Log a debug message.

        Only shown in high verbosity.
        """
        if self.verbosity == "high":
            self.logger.debug(message)
