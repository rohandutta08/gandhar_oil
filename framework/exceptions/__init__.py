import sys
import traceback


class WorkerRetryableException(Exception):
    """Exception raised for errors that are potentially recoverable and the action can be retried."""

    def __init__(self, original_exception: Exception, message="An error occurred that can be retried."):
        self.original_exception = original_exception
        full_message = f"WorkerRetryableException: {message} Original error: {str(original_exception)}"
        super().__init__(full_message)


class WorkerNonRetryableException(Exception):
    """Exception raised for errors that are not recoverable and the action should not be retried."""

    def __init__(self, original_exception: Exception, message="An error occurred that cannot be retried."):
        self.original_exception = original_exception
        full_message = f"WorkerNonRetryableException: {message} Original error: {str(original_exception)}"
        super().__init__(full_message)


def get_formatted_exception(e: Exception) -> str:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    formatted_traceback = ''.join(traceback.format_tb(exc_traceback))
    lineno = exc_traceback.tb_lineno
    exception_name = e.__class__.__name__
    return f"""{exception_name} processing message  
                                            occurred line {lineno}: {e}
                                            Traceback: {formatted_traceback}"""


class HttpTransientException(Exception):
    """Raised when http server timeouts"""
    pass
