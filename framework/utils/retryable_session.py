import os
import time

from requests import Session
from requests.exceptions import HTTPError, ConnectionError, ProxyError, Timeout, ConnectTimeout, SSLError

from framework.exceptions import HttpTransientException

from framework.utils.custlogging import LoggerProvider

logger = LoggerProvider().get_logger(os.path.basename(__file__))


class RetryableSession(Session):
    """
    Retries for 500, 501, 502, 503, 504 and network failures
    Can add/remove custom error codes as per requirements
    """

    def request(self, method, url, max_retry=5, backoff_factor=15, **kwargs):
        counter = 0
        while counter < max_retry:
            counter += 1
            try:
                r = super(RetryableSession, self).request(method, url, **kwargs)
                if r.status_code == 200:
                    return r
                elif r.status_code in [500, 501, 502, 503, 504]:
                    logger.warn(f"Received error response from api: {r.status_code}")
                    raise HTTPError()
                else:
                    raise Exception(
                        f"Unrecoverable status code {r.status_code} received from {method} {url}. Failing the batch...")
            except (HTTPError, SSLError, ConnectionError, ConnectTimeout, ProxyError, Timeout) as e:
                if counter < max_retry:
                    # backoff for 15s, 30s, 45s and exit
                    delay = backoff_factor * counter
                    logger.warn(f"Received retryable error from {method} {url}, retry count: {counter}")
                    time.sleep(delay)
                else:
                    logger.error(f"Failed to get data from {method} {url} {kwargs}")
                    raise HttpTransientException
