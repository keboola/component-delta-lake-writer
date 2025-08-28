import urllib.request
import json
import time
import urllib.error
import logging


class SAPIClient:
    def __init__(self, base_url, sapi_token, retry_attempts=3):
        self.base_url = base_url.rstrip("/")
        self.headers = {"X-StorageApi-Token": sapi_token}
        self.retry_attempts = retry_attempts

    def get_table_detail(self, table_id):
        url = f"{self.base_url}/v2/storage/tables/{table_id}"
        last_exception = None

        for attempt in range(self.retry_attempts):
            try:
                req = urllib.request.Request(url, headers=self.headers)
                with urllib.request.urlopen(req) as response:
                    response_data = response.read().decode("utf-8")
                    return json.loads(response_data)
            except Exception as e:
                last_exception = e
                logging.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < self.retry_attempts - 1:
                    time.sleep(attempt + 1)

        raise last_exception
