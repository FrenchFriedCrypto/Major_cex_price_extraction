import time

import requests


RETRYABLE_STATUS_CODES = {400, 418, 429}
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_SLEEP_SECONDS = 1


def _response_body(response: requests.Response) -> str:
    body = str(getattr(response, "text", "") or "").strip()
    if not body:
        try:
            body = str(response.json()).strip()
        except ValueError:
            body = ""
    if len(body) > 500:
        return body[:500] + "..."
    return body


def _retry_reason(response: requests.Response, url: str) -> str:
    status_code = response.status_code
    if status_code in {418, 429}:
        prefix = "Rate limit response"
    elif status_code == 400:
        prefix = "HTTP 400 Bad Request response"
    else:
        prefix = "Retryable response"

    request_url = getattr(response, "url", None) or url
    reason = f"{prefix} from {request_url} (status {status_code})"
    body = _response_body(response)
    if body:
        reason = f"{reason}; body={body!r}"
    return reason


def request_with_retries(
    method: str,
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    data: object | None = None,
    json: object | None = None,
    timeout: int | float | None = None,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_sleep_seconds: int | float = DEFAULT_RETRY_SLEEP_SECONDS,
) -> requests.Response | None:
    last_failure = "unknown error"
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.request(
                method,
                url,
                params=params,
                headers=headers,
                data=data,
                json=json,
                timeout=timeout,
            )
            if response.status_code in RETRYABLE_STATUS_CODES or response.status_code >= 500:
                delay_seconds = retry_sleep_seconds * attempt
                last_failure = _retry_reason(response, url)
                if attempt < max_retries:
                    print(
                        f"[RETRY] {last_failure}. Attempt {attempt}/{max_retries}; "
                        f"sleeping {delay_seconds:g}s before retry."
                    )
                    time.sleep(delay_seconds)
                else:
                    print(f"[RETRY] {last_failure}. Attempt {attempt}/{max_retries}; no retries left.")
                continue

            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as exc:
            delay_seconds = retry_sleep_seconds * attempt
            last_failure = f"Request error for {url}: {exc}"
            if attempt < max_retries:
                print(
                    f"[RETRY] {last_failure}. Attempt {attempt}/{max_retries}; "
                    f"sleeping {delay_seconds:g}s before retry."
                )
                time.sleep(delay_seconds)
            else:
                print(f"[RETRY] {last_failure}. Attempt {attempt}/{max_retries}; no retries left.")

    print(f"[ERROR] Failed to retrieve {url} after {max_retries} attempts. Last failure: {last_failure}")
    return None


def request_json(
    method: str,
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    data: object | None = None,
    json: object | None = None,
    timeout: int | float | None = None,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_sleep_seconds: int | float = DEFAULT_RETRY_SLEEP_SECONDS,
) -> object | None:
    response = request_with_retries(
        method,
        url,
        params=params,
        headers=headers,
        data=data,
        json=json,
        timeout=timeout,
        max_retries=max_retries,
        retry_sleep_seconds=retry_sleep_seconds,
    )
    if response is None:
        return None
    try:
        return response.json()
    except ValueError as exc:
        print(f"[ERROR] Error parsing JSON response from {getattr(response, 'url', url)}: {exc}")
        return None
