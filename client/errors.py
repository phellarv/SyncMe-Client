import httpx

EXIT_AUTH = 78      # sysexits EX_CONFIG: systemd must not restart the service on this code


def is_auth_error(exc: BaseException) -> bool:
    """True only for HTTP 401, which is what the server answers for a bad or revoked key.

    A 403 never comes from the server for that: it is a proxy, WAF or load balancer deny rule
    (for instance HAProxy `http-request deny`), so it is handled like any other HTTP error.
    """
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 401


def describe_stream_error(exc: BaseException) -> str:
    """Short, log-safe text for an event stream failure: the type, plus status and path for an HTTP error."""
    if isinstance(exc, httpx.HTTPStatusError):
        return f"{type(exc).__name__}: HTTP {exc.response.status_code} for {exc.request.url.path}"
    return type(exc).__name__
