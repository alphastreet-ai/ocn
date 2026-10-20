"""Stage C must not hand Stage D a row with empty tags.

Two failure classes reach the same place in Stage C and must be treated
oppositely:

  TRANSPORT  a 504, a reset connection, a read past the timeout. The
             request never reached a model, so the same request is
             worth sending again. Measured live: one 504 on one row of
             a 45-row run left that row with every Stage C field null,
             and re-running that one row succeeded on its first attempt.

  RESPONSE   a reply that arrived but cannot be parsed or validated.
             Sending it again buys the same reply - this codebase's
             filing classifier measured 54 rows failing a parse twice,
             "every one of the 54 failed IDENTICALLY on both attempts".

So the retry must fire on the first class and must NOT fire on the
second. A test that only checked "a 504 is retried" would pass just as
happily against code that retried everything, which would double the
cost of every genuinely bad response.
"""
from __future__ import annotations

import io
import sys
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError, URLError

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipeline import geopolitical_signal_layer2 as l2  # noqa: E402


def _http_error(code: int) -> HTTPError:
    return HTTPError("http://x", code, "err", {}, None)


def _ok_body(content: str = '{"channel": "sanctions"}') -> io.BytesIO:
    """Minimal OpenAI-shaped response body."""
    import json
    payload = {"choices": [{"message": {"content": content}}]}
    return io.BytesIO(json.dumps(payload).encode())


class _Resp:
    """urlopen's context-manager contract, which _post_with_retry uses."""

    def __init__(self, body: io.BytesIO) -> None:
        self._body = body

    def __enter__(self):
        return self._body

    def __exit__(self, *exc):
        return False


# --------------------------------------------------------------------
# transport failures: retried
# --------------------------------------------------------------------

@pytest.mark.parametrize("code", sorted(l2._TRANSIENT_HTTP_STATUSES))
def test_transient_status_is_retried_and_can_succeed(code: int) -> None:
    """Every status in the transient set gets another attempt, and a
    success on that attempt is returned normally - the row is tagged, so
    it never reaches Stage D empty."""
    calls = []

    def _fake(req, timeout=None):
        calls.append(1)
        if len(calls) == 1:
            raise _http_error(code)
        return _Resp(_ok_body())

    with patch.object(l2, "urlopen", _fake), patch.object(l2.time, "sleep"):
        out = l2._post_with_retry(object(), 30)

    assert len(calls) == 2, f"status {code} should have been retried once"
    assert out["choices"][0]["message"]["content"] == '{"channel": "sanctions"}'


def test_url_error_is_retried() -> None:
    """A transport error carries no status code; it is the same class as
    a 504 and must retry too."""
    calls = []

    def _fake(req, timeout=None):
        calls.append(1)
        if len(calls) < 3:
            raise URLError("connection reset")
        return _Resp(_ok_body())

    with patch.object(l2, "urlopen", _fake), patch.object(l2.time, "sleep"):
        l2._post_with_retry(object(), 30)

    assert len(calls) == 3


def test_retry_gives_up_after_max_attempts_and_reraises() -> None:
    """A sustained outage must still surface as an exception, so the
    caller's fail-open handler - the single place that decides what a
    permanently failed row looks like - still runs."""
    calls = []

    def _fake(req, timeout=None):
        calls.append(1)
        raise _http_error(504)

    with patch.object(l2, "urlopen", _fake), patch.object(l2.time, "sleep"):
        with pytest.raises(HTTPError):
            l2._post_with_retry(object(), 30)

    assert len(calls) == l2._HTTP_MAX_ATTEMPTS


def test_backoff_sleeps_between_attempts_not_after_the_last() -> None:
    """Backoff is 1s then 2s, and the final failure returns immediately -
    sleeping after the last attempt would hold a concurrency slot for
    nothing."""
    slept: list[float] = []

    def _fake(req, timeout=None):
        raise _http_error(503)

    with patch.object(l2, "urlopen", _fake), \
            patch.object(l2.time, "sleep", slept.append):
        with pytest.raises(HTTPError):
            l2._post_with_retry(object(), 30)

    assert slept == [1, 2]


# --------------------------------------------------------------------
# response failures: NOT retried
# --------------------------------------------------------------------

@pytest.mark.parametrize("code", [400, 401, 403, 404, 422])
def test_client_error_is_not_retried(code: int) -> None:
    """The request itself is wrong and will be wrong again. Retrying
    doubles the cost and changes nothing."""
    calls = []

    def _fake(req, timeout=None):
        calls.append(1)
        raise _http_error(code)

    with patch.object(l2, "urlopen", _fake), patch.object(l2.time, "sleep"):
        with pytest.raises(HTTPError):
            l2._post_with_retry(object(), 30)

    assert len(calls) == 1, f"status {code} must not be retried"


def test_unparseable_response_is_not_retried_by_the_transport_layer() -> None:
    """A reply that arrived is a RESPONSE failure. _post_with_retry hands
    it straight back; the parse happens above it and fails open there on
    the first try."""
    calls = []

    def _fake(req, timeout=None):
        calls.append(1)
        return _Resp(io.BytesIO(b"not json at all"))

    with patch.object(l2, "urlopen", _fake), patch.object(l2.time, "sleep"):
        with pytest.raises(Exception):
            l2._post_with_retry(object(), 30)

    assert len(calls) == 1


def test_happy_path_makes_exactly_one_call() -> None:
    """The common case must not pay for the retry machinery."""
    calls = []

    def _fake(req, timeout=None):
        calls.append(1)
        return _Resp(_ok_body())

    with patch.object(l2, "urlopen", _fake), patch.object(l2.time, "sleep"):
        l2._post_with_retry(object(), 30)

    assert len(calls) == 1
