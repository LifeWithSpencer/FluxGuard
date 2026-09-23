"""
tests/test_identify_caller.py

Regression tests for middleware/rate_limit.py::identify_caller.

Context: the original implementation trusted the FIRST entry in
X-Forwarded-For. Paired with Nginx's default $proxy_add_x_forwarded_for
(which APPENDS to whatever XFF a client already sent rather than
overwriting it), a client sending its own XFF header landed first in the
resulting header - making the rate-limit identity attacker-controlled and
letting any caller dodge its bucket by sending a fresh spoofed value per
request.

The fix has two parts, and this file tests the part that lives in
application code:
  1. nginx.conf now overwrites X-Forwarded-For with $remote_addr instead of
     appending to it (not exercised here - that's a config, not code).
  2. identify_caller() now trusts the LAST entry, not the first, as
     defense-in-depth: even if a future config change reintroduces
     appending, the attacker-supplied value is no longer the one selected.

Run:
    pytest tests/test_identify_caller.py -v
"""

from __future__ import annotations

from starlette.requests import Request

from middleware.rate_limit import identify_caller


def _make_request(headers: dict[str, str], client_host: str = "10.0.0.1") -> Request:
    """Build a minimal Starlette Request from an ASGI scope - no server
    needed, identify_caller only reads headers and request.client."""
    encoded_headers = [
        (k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers.items()
    ]
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/resource",
        "headers": encoded_headers,
        "client": (client_host, 12345),
    }
    return Request(scope)


class TestIdentificationPriority:
    def test_api_key_wins_over_everything(self):
        req = _make_request({
            "x-api-key": "sk-real-key",
            "authorization": "Bearer some-token",
            "x-forwarded-for": "1.2.3.4",
        })
        caller_id, method = identify_caller(req)
        assert (caller_id, method) == ("sk-real-key", "api_key")

    def test_bearer_token_wins_over_ip(self):
        req = _make_request({"authorization": "Bearer abc123"})
        caller_id, method = identify_caller(req)
        assert (caller_id, method) == ("abc123", "bearer_token")

    def test_falls_back_to_client_host_with_no_headers(self):
        req = _make_request({}, client_host="203.0.113.9")
        caller_id, method = identify_caller(req)
        assert (caller_id, method) == ("203.0.113.9", "ip")


class TestForwardedForTrust:
    def test_single_trusted_value_from_edge(self):
        """The expected shape after the Nginx fix: exactly one value,
        set by the trusted edge, nothing client-influenced."""
        req = _make_request({"x-forwarded-for": "198.51.100.7"})
        caller_id, method = identify_caller(req)
        assert (caller_id, method) == ("198.51.100.7", "ip")

    def test_spoofed_first_entry_is_not_trusted(self):
        """Regression test for the actual bypass: simulates what an
        appending proxy would produce if a client sent its own XFF value -
        attacker's value first, real client IP last. Before the fix,
        identify_caller took index [0] and returned the attacker's value.
        """
        attacker_supplied = "1.2.3.4"
        real_client_ip = "203.0.113.55"
        req = _make_request({"x-forwarded-for": f"{attacker_supplied}, {real_client_ip}"})

        caller_id, method = identify_caller(req)

        assert caller_id == real_client_ip, (
            "identify_caller trusted an attacker-controlled entry - "
            "the X-Forwarded-For spoofing bypass has regressed"
        )
        assert caller_id != attacker_supplied
        assert method == "ip"

    def test_two_spoofed_requests_now_collide_on_real_ip(self):
        """Directly demonstrates the bypass is closed: two requests that
        previously would have produced two different (attacker-chosen)
        caller_ids now produce the SAME caller_id, because both are
        resolved to the one value an appending proxy could not have
        forged - the last entry."""
        real_client_ip = "203.0.113.55"
        req1 = _make_request({"x-forwarded-for": f"attacker-value-one, {real_client_ip}"})
        req2 = _make_request({"x-forwarded-for": f"attacker-value-two, {real_client_ip}"})

        caller_id_1, _ = identify_caller(req1)
        caller_id_2, _ = identify_caller(req2)

        assert caller_id_1 == caller_id_2 == real_client_ip


class TestCloudflareConnectingIP:
    def test_cf_connecting_ip_preferred_over_xff(self):
        """Render deployments sit behind Cloudflare. CF-Connecting-IP is set
        directly by Cloudflare's edge (not appended to, unlike XFF), so it
        should win when present - added after observing inconsistent
        identity resolution in production that XFF's multi-hop ordering
        couldn't reliably account for on that specific platform."""
        req = _make_request({
            "cf-connecting-ip": "203.0.113.99",
            "x-forwarded-for": "1.2.3.4, 203.0.113.99",
        })
        caller_id, method = identify_caller(req)
        assert (caller_id, method) == ("203.0.113.99", "ip")

    def test_falls_back_to_xff_when_no_cf_header(self):
        """Local Docker Compose (no Cloudflare in front) has no
        CF-Connecting-IP at all - must still fall back to the XFF logic."""
        req = _make_request({"x-forwarded-for": "198.51.100.7"})
        caller_id, method = identify_caller(req)
        assert (caller_id, method) == ("198.51.100.7", "ip")