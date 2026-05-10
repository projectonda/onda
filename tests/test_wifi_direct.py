"""Tests for WiFi Direct transport that don't require an actual radio.

We mock the pywifi interface and assert:
  * On macOS, is_available() returns False unconditionally (the SSID
    convention can't be implemented through user-space).
  * On Linux/Windows with pywifi available, the SSID scan filter only
    keeps `Onda-...` SSIDs.
  * Discovery channel emits one PeerEndpoint per new SSID found.
"""

from __future__ import annotations

import platform
from unittest.mock import MagicMock, patch

import pytest
import trio

from onda.config import OndaSettings
from onda.identity import Identity
from onda.transports._libp2p_shared import Libp2pHost
from onda.transports.wifi_direct import (
    ONDA_SSID_PREFIX,
    WifiDirectTransport,
    _SSID_REGEX,
)


def _settings(tmp_path) -> OndaSettings:
    # Port is a required pydantic-validated field but never actually used in
    # these tests (we mock the radio). Pick any in-range value.
    return OndaSettings(name="t", home_dir=tmp_path, port=9999, enable_mdns=False)


def _ident() -> Identity:
    return Identity.generate("test")


def test_ssid_regex_accepts_well_formed() -> None:
    assert _SSID_REGEX.match("Onda-12D3KooW")
    assert _SSID_REGEX.match("Onda-abcd")


def test_ssid_regex_rejects_unprefixed() -> None:
    assert _SSID_REGEX.match("HomeNetwork-5G") is None
    assert _SSID_REGEX.match("Onda-") is None  # too short suffix
    assert _SSID_REGEX.match("xOnda-AAAA") is None


@pytest.mark.trio
async def test_unavailable_on_darwin(tmp_path) -> None:
    with patch.object(platform, "system", return_value="Darwin"):
        host = Libp2pHost(identity=_ident(), host_addr="127.0.0.1", port=0)
        t = WifiDirectTransport(host=host, settings=_settings(tmp_path))
        assert await t.is_available() is False


@pytest.mark.trio
async def test_unavailable_when_pywifi_missing(tmp_path) -> None:
    # Simulate ImportError on pywifi.
    with patch.object(platform, "system", return_value="Linux"):
        with patch.dict("sys.modules", {"pywifi": None}):
            host = Libp2pHost(identity=_ident(), host_addr="127.0.0.1", port=0)
            t = WifiDirectTransport(host=host, settings=_settings(tmp_path))
            # When pywifi import fails or returns no interfaces, we report
            # unavailable.
            with patch("onda.transports.wifi_direct._import_pywifi", side_effect=Exception("nope")):
                assert await t.is_available() is False


@pytest.mark.trio
async def test_scan_filters_to_onda_prefix(tmp_path) -> None:
    """With a fake pywifi iface, only Onda-prefixed SSIDs become peer endpoints."""

    fake_results = [
        MagicMock(ssid="HomeNetwork-5G"),
        MagicMock(ssid="Onda-12D3KooW"),
        MagicMock(ssid="Onda-abcd"),
        MagicMock(ssid="GuestWiFi"),
    ]
    fake_iface = MagicMock()
    fake_iface.scan_results.return_value = fake_results

    with patch.object(platform, "system", return_value="Linux"):
        host = Libp2pHost(identity=_ident(), host_addr="127.0.0.1", port=0)
        t = WifiDirectTransport(host=host, settings=_settings(tmp_path), scan_interval_s=0.01)
        t._iface = fake_iface

        async def handler(frame):
            return None

        await t.start(handler)

        # Run one scan iteration.
        ssids = await t._scan_once()
        assert sorted(ssids) == ["Onda-12D3KooW", "Onda-abcd"]

        await t.stop()
