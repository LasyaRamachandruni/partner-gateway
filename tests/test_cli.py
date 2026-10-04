"""The pgw command: certificates and a short chaos run."""

import stat

from pgw import certs
from pgw.cli import main


def test_certs_command(tmp_path, capsys):
    assert main(["certs", "--out", str(tmp_path)]) == 0
    for name in ("ca", "vehicle-service", "partner-gateway"):
        assert (tmp_path / f"{name}.pem").exists()
        assert stat.S_IMODE((tmp_path / f"{name}.key").stat().st_mode) == 0o600  # private keys not world-readable
    bundle = certs.load(tmp_path)
    assert b"BEGIN CERTIFICATE" in bundle.server.cert_pem


def test_chaos_command_short_run(tmp_path, capsys):
    assert main(["chaos", "--out", str(tmp_path), "--duration", "1.5", "--workers", "4", "--repeats", "1"]) == 0
    report = (tmp_path / "CHAOS.md").read_text()
    assert "Retries + circuit breaker" in report and (tmp_path / "outage_timeline.png").stat().st_size > 0
