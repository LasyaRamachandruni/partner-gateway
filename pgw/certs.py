"""A small internal certificate authority for mTLS between services.

    pgw certs --out certs/

Generates:
  ca.pem / ca.key                 the internal CA (trust anchor for both sides)
  vehicle-service.pem / .key      server certificate (SAN: vehicle-service, localhost, 127.0.0.1)
  partner-gateway.pem / .key      client certificate (CN=partner-gateway)

The vehicle service only accepts connections that present a certificate signed
by this CA, and then checks the client's common name against an allow-list. A
leaked network path alone is not enough to send commands to cars. In production
these would come from a managed CA (e.g. GCP Certificate Authority Service)
with short lifetimes and automatic rotation, and the private keys would live
in a secret manager, not on disk.
"""

from __future__ import annotations

import datetime as dt
import ipaddress
from dataclasses import dataclass
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


@dataclass
class Pair:
    cert_pem: bytes
    key_pem: bytes


def _key():
    return ec.generate_private_key(ec.SECP256R1())


def _pem_key(k) -> bytes:
    return k.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())


def _name(cn: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.ORGANIZATION_NAME, "partner-gateway dev CA"),
                      x509.NameAttribute(NameOID.COMMON_NAME, cn)])


def make_ca(cn: str = "pgw-internal-ca", days: int = 365) -> tuple[Pair, object, object]:
    key = _key()
    now = dt.datetime.now(dt.timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(_name(cn)).issuer_name(_name(cn)).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now - dt.timedelta(minutes=5))
            .not_valid_after(now + dt.timedelta(days=days))
            .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
            .add_extension(x509.KeyUsage(digital_signature=True, key_cert_sign=True, crl_sign=True,
                                         content_commitment=False, key_encipherment=False, data_encipherment=False,
                                         key_agreement=False, encipher_only=False, decipher_only=False), critical=True)
            .sign(key, hashes.SHA256()))
    return Pair(cert.public_bytes(serialization.Encoding.PEM), _pem_key(key)), cert, key


def issue(ca_cert, ca_key, cn: str, *, server: bool, sans: list[str] = (), days: int = 30) -> Pair:
    key = _key()
    now = dt.datetime.now(dt.timezone.utc)
    alt = []
    for s in sans:
        try:
            alt.append(x509.IPAddress(ipaddress.ip_address(s)))
        except ValueError:
            alt.append(x509.DNSName(s))
    b = (x509.CertificateBuilder().subject_name(_name(cn)).issuer_name(ca_cert.subject).public_key(key.public_key())
         .serial_number(x509.random_serial_number()).not_valid_before(now - dt.timedelta(minutes=5))
         .not_valid_after(now + dt.timedelta(days=days))
         .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
         .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH if server
                                               else ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False))
    if alt:
        b = b.add_extension(x509.SubjectAlternativeName(alt), critical=False)
    cert = b.sign(ca_key, hashes.SHA256())
    return Pair(cert.public_bytes(serialization.Encoding.PEM), _pem_key(key))


@dataclass
class Bundle:
    ca: Pair
    server: Pair
    client: Pair


def generate(server_cn: str = "vehicle-service", client_cn: str = "partner-gateway") -> Bundle:
    ca, ca_cert, ca_key = make_ca()
    server = issue(ca_cert, ca_key, server_cn, server=True, sans=[server_cn, "localhost", "127.0.0.1"])
    client = issue(ca_cert, ca_key, client_cn, server=False)
    return Bundle(ca, server, client)


def write(bundle: Bundle, out: str | Path, server_cn: str = "vehicle-service", client_cn: str = "partner-gateway") -> Path:
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    for name, pair in (("ca", bundle.ca), (server_cn, bundle.server), (client_cn, bundle.client)):
        (out / f"{name}.pem").write_bytes(pair.cert_pem)
        key = out / f"{name}.key"
        key.write_bytes(pair.key_pem)
        key.chmod(0o600)
    return out


def load(directory: str | Path, server_cn: str = "vehicle-service", client_cn: str = "partner-gateway") -> Bundle:
    d = Path(directory)
    r = lambda n: Pair((d / f"{n}.pem").read_bytes(), (d / f"{n}.key").read_bytes())  # noqa: E731
    return Bundle(r("ca"), r(server_cn), r(client_cn))
