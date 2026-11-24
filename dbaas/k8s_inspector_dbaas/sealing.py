"""Seals a secret for the controller (`k8s_inspector/dbaas_seal.py` opens it; the two packages share no code, so the
scheme is written in both and a test compares them).

The up Job reports through its termination message, which anyone who can read Pods can read. A vendor-generated admin
password goes into it sealed with the run's own password (`DBAAS_PASSWORD`, from the Secret `<run>-db`, which only the
controller and the Job's `secretKeyRef` see): HMAC-SHA256 in counter mode as the stream cipher, then an HMAC tag over
nonce and ciphertext. The key is a random password of 192 bits, so it needs no slow key derivation.
"""

import base64
import hashlib
import hmac
import os

NONCE_BYTES = 16


def _key(password: str, purpose: bytes) -> bytes:
    return hmac.digest(password.encode(), purpose, hashlib.sha256)


def _stream(key: bytes, nonce: bytes, length: int) -> bytes:
    blocks = (hmac.digest(key, nonce + n.to_bytes(4), hashlib.sha256) for n in range((length + 31) // 32))
    return b"".join(blocks)[:length]


def seal(password: str, text: str) -> str:
    """`text`, encrypted and authenticated with `password`, as URL-safe base64."""
    nonce, plain = os.urandom(NONCE_BYTES), text.encode()
    body = bytes(a ^ b for a, b in zip(plain, _stream(_key(password, b"enc"), nonce, len(plain)), strict=True))
    tag = hmac.digest(_key(password, b"mac"), nonce + body, hashlib.sha256)
    return base64.urlsafe_b64encode(nonce + body + tag).decode()
