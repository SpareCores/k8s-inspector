"""Opens what the provisioner image sealed (`k8s_inspector_dbaas/sealing.py`; the two packages share no code, so the
scheme is written in both and a test of the image compares them).

A vendor-generated admin password travels from the up Job to the controller in the Job's termination message, which
anyone who can read Pods can read. The Job seals it with the run's own password (the Secret `<run>-db`, which only the
controller and the Job's `secretKeyRef` see): HMAC-SHA256 in counter mode as the stream cipher, then an HMAC tag over
nonce and ciphertext. The key is a random password of 192 bits, so it needs no slow key derivation.
"""

import base64
import hashlib
import hmac

NONCE_BYTES = 16
TAG_BYTES = 32


def _key(password: str, purpose: bytes) -> bytes:
    return hmac.digest(password.encode(), purpose, hashlib.sha256)


def _stream(key: bytes, nonce: bytes, length: int) -> bytes:
    blocks = (hmac.digest(key, nonce + n.to_bytes(4), hashlib.sha256) for n in range((length + 31) // 32))
    return b"".join(blocks)[:length]


def unseal(password: str, token: str) -> str:
    """The text that `sealing.seal` made of it with the same password. Raise `ValueError` for anything else."""
    try:
        raw = base64.urlsafe_b64decode(token.encode())
    except ValueError as e:
        raise ValueError("not a sealed value") from e
    nonce, body, tag = raw[:NONCE_BYTES], raw[NONCE_BYTES:-TAG_BYTES], raw[-TAG_BYTES:]
    if len(raw) < NONCE_BYTES + TAG_BYTES or not hmac.compare_digest(
        tag, hmac.digest(_key(password, b"mac"), nonce + body, hashlib.sha256)
    ):
        raise ValueError("not sealed with this password")
    return bytes(a ^ b for a, b in zip(body, _stream(_key(password, b"enc"), nonce, len(body)), strict=True)).decode()
