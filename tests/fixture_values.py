"""Fixture literals detect-secrets flags, kept in one place.

`.secrets.baseline` anchors each finding by line number, so a fake
credential or a known-answer digest spelled inline in a test module moves
whenever a test lands above it, and `make security-secrets` goes red on a
commit that touched no credential. Everything the
scanner fires on lives here instead, constants first, and the test modules
import it: these line numbers change only when this file does.

The module name does not match pytest's `test_*.py`, so it is never
collected. None of these values is a secret.
"""

# Bogus AWS credentials for moto and directly built boto3 clients, so
# nothing ever reaches the real credential chain.
FAKE_AWS_ACCESS_KEY_ID = "testing"
FAKE_AWS_SECRET_ACCESS_KEY = "testing"

# What the `config` tests type at the access-key and secret-key prompts.
CONFIG_PROMPT_ACCESS_KEY = "ACCESS123"
CONFIG_PROMPT_SECRET_KEY = "SECRET456"

# Known-answer digests.
HELLO = b"hello"
HELLO_MD5 = "5d41402abc4b2a76b9719d911017c592"
HELLO_SHA1 = "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"
HELLO_SHA256 = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

EMPTY_MD5 = "d41d8cd98f00b204e9800998ecf8427e"
EMPTY_SHA1 = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

# Frozen manifest identity of `bagit.txt` = b"BagIt-Version: 1.0\n"
# (test_manifest's golden vector). It must never change without a
# coordinated bump of every inventory snapshot already in the wild.
GOLDEN_BAGIT_MD5 = "9e28fcefb9ca3530e043b6334904fd7c"
GOLDEN_BAGIT_SHA1 = "922a2b6762717eabcade85ba446f13b1d861f250"
GOLDEN_BAGIT_SHA256 = "02d3510b13d2351380e0509feddfd17acdcb605d82c79e8e61a3ff1f1cdb5684"
