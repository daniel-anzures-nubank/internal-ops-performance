"""Unit tests for the ``gsheets`` credential loader (pure, no network).

Covers the per-field ``GOOGLE_SA_*`` reassembly path used on Databricks, where
the SA key is stored one secret-key-per-field in ``nu-mx-internal-ops-sa-secret``.
All values below are obviously fake test data, never real credentials.
"""

import gsheets

# NOT VALID — test data only. A stand-in for the private_key value carrying
# literal "\n" escapes (the reassembly path unescapes them); intentionally not a
# real PEM so it can't be mistaken for a credential.
mock_escaped_key = "NU_TEST_line1\\nline2\\nline3\\n"


def test_credentials_from_fields_reassembles_and_unescapes_private_key() -> None:
    environ = {
        "GOOGLE_SA_TYPE": "service_account",
        "GOOGLE_SA_PROJECT_ID": "nu-mx-internal-ops",
        "GOOGLE_SA_PRIVATE_KEY_ID": "NU_TEST_keyid",
        "GOOGLE_SA_PRIVATE_KEY": mock_escaped_key,
        "GOOGLE_SA_CLIENT_EMAIL": "sa@example.com",
        "GOOGLE_SA_CLIENT_ID": "111",
        "GOOGLE_SA_AUTH_URI": "https://accounts.google.com/o/oauth2/auth",
        "GOOGLE_SA_TOKEN_URI": "https://oauth2.googleapis.com/token",
        "GOOGLE_SA_AUTH_PROVIDER_X509_CERT_URL": "https://www.googleapis.com/oauth2/v1/certs",
        "GOOGLE_SA_CLIENT_X509_CERT_URL": "https://www.googleapis.com/robot/v1/x",
        "GOOGLE_SA_UNIVERSE_DOMAIN": "googleapis.com",
    }

    info = gsheets._credentials_from_fields(environ)

    assert info is not None
    assert info["type"] == "service_account"
    assert info["project_id"] == "nu-mx-internal-ops"
    assert info["client_email"] == "sa@example.com"
    # literal "\n" is un-escaped to real newlines; no leftover backslash-n.
    assert "\\n" not in info["private_key"]
    assert info["private_key"] == "NU_TEST_line1\nline2\nline3\n"
    assert info["private_key"].count("\n") == 3


def test_credentials_from_fields_real_newlines_are_noop() -> None:
    already_unescaped = "NU_TEST_line1\nline2\n"  # NOT VALID — test data only
    info = gsheets._credentials_from_fields({"GOOGLE_SA_PRIVATE_KEY": already_unescaped})

    assert info == {"private_key": already_unescaped}


def test_credentials_from_fields_none_when_no_sa_vars() -> None:
    assert gsheets._credentials_from_fields({"UNRELATED": "x"}) is None


def test_credentials_from_fields_assembles_partial_set() -> None:
    # We assemble whatever GOOGLE_SA_* vars are present; google-auth (not us)
    # decides which fields are required.
    info = gsheets._credentials_from_fields(
        {"GOOGLE_SA_TYPE": "service_account", "GOOGLE_SA_CLIENT_EMAIL": "sa@example.com"}
    )

    assert info == {"type": "service_account", "client_email": "sa@example.com"}
