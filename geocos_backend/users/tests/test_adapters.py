from __future__ import annotations

import pytest
from django.test import override_settings

from geocos_backend.users.adapters import AccountAdapter, SocialAccountAdapter


class TestAccountAdapter:
    def test_is_open_for_signup_with_setting_true(self):
        """Test is_open_for_signup when ACCOUNT_ALLOW_REGISTRATION is True."""
        with override_settings(ACCOUNT_ALLOW_REGISTRATION=True):
            adapter = AccountAdapter()
            assert adapter.is_open_for_signup(None) is True

    def test_is_open_for_signup_with_setting_false(self):
        """Test is_open_for_signup when ACCOUNT_ALLOW_REGISTRATION is False."""
        with override_settings(ACCOUNT_ALLOW_REGISTRATION=False):
            adapter = AccountAdapter()
            assert adapter.is_open_for_signup(None) is False

    def test_is_open_for_signup_default(self):
        """Test is_open_for_signup with default value."""
        # Default is True
        adapter = AccountAdapter()
        assert adapter.is_open_for_signup(None) is True


class TestSocialAccountAdapter:
    def test_is_open_for_signup_with_setting_true(self):
        """Test is_open_for_signup when ACCOUNT_ALLOW_REGISTRATION is True."""
        with override_settings(ACCOUNT_ALLOW_REGISTRATION=True):
            adapter = SocialAccountAdapter()
            assert adapter.is_open_for_signup(None, None) is True

    def test_is_open_for_signup_with_setting_false(self):
        """Test is_open_for_signup when ACCOUNT_ALLOW_REGISTRATION is False."""
        with override_settings(ACCOUNT_ALLOW_REGISTRATION=False):
            adapter = SocialAccountAdapter()
            assert adapter.is_open_for_signup(None, None) is False

    def test_is_open_for_signup_default(self):
        """Test is_open_for_signup with default value."""
        adapter = SocialAccountAdapter()
        assert adapter.is_open_for_signup(None, None) is True

    @pytest.mark.parametrize(
        ("data", "expected_name"),
        [
            ({"name": "John Doe"}, "John Doe"),
            ({"first_name": "John", "last_name": "Doe"}, "John Doe"),
            ({"first_name": "John"}, "John"),
            ({}, None),
        ],
    )
    def test_populate_user(self, data, expected_name):
        """Test populate_user with different data."""
        from unittest.mock import MagicMock, patch

        adapter = SocialAccountAdapter()
        request = MagicMock()
        sociallogin = MagicMock()
        sociallogin.account.provider = "test_provider"

        user = MagicMock()
        user.name = None

        # Mock super().populate_user
        with patch.object(adapter.__class__.__bases__[0], 'populate_user', return_value=user):
            result = adapter.populate_user(request, sociallogin, data)

            assert result == user
            if expected_name:
                assert result.name == expected_name
            else:
                assert result.name is None
