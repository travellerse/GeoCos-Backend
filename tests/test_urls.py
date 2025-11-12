from __future__ import annotations

import pytest
from django.conf import settings
from django.test import override_settings

from config.urls import urlpatterns


class TestURLPatterns:
    def test_urlpatterns_debug_true(self):
        """Test urlpatterns when DEBUG=True."""
        with override_settings(DEBUG=True, INSTALLED_APPS=["debug_toolbar"]):
            # Re-import to get updated urlpatterns
            from importlib import reload

            import config.urls

            reload(config.urls)
            urls = config.urls.urlpatterns

            # Check that staticfiles_urlpatterns are included
            assert any("static" in str(url.pattern) for url in urls)

            # Check that debug_toolbar urls are included
            assert any("__debug__" in str(url.pattern) for url in urls)

            # Check error pages are included
            error_patterns = [str(url.pattern) for url in urls]
            assert any("400" in pattern for pattern in error_patterns)
            assert any("403" in pattern for pattern in error_patterns)
            assert any("404" in pattern for pattern in error_patterns)
            assert any("500" in pattern for pattern in error_patterns)

    def test_urlpatterns_debug_false(self):
        """Test urlpatterns when DEBUG=False."""
        with override_settings(DEBUG=False, INSTALLED_APPS=[]):
            from importlib import reload

            import config.urls

            reload(config.urls)
            urls = config.urls.urlpatterns

            # Check that staticfiles_urlpatterns are not included
            assert not any("static" in str(url.pattern) for url in urls)

            # Check that debug_toolbar urls are not included
            assert not any("__debug__" in str(url.pattern) for url in urls)

            # Check error pages are not included
            error_patterns = [str(url.pattern) for url in urls]
            assert not any("400" in pattern for pattern in error_patterns)
            assert not any("403" in pattern for pattern in error_patterns)
            assert not any("404" in pattern for pattern in error_patterns)
            assert not any("500" in pattern for pattern in error_patterns)

    def test_urlpatterns_debug_true_no_debug_toolbar(self):
        """Test urlpatterns when DEBUG=True but debug_toolbar not in INSTALLED_APPS."""
        with override_settings(DEBUG=True, INSTALLED_APPS=[]):
            from importlib import reload

            import config.urls

            reload(config.urls)
            urls = config.urls.urlpatterns

            # Check that debug_toolbar urls are not included
            assert not any("__debug__" in str(url.pattern) for url in urls)
