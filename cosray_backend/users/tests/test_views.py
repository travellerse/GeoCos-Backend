from django.conf import settings


def test_headless_apps_enabled():
    assert "allauth.headless" in settings.INSTALLED_APPS


def test_headless_configuration():
    assert settings.HEADLESS_ONLY is True
    assert settings.HEADLESS_CLIENTS == ("app",)
