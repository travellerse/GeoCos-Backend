"""
With these settings, tests run faster.
"""

from .base import *  # noqa: F403
from .base import env

# GENERAL
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#secret-key
SECRET_KEY = env(
    "DJANGO_SECRET_KEY",
    default="QoV1hvpVq5GcNHzj81RDnbwR8weYkyJlPooK3FNPEjGVJKOeiXELlGZsV66NItE5",
)
# https://docs.djangoproject.com/en/dev/ref/settings/#test-runner
TEST_RUNNER = "django.test.runner.DiscoverRunner"

# PASSWORDS
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#password-hashers
PASSWORD_HASHERS = ["django.contrib.auth.hashers.MD5PasswordHasher"]

# EMAIL
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#email-backend
EMAIL_BACKEND = "django.core.mail.backends.locmem.EmailBackend"

# MEDIA
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#media-url
MEDIA_URL = "http://media.testserver/"

# Allow free cross-origin access during tests so API clients can be exercised without
# additional configuration.
CORS_ALLOW_ALL_ORIGINS = True
CSRF_TRUSTED_ORIGINS = []
# Your stuff...
# ------------------------------------------------------------------------------
