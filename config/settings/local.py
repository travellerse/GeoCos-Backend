from .base import *  # noqa: F403

# GENERAL
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#debug
DEBUG = True
# https://docs.djangoproject.com/en/dev/ref/settings/#secret-key
SECRET_KEY = env(
    "DJANGO_SECRET_KEY",
    default="eluP5ZXzB3txkA2HanPOCO0nk6BGyR48ARvl341FGRGYtdUiBT1XRh4pJyQVK6NR",
)
# https://docs.djangoproject.com/en/dev/ref/settings/#allowed-hosts
ALLOWED_HOSTS = ["localhost", "0.0.0.0", "127.0.0.1", "10.0.2.2"]  # noqa: S104

# CACHES
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#caches
CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        "LOCATION": "",
    },
}

# EMAIL
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#email-backend
EMAIL_BACKEND = env(
    "DJANGO_EMAIL_BACKEND",
    default="django.core.mail.backends.console.EmailBackend",
)

# django-debug-toolbar (removed for API-only deployment)
# ------------------------------------------------------------------------------
USE_DOCKER = env.bool("USE_DOCKER", default=False)

# django-extensions
# ------------------------------------------------------------------------------
# https://django-extensions.readthedocs.io/en/latest/installation_instructions.html#configuration
INSTALLED_APPS += ["django_extensions"]

# API-only local development usually serves a separate frontend (Next.js, Vite, etc.).
# Allow all origins locally to avoid extra environment tweaks and trust common dev hosts for CSRF.
CORS_ALLOW_ALL_ORIGINS = True
CSRF_TRUSTED_ORIGINS += [
    "http://localhost",
    "http://127.0.0.1",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

# Your stuff...
# ------------------------------------------------------------------------------

LOCAL_DEV_TEST_USER = {
    "ENABLED": env.bool("LOCAL_DEV_TEST_USER_ENABLED", default=True),
    "USERNAME": env("LOCAL_DEV_TEST_USER_USERNAME", default="test"),
    "PASSWORD": env("LOCAL_DEV_TEST_USER_PASSWORD", default="LocalPass123!"),
    "EMAIL": env("LOCAL_DEV_TEST_USER_EMAIL", default="localtester@example.com"),
    "NAME": env("LOCAL_DEV_TEST_USER_NAME", default="Local Tester"),
    "IS_STAFF": env.bool("LOCAL_DEV_TEST_USER_IS_STAFF", default=True),
    "IS_SUPERUSER": env.bool("LOCAL_DEV_TEST_USER_IS_SUPERUSER", default=True),
}
