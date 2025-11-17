from __future__ import annotations

from typing import Any, Dict, Optional

from allauth.account.models import EmailAddress
from django.conf import settings
from django.contrib.auth import get_user_model
from django.db.models.signals import post_migrate
from django.dispatch import receiver

LOCAL_TEST_USER_ATTR = "LOCAL_DEV_TEST_USER"


def _get_local_test_user_config() -> Optional[Dict[str, Any]]:
    config = getattr(settings, LOCAL_TEST_USER_ATTR, None)
    if not isinstance(config, dict):
        return None
    return config


def create_local_test_user():
    config = _get_local_test_user_config()
    if not config or not config.get("ENABLED"):
        return None

    username = config.get("USERNAME")
    password = config.get("PASSWORD")
    if not username or not password:
        return None

    defaults = {
        "email": config.get("EMAIL", ""),
        "name": config.get("NAME", ""),
        "is_staff": config.get("IS_STAFF", False),
        "is_superuser": config.get("IS_SUPERUSER", False),
        "is_active": True,
    }

    user_model = get_user_model()
    user, created = user_model.objects.get_or_create(username=username, defaults=defaults)
    if created:
        user.set_password(password)
        user.save(update_fields=["password"])
    _ensure_user_email_verified(user, defaults["email"])
    return user


def _ensure_user_email_verified(user, email):
    if not email:
        return
    EmailAddress.objects.update_or_create(
        user=user,
        email=email,
        defaults={"primary": True, "verified": True},
    )


@receiver(post_migrate)
def _ensure_local_test_user(sender: Any, **kwargs: Any) -> None:  # pragma: no cover - signal
    if getattr(sender, "name", None) != "cosray_backend.users":
        return
    if not _get_local_test_user_config():
        return
    create_local_test_user()
