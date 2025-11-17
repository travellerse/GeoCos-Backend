import pytest
from allauth.account.models import EmailAddress

from cosray_backend.users.signals import create_local_test_user


@pytest.mark.django_db
def test_create_local_test_user(settings):
    settings.LOCAL_DEV_TEST_USER = {
        "ENABLED": True,
        "USERNAME": "devtester",
        "PASSWORD": "DevPass!2025",
        "EMAIL": "devtester@example.com",
        "NAME": "Dev Tester",
        "IS_STAFF": True,
        "IS_SUPERUSER": False,
    }

    user = create_local_test_user()
    assert user is not None
    assert user.username == "devtester"
    assert user.email == "devtester@example.com"
    assert user.name == "Dev Tester"
    assert user.is_staff
    assert not user.is_superuser
    assert user.check_password("DevPass!2025")
    assert EmailAddress.objects.filter(user=user, email=user.email, verified=True, primary=True).exists()


def test_create_local_test_user_disabled(settings):
    settings.LOCAL_DEV_TEST_USER = {"ENABLED": False}
    assert create_local_test_user() is None
