import pytest
from django.conf import settings
from rest_framework.test import APIClient


def test_headless_apps_enabled():
    assert "allauth.headless" in settings.INSTALLED_APPS


def test_headless_configuration():
    assert settings.HEADLESS_ONLY is True
    assert settings.HEADLESS_CLIENTS == ("app",)


# https://allauth.org/docs/draft-api/#tag/Authentication:-Account/paths/~1_allauth~1%7Bclient%7D~1v1~1auth~1login/post
@pytest.mark.django_db
def test_headless_login_url_accessible():
    client = APIClient()
    response = client.post(
        "/_allauth/app/v1/auth/login",
        data={"username": "test", "password": "testpass"},
        format="json",
    )
    # Missing credentials should yield a validation error (400) while authenticated retries return 409.
    assert response.status_code in {400, 401, 409}


@pytest.mark.django_db
def test_headless_login_successful_with_valid_credentials(django_user_model):
    # Create a user with known credentials
    username = "validuser"
    email = "validuser@example.com"
    password = "validpassword123"
    user = django_user_model.objects.create_user(username=username, email=email, password=password)

    # verify email in allauth
    user.emailaddress_set.create(email=email, verified=True, primary=True)

    client = APIClient()
    response = client.post(
        "/_allauth/app/v1/auth/login",
        data={"username": username, "password": password},
        format="json",
    )
    assert response.status_code == 200
    # Check for token/session data in the response
    data = response.json()
    assert "session_token" in data.get("meta", {})


@pytest.mark.django_db
def test_headless_logout_url_accessible():
    client = APIClient()
    response = client.delete("/_allauth/app/v1/auth/session")
    # Without authentication, logout may still return a successful response or prompt for auth.
    assert response.status_code in {200, 204, 401}
