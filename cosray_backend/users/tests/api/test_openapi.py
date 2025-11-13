from http import HTTPStatus

import pytest
from django.urls import reverse

from cosray_backend.users.tests.factories import UserFactory


@pytest.mark.django_db
def test_api_schema_requires_staff_privileges(client):
    url = reverse("api-schema")
    response = client.get(url)
    assert response.status_code == HTTPStatus.FORBIDDEN


@pytest.mark.django_db
def test_api_schema_generated_successfully(client):
    user = UserFactory(is_staff=True)
    client.force_login(user)

    url = reverse("api-schema")
    response = client.get(url)
    assert response.status_code == HTTPStatus.OK
