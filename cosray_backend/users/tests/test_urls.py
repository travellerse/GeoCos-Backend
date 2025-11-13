from django.urls import resolve, reverse

from cosray_backend.users.models import User


def test_api_user_detail_url(user: User):
    url = reverse("api:user-detail", kwargs={"username": user.username})

    assert url == f"/api/users/{user.username}/"
    assert resolve(url).view_name == "api:user-detail"
