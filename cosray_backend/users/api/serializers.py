from rest_framework import serializers

from cosray_backend.users.models import User


class UserSerializer(serializers.ModelSerializer[User]):
    """序列化当前用户，返回 headless UI 所需的最小字段。

    - `username`：用户唯一标识。
    - `name`：展示名称。
    - `url`：指向 `api:user-detail` 的链接，用于跳转详情。

    `users.api.views.UserViewSet` 中的 `/me/` 和其它列表/详情接口会复用该
    序列化器，让已经通过 allauth headless 认证的客户端能直接展示当前
    登录用户的信息。
    """

    class Meta:
        model = User
        fields = ["username", "name", "url"]

        extra_kwargs = {
            "url": {"view_name": "api:user-detail", "lookup_field": "username"},
        }
