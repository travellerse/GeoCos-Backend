from rest_framework import status
from rest_framework.decorators import action
from rest_framework.mixins import ListModelMixin, RetrieveModelMixin, UpdateModelMixin
from rest_framework.response import Response
from rest_framework.viewsets import GenericViewSet

from cosray_backend.users.models import User

from .serializers import UserSerializer


class UserViewSet(RetrieveModelMixin, ListModelMixin, UpdateModelMixin, GenericViewSet):
    """Headless 用户 API，主要依赖 django-allauth 提供认证。

    所有认证相关的 HTTP 接口都由 allauth headless 模块负责，使用方式示例：

    - 登录：`POST /_allauth/app/v1/auth/login`，负载 `{"client": "web", "email": "...", "password": "..."}`。
    - 注销：`DELETE /_allauth/app/v1/auth/session`，无需额外负载。
    - 注册：`POST /_allauth/app/v1/auth/signup`，负载示例 `{"client": "web", "email": "...", "password": "..."}`。
    - 获取当前会话：`GET /_allauth/app/v1/auth/session`。

    只要这些接口返回了有效的 session 或 token，下面的 `UserViewSet` 就可以
    让客户端查看 / 更新当前用户的数据，以及通过 `/me/` 快速获取当前用户。
    """

    serializer_class = UserSerializer
    queryset = User.objects.all()
    lookup_field = "username"

    def get_queryset(self, *args, **kwargs):
        # Guardrail: even though the viewset supports list/retrieve, users can
        # only see their own instance once authenticated via allauth headless
        # (the authentication session/state is handled before this viewset
        # executes).
        assert isinstance(self.request.user.id, int)
        return self.queryset.filter(id=self.request.user.id)

    @action(detail=False)
    def me(self, request):
        # Convenience endpoint for fetching the current user without
        # supplying a username or additional lookup parameter.
        serializer = UserSerializer(request.user, context={"request": request})
        return Response(status=status.HTTP_200_OK, data=serializer.data)
