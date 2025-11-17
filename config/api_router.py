from django.conf import settings
from rest_framework.routers import DefaultRouter, SimpleRouter

from cosray_backend.mu_packets.api.views import PacketViewSet
from cosray_backend.users.api.views import UserViewSet

router = DefaultRouter() if settings.DEBUG else SimpleRouter()

router.register("users", UserViewSet)
router.register("mu-packets", PacketViewSet, basename="mu-packets")


app_name = "api"
urlpatterns = router.urls
