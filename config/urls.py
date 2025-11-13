from django.conf import settings
from django.http import HttpRequest, JsonResponse
from django.urls import include, path
from django.views import defaults as default_views
from drf_spectacular.views import SpectacularAPIView
from rest_framework.authtoken.views import obtain_auth_token


def api_root(request: HttpRequest) -> JsonResponse:
    """Lightweight entry point for mobile clients."""

    base_url = request.build_absolute_uri("/")
    schema_url = request.build_absolute_uri("/api/schema/")
    return JsonResponse(
        {
            "service": "CosRay-Backend API",
            "status": "ok",
            "base_url": base_url.rstrip("/"),
            "schema_url": schema_url,
        }
    )


urlpatterns = [
    path("", api_root, name="api-root"),
    path("_allauth/", include("allauth.headless.urls")),
    # Your stuff: custom urls includes go here
    # ...
]

# API URLS
urlpatterns += [
    # API base url
    path("api/", include("config.api_router")),
    # DRF auth token
    path("api/auth-token/", obtain_auth_token, name="obtain_auth_token"),
    path("api/schema/", SpectacularAPIView.as_view(), name="api-schema"),
]

if settings.DEBUG:
    from django.conf.urls.static import static

    # Media files served only in debug mode; production should use a CDN/object storage.
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

    # This allows the error pages to be debugged during development, just visit
    # these url in browser to see how these error pages look like.
    urlpatterns += [
        path(
            "400/",
            default_views.bad_request,
            kwargs={"exception": Exception("Bad Request!")},
        ),
        path(
            "403/",
            default_views.permission_denied,
            kwargs={"exception": Exception("Permission Denied")},
        ),
        path(
            "404/",
            default_views.page_not_found,
            kwargs={"exception": Exception("Page not Found")},
        ),
        path("500/", default_views.server_error),
    ]
    if "debug_toolbar" in settings.INSTALLED_APPS:
        import debug_toolbar

        urlpatterns = [
            path("__debug__/", include(debug_toolbar.urls)),
            *urlpatterns,
        ]
