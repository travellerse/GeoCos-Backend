from django.core.exceptions import ImproperlyConfigured


def _deprecated_view(*args, **kwargs):  # type: ignore[unused-argument]
    raise ImproperlyConfigured("HTML-based user views have been removed. Use the REST API endpoints instead.")


user_detail_view = _deprecated_view
user_update_view = _deprecated_view
user_redirect_view = _deprecated_view
