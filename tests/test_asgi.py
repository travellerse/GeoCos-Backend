from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from config.asgi import application


class TestASGIApplication:
    @pytest.mark.anyio
    async def test_application_http(self):
        """Test that HTTP requests are handled by django_application."""
        mock_django_app = AsyncMock()
        mock_websocket_app = AsyncMock()

        # Mock the imported applications
        import config.asgi

        config.asgi.django_application = mock_django_app
        config.asgi.websocket_application = mock_websocket_app

        scope = {"type": "http"}
        receive = AsyncMock()
        send = AsyncMock()

        await application(scope, receive, send)

        mock_django_app.assert_called_once_with(scope, receive, send)
        mock_websocket_app.assert_not_called()

    @pytest.mark.anyio
    async def test_application_websocket(self):
        """Test that WebSocket requests are handled by websocket_application."""
        mock_django_app = AsyncMock()
        mock_websocket_app = AsyncMock()

        # Mock the imported applications
        import config.asgi

        config.asgi.django_application = mock_django_app
        config.asgi.websocket_application = mock_websocket_app

        scope = {"type": "websocket"}
        receive = AsyncMock()
        send = AsyncMock()

        await application(scope, receive, send)

        mock_websocket_app.assert_called_once_with(scope, receive, send)
        mock_django_app.assert_not_called()

    @pytest.mark.anyio
    async def test_application_unknown_type(self):
        """Test that unknown scope types raise NotImplementedError."""
        mock_django_app = AsyncMock()
        mock_websocket_app = AsyncMock()

        # Mock the imported applications
        import config.asgi

        config.asgi.django_application = mock_django_app
        config.asgi.websocket_application = mock_websocket_app

        scope = {"type": "unknown"}
        receive = AsyncMock()
        send = AsyncMock()

        with pytest.raises(NotImplementedError, match="Unknown scope type unknown"):
            await application(scope, receive, send)

        mock_django_app.assert_not_called()
        mock_websocket_app.assert_not_called()
