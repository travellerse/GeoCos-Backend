# define an alias for the specific python version used in this file.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS python

# Python build stage
FROM python AS python-build-stage

ARG APP_HOME=/app

WORKDIR ${APP_HOME}

# 环境变量
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    PATH="${APP_HOME}/.venv/bin:$PATH" \
    PYTHONPATH="${APP_HOME}/.venv/lib/python3.13/site-packages"

# 安装构建与运行依赖
RUN apt-get update && \
    apt-get install --no-install-recommends -y \
      bash-completion build-essential gettext git \
      libpq-dev nano ssh sudo wait-for-it && \
    rm -rf /var/lib/apt/lists/*

# 安装 Python 依赖（使用缓存 mount 加速）
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock:rw \
    uv sync --no-install-project

# 复制项目代码
COPY . ${APP_HOME}

# 再次同步以确保项目依赖完整（仍使用缓存）
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

# 创建开发用户
RUN groupadd --gid 1000 dev-user && \
    useradd --uid 1000 --gid dev-user --shell /bin/bash --create-home dev-user && \
    echo "dev-user ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/dev-user && \
    chmod 0440 /etc/sudoers.d/dev-user

COPY ./compose/production/django/entrypoint /entrypoint
RUN sed -i 's/\r$//g' /entrypoint
RUN chmod +x /entrypoint

COPY ./compose/local/django/start /start
RUN sed -i 's/\r$//g' /start
RUN chmod +x /start


COPY ./compose/local/django/celery/worker/start /start-celeryworker
RUN sed -i 's/\r$//g' /start-celeryworker
RUN chmod +x /start-celeryworker

COPY ./compose/local/django/celery/beat/start /start-celerybeat
RUN sed -i 's/\r$//g' /start-celerybeat
RUN chmod +x /start-celerybeat

COPY ./compose/local/django/celery/flower/start /start-flower
RUN sed -i 's/\r$//g' /start-flower
RUN chmod +x /start-flower


ENTRYPOINT ["/entrypoint"]
