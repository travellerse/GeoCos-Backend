# ============================================================
# 1️⃣ Build stage — 安装依赖并构建虚拟环境
# ============================================================
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

ARG APP_HOME=/app
WORKDIR ${APP_HOME}

# 安装必要的构建依赖
RUN apt-get update && \
    apt-get install --no-install-recommends -y \
      build-essential \
      libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# 安装 Python 依赖（利用缓存 mount 提速）
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock,readonly \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml,readonly \
    uv sync --locked --no-install-project --no-dev

# 复制项目代码并再次同步以安装应用本身
COPY . ${APP_HOME}
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# ============================================================
# 2️⃣ Runtime stage — 运行环境（体积更小）
# ============================================================
FROM python:3.13-slim-bookworm AS runtime

ARG APP_HOME=/app
WORKDIR ${APP_HOME}

# 创建安全用户
RUN addgroup --system django && \
    adduser --system --ingroup django django

# 安装运行所需的最小依赖
RUN apt-get update && \
    apt-get install --no-install-recommends -y \
      gettext \
      libpq-dev \
      wait-for-it && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY --chown=django:django ./compose/production/django/entrypoint /entrypoint
RUN sed -i 's/\r$//g' /entrypoint
RUN chmod +x /entrypoint


COPY --chown=django:django ./compose/production/django/start /start
RUN sed -i 's/\r$//g' /start
RUN chmod +x /start
COPY --chown=django:django ./compose/production/django/celery/worker/start /start-celeryworker
RUN sed -i 's/\r$//g' /start-celeryworker
RUN chmod +x /start-celeryworker


COPY --chown=django:django ./compose/production/django/celery/beat/start /start-celerybeat
RUN sed -i 's/\r$//g' /start-celerybeat
RUN chmod +x /start-celerybeat


COPY --chown=django:django ./compose/production/django/celery/flower/start /start-flower
RUN sed -i 's/\r$//g' /start-flower
RUN chmod +x /start-flower

# 从构建阶段拷贝应用及虚拟环境
COPY --from=builder --chown=django:django ${APP_HOME} ${APP_HOME}

# 创建媒体目录并确保权限正确
RUN mkdir -p ${APP_HOME}/geocos_backend/media && \
    chown -R django:django ${APP_HOME}

# 设置 PATH 与 Python 环境
ENV PATH="${APP_HOME}/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

USER django

# 编译翻译文件（提前执行，避免容器启动时重复）
RUN DATABASE_URL="" \
    DJANGO_SETTINGS_MODULE="config.settings.test" \
    python manage.py compilemessages || true

ENTRYPOINT ["/entrypoint"]
