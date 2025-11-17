# CosRay-Backend

CosRay 服务端

[![Built with Cookiecutter Django](https://img.shields.io/badge/built%20with-Cookiecutter%20Django-ff69b4.svg?logo=cookiecutter)](https://github.com/cookiecutter/cookiecutter-django/)
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License](https://img.shields.io/badge/license-AGPLv3-blue.svg)](LICENSE)

## 快速开始

```bash
# 安装依赖
uv sync

# 迁移数据库并启动开发服务
uv run python manage.py migrate
uv run python manage.py runserver
```

默认开发环境允许任意来源的跨域访问，便于与本地前端（如 `localhost:3000`）联调。

### 创建超级用户

```bash
uv run python manage.py createsuperuser
```

### 本地测试账号

运行本地配置时，`post_migrate` 会自动填充一个便于开发的测试账号：

- 用户名：`test`
- 密码：`LocalPass123!`
- 邮箱：`localtester@example.com`

该账号会在创建后自动标记邮箱为 `verified`，以便使用所有认证流程。

也可以通过环境变量 `LOCAL_DEV_TEST_USER_USERNAME`、`LOCAL_DEV_TEST_USER_PASSWORD`、`LOCAL_DEV_TEST_USER_EMAIL`、`LOCAL_DEV_TEST_USER_NAME`、`LOCAL_DEV_TEST_USER_IS_STAFF`、`LOCAL_DEV_TEST_USER_IS_SUPERUSER` 以及 `LOCAL_DEV_TEST_USER_ENABLED` 覆盖这个默认行为。

### 运行测试

```bash
uv run pytest
```

### 类型检查

```bash
uv run mypy cosray_backend
```

### 生成覆盖率报告

```bash
uv run coverage run -m pytest
uv run coverage html
uv run python -m webbrowser -t htmlcov/index.html
```
