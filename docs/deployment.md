# Deployment

You can set up a production-ready system to orchestrate multiple vantage points from a dedicated API.

We provide a [`docker-compose.yml`](https://github.com/dioptra-io/iris/blob/main/docker-compose.yml) file to set up Iris locally.
Feel free to adapt it with your own configurations.
Don't forget to change the default passwords before pushing it to production!

First, add the following entry to [`/etc/hosts`](file:///etc/hosts):
```
127.0.0.1 api.docker.localhost
# The lines below are optional:
127.0.0.1 clickhouse.docker.localhost
127.0.0.1 minio.docker.localhost
127.0.0.1 minio-console.docker.localhost
127.0.0.1 postgres.docker.localhost
127.0.0.1 redis.docker.localhost
127.0.0.1 traefik.docker.localhost
```

Then run the stack and seed the database:
```bash
docker-compose up --build --detach
docker-compose exec api .venv/bin/alembic upgrade head
```

The API documentation will be available on http://api.docker.localhost/docs.

## Users

By default, a single admin user is created with the email `admin@example.org` and the password `admin`.
To change its email and/or password, run:
```bash
# Login and retrieve the JWT access token
curl -X POST -F 'username=admin@example.org' -F 'password=admin' http://api.docker.localhost/auth/jwt/login
export TOKEN="copy access_token here"
# Patch the user
curl -X PATCH -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"email": "new@example.org", "password": "newpassword"}' http://api.docker.localhost/users/me
```

## Configuration

In this section we will document the different settings to configure Iris.
Most of the settings are commons to the API, the worker and the agent components.
All of the settings must be declared in the [`docker-compose.yml`](https://github.com/dioptra-io/iris/blob/main/docker-compose.yml) file for each component.

| Component    | Settings location                                |
|--------------|--------------------------------------------------|
| Commons  | [iris/commons/settings.py](https://github.com/dioptra-io/iris/blob/main/iris/commons/settings.py) |
| API      | [iris/api/settings.py](https://github.com/dioptra-io/iris/blob/main/iris/api/settings.py)         |
| Worker   | [iris/worker/settings.py](https://github.com/dioptra-io/iris/blob/main/iris/worker/settings.py)   |
| Agent    | [iris/agent/settings.py](https://github.com/dioptra-io/iris/blob/main/iris/agent/settings.py)     |
