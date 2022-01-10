# Deployment

You can set up a production-ready system to orchestrate multiple vantage points from a dedicated API.

We provide a [docker-compose.yml](docker-compose.yml) file to set up Iris locally.
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
pip install alembic
alembic upgrade head
```

The API documentation will be available on http://api.docker.localhost/docs.

## Users

By default, a single admin user is created with the email `admin@example.org` and the password `admin`.
To change its password, run: XXX.

### Capabilities

To change a user capabilities: TODO with psql.

At this point a user is created but it's not verified and don't have probing capabilities.
It's not possible to add these capababilities without first having an admin user.  So we need to change the user's role in the database.
We can use docker compose to make this change.
```bash
docker compose exec api sqlite3 iris_data/iris.sqlite3 'UPDATE user SET is_verified = true, probing_enabled = true, probing_limit = none WHERE email = "user@example.com"'
```

Now the user has the ability to do measurements, but you probably may want it to be an admin.
```bash
docker compose exec api sqlite3 iris_data/iris.sqlite3 'UPDATE user SET is_superuser = true WHERE email = "user@example.com"'
```

That's it, a super user is created. This user can perform measurements, patch any other users and even promote them admin.

## Configuration

In this section we will document the different settings to configure Iris.
Most of the settings are commons to the API, the worker and the agent components.
All of the settings must be declared in the [docker-compose.yml](docker-compose.yml) file for each component.

| Component    | Settings location                                |
|--------------|--------------------------------------------------|
| Commons  | [iris/commons/settings.py](iris/commons/settings.py) |
| API      | [iris/api/settings.py](iris/api/settings.py)         |
| Worker   | [iris/worker/settings.py](iris/worker/settings.py)   |
| Agent    | [iris/agent/settings.py](iris/agent/settings.py)     |
