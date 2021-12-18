# Deployment

You can set up a production-ready system to orchestrate multiple vantage points from a dedicated API.

We provide a [docker-compose.yml](docker-compose.yml) file to set up Iris locally. Feel free to adapt it with your own configurations.
Don't forget to change the default passwords before pushing it to production!

Then, simply run the stack with docker-compose:
```bash
docker-compose up -d --build
```

You can then access the Swagger UI of the API from the browser: http://api.docker.localhost/docs

> **Note:** You may need to add this to your hosts file:
>```
>127.0.0.1 api.docker.localhost
>127.0.0.1 minio-console.docker.localhost
>127.0.0.1 traefik.docker.localhost
>```

## Create the first (super) user

By default no user is registred to Iris so you need to create one.

You can do it via the Swagger UI or directly with a POST request to the /auth endpoint:

```bash
curl -X POST http://api.docker.localhost/auth/register -H 'Content-Type: application/json' -d '{"email":"user@example.com","password":"admin"}'
```

At this point a user is created but it's not verified and don't have probing capabilities.
It's not possible to add these capababilities without first having an admin user.  So we need to change the user's role in the database.
We can use docker compose to make this change.
```bash
docker compose exec api sqlite3 iris.sqlite3 'UPDATE user SET is_verified = true, probing_enabled = true, probing_limit = none WHERE email = "user@example.com"'
```

Now the user has the ability to do measurements, but you probably may want it to be an admin.
```bash
docker compose exec api sqlite3 iris.sqlite3 'UPDATE user SET is_superuser = true WHERE email = "user@example.com"'
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
