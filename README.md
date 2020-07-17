# Iris Controller


## Contribute

### Run with Docker-Compose

For development purposes, you can setup a new local infrastructure like this.

```
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up --build
```

### Release

It is recommended to use [bumpversion](https://pypi.org/project/bumpversion/0.6.0/) to conduct the releases.
The version bump will automatically create a new commit associated with a tag.
When pushed into Github, the tag will trigger a deployment workflow that will push the new version of Iris Agent into Docker Hub.