# Contributing

Thanks for contributing ! Here is some guidelines to make your life easier during the development process.



## Run with Docker-Compose

For development purposes, you can setup a new local infrastructure like this.

```
make run-dev
```

## Syntax checking

You can check the syntax using flake8.

```
make lint
```

## Type checking

If you used annotations to do static Python type checking with mypy.

```
make type
```

## Test coverage

You can run the coverage using pytest.

```
make test
```


## Release

It is recommended to use [bumpversion](https://pypi.org/project/bumpversion/0.6.0/) to conduct the releases.
The version bump will automatically create a new commit associated with a tag.
When pushed into Github, the tag will trigger a deployment workflow that will push the new version of Iris Agent into Docker Hub.