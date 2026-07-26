# restate-ffmpeg-endpoint

**Standalone endpoint hosting the FFmpeg service for [Restate](https://restate.dev/).**

## Quick Start

Run the endpoint and register it with Restate:

```bash
restate-ffmpeg --port 9080
restate deployments register http://localhost:9080
```

Pre-built container images are available from GitHub Container Registry:

```bash
docker run -p 9080:9080 ghcr.io/sagikazarmark/restate-ffmpeg:v0.4.0
```

## Configuration

Use `--config <FILE>` to load JSON, YAML, or TOML and `--port <PORT>` to change the listen port. The equivalent environment variables are `CONFIG_FILE` and `PORT`.

OpenDAL profiles can be configured with `OPENDAL_PROFILE_<NAME>_<OPTION>` variables.

## License

The project is licensed under the [MIT License](../../LICENSE).
