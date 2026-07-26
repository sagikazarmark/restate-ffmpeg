# restate-ffmpeg

**FFmpeg service for [Restate](https://restate.dev/).**

`restate-ffmpeg` provides `ffmpeg` and `ffprobe` handlers that can read media from URLs and write FFmpeg output through [Apache OpenDAL](https://opendal.apache.org/).

## Usage

Bind the service to a Restate endpoint:

```rust
use restate_ffmpeg::ServiceImpl;
use restate_sdk::{endpoint::Endpoint, service::IntoServiceDefinition};

let service = ServiceImpl::new(operator_factory).into_service_definition();
let endpoint = Endpoint::builder().bind(service).build();
```

The host environment must provide the `ffmpeg` and `ffprobe` executables.

## License

The project is licensed under the [MIT License](../../LICENSE).
