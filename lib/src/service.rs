use std::{collections::HashMap, process::Stdio};

use anyhow::Result;
use futures::io::AsyncWriteExt as _;
use opendal::Operator;
use opendal::services::Fs;
use opendal_util::{Copier, OperatorFactory};
use restate_sdk::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use url::Url;

#[restate_sdk::service]
#[name = "FFmpeg"]
pub trait Service {
    /// Run ffmpeg command.
    async fn ffmpeg(request: Json<FfmpegRequest>) -> HandlerResult<Json<FfmpegResponse>>;

    /// Run ffprobe command.
    async fn ffprobe(request: Json<FfprobeRequest>) -> HandlerResult<Json<FfprobeResponse>>;
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_ffmpeg_request())]
pub struct FfmpegRequest {
    args: Vec<String>,
    output: Output,
}

fn example_ffmpeg_request() -> FfmpegRequest {
    FfmpegRequest {
        args: vec!["-i", "input.mp4", "-vf", "scale=-1:720", "output.mp4"]
            .into_iter()
            .map(String::from)
            .collect(),
        output: Output {
            location: Url::parse("s3://bucket/").unwrap(),
        },
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_ffmpeg_response())]
pub struct FfmpegResponse {
    stderr: String,
}

fn example_ffmpeg_response() -> FfmpegResponse {
    FfmpegResponse {
        stderr: String::new(),
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Output {
    location: Url,
}

pub struct ServiceImpl<F>
where
    F: OperatorFactory,
{
    factory: F,
}

impl<F> ServiceImpl<F>
where
    F: OperatorFactory,
{
    pub fn new(factory: F) -> Self {
        Self { factory }
    }
}

impl<F> ServiceImpl<F>
where
    F: OperatorFactory,
{
    async fn _ffmpeg(&self, request: FfmpegRequest) -> HandlerResult<FfmpegResponse> {
        // Check if output is stdout (indicated by "-" as last arg or output file)
        let output_to_stdout = request.args.last().map_or(false, |s| s == "-");

        let work_dir = TempDir::new()?;

        let mut cmd = Command::new("ffmpeg")
            .current_dir(work_dir.path())
            .arg("-nostdin")
            .arg("-y")
            .args(&request.args)
            .stderr(Stdio::piped())
            .stdout(if output_to_stdout {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .spawn()?;

        let mut stderr = cmd.stderr.take().expect("Failed to get stderr");

        let (uri, path) = parse_uri(request.output.location);

        let operator = self.factory.load(uri.as_str())?;

        if output_to_stdout {
            let mut writer = operator
                .writer(&path)
                .await?
                .into_futures_async_write()
                .compat_write();

            let mut stdout = cmd.stdout.take().expect("Failed to get stdout");

            let (status, stderr_string, _) = tokio::try_join!(
                cmd.wait(),
                async {
                    let mut s = String::new();
                    stderr.read_to_string(&mut s).await?;
                    Ok::<_, std::io::Error>(s)
                },
                async {
                    tokio::io::copy(&mut stdout, &mut writer).await?;
                    writer.flush().await?;
                    writer.into_inner().close().await?;
                    Ok::<_, std::io::Error>(())
                }
            )?;

            if !status.success() {
                return Err(HandlerError::from(format!(
                    "ffmpeg failed: {}",
                    stderr_string
                )));
            }

            Ok(FfmpegResponse {
                stderr: stderr_string,
            })
        } else {
            // Output to file - extract filename from args
            // let output_file = request
            //     .args
            //     .last()
            //     .filter(|arg| !arg.starts_with('-'))
            //     .ok_or("No output file found in args")?;

            let (status, stderr_string) = tokio::try_join!(cmd.wait(), async {
                let mut s = String::new();
                stderr.read_to_string(&mut s).await?;
                Ok::<_, std::io::Error>(s)
            })?;

            if !status.success() {
                return Err(HandlerError::from(format!(
                    "ffmpeg failed: {}",
                    stderr_string
                )));
            }

            let source = Operator::new(
                Fs::default().root(work_dir.path().to_string_lossy().to_string().as_str()),
            )?
            .finish();

            let copier = Copier::new(source, operator);

            copier.copy("*", path).await?;

            // Stream the file to OpenDAL
            // let mut file = tokio::fs::File::open(&output_file).await?;

            // tokio::io::copy(&mut file, &mut writer).await?;
            // writer.flush().await?;
            // writer.into_inner().close().await?;

            // Clean up local file
            // tokio::fs::remove_file(&output_file).await?;

            Ok(FfmpegResponse {
                stderr: stderr_string,
            })
        }
    }
}
// async fn _ffmpeg(&self, request: FfmpegRequest) -> HandlerResult<FfmpegResponse> {
//     let mut cmd = Command::new("ffmpeg")
//         .args(request.args)
//         .stdout(Stdio::piped())
//         .stderr(Stdio::piped())
//         .spawn()?;

//     let mut stdout = cmd.stdout.take().expect("Failed to get stdout");
//     let mut stderr = cmd.stderr.take().expect("Failed to get stderr");

//     use tokio::io::AsyncReadExt;

//     // Wait for everything concurrently
//     let (status, stderr_string, stdout_bytes) = tokio::try_join!(
//         cmd.wait(),
//         async {
//             let mut s = String::new();
//             stderr.read_to_string(&mut s).await?;
//             Ok::<_, std::io::Error>(s)
//         },
//         async {
//             let mut b = Vec::new();
//             stdout.read_to_end(&mut b).await?;
//             Ok::<_, std::io::Error>(b)
//         }
//     )?;

//     if !status.success() {
//         return Err(HandlerError::from(format!(
//             "ffmpeg failed: {}",
//             stderr_string
//         )));
//     }

//     Ok(FfmpegResponse {})
// }

// async fn _ffmpeg(&self, request: FfmpegRequest) -> HandlerResult<FfmpegResponse> {
//     let mut cmd = Command::new("ffmpeg")
//         .args(request.args)
//         .stdout(Stdio::piped())
//         .stderr(Stdio::piped())
//         .spawn()?;

//     let stdout = cmd.stdout.take().expect("Failed to get stdout");
//     let stderr = cmd.stderr.take().expect("Failed to get stderr");

//     use tokio::io::AsyncReadExt;

//     // Read both streams concurrently
//     let stderr_handle = tokio::spawn(async move {
//         let mut stderr_string = String::new();
//         let mut stderr_reader = tokio::io::BufReader::new(stderr);
//         stderr_reader.read_to_string(&mut stderr_string).await?;
//         Ok::<String, std::io::Error>(stderr_string)
//     });

//     let stdout_handle = tokio::spawn(async move {
//         let mut stdout_bytes = Vec::new();
//         let mut stdout_reader = tokio::io::BufReader::new(stdout);
//         stdout_reader.read_to_end(&mut stdout_bytes).await?;
//         Ok::<Vec<u8>, std::io::Error>(stdout_bytes)
//     });

//     // Wait for both streams and the process
//     let stderr_string = stderr_handle.await??;
//     let stdout_bytes = stdout_handle.await??;
//     let status = cmd.wait().await?;

//     if !status.success() {
//         return Err(HandlerError::from(format!(
//             "ffmpeg failed: {}",
//             stderr_string
//         )));
//     }

//     Ok(FfmpegResponse {})
// }

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_ffprobe_request())]
pub struct FfprobeRequest {
    /// Path or URL to the media file
    pub input: Url,

    /// Include format information
    #[serde(default)]
    pub show_format: bool,

    /// Include stream information
    #[serde(default)]
    pub show_streams: bool,
}

fn example_ffprobe_request() -> FfprobeRequest {
    FfprobeRequest {
        input: Url::parse(
            "https://download.blender.org/peach/bigbuckbunny_movies/big_buck_bunny_1080p_h264.mov",
        )
        .unwrap(),
        show_format: true,
        show_streams: true,
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_ffprobe_response())]
pub struct FfprobeResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<Format>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub streams: Option<Vec<Stream>>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
// #[serde(rename_all = "camelCase")]
pub struct Format {
    pub filename: String,
    pub nb_streams: i32,
    pub nb_programs: i32,
    pub format_name: String,
    pub format_long_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub bit_rate: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub probe_score: Option<i32>,

    #[serde(default)]
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
// #[serde(rename_all = "camelCase")]
pub struct Stream {
    pub index: i32,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub codec_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codec_long_name: Option<String>,

    pub codec_type: String, // "video", "audio", "subtitle", "data"

    #[serde(skip_serializing_if = "Option::is_none")]
    pub codec_tag_string: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub codec_tag: Option<String>,

    // Video-specific fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub coded_width: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub coded_height: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub r_frame_rate: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_frame_rate: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub pix_fmt: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub color_range: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub color_space: Option<String>,

    // Audio-specific fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_fmt: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub channels: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_layout: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub bits_per_sample: Option<i32>,

    // Common timing fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_base: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_pts: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ts: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub bit_rate: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub nb_frames: Option<String>,

    // Disposition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disposition: Option<Disposition>,

    #[serde(default)]
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
// #[serde(rename_all = "camelCase")]
pub struct Disposition {
    #[serde(default)]
    pub default: i32,

    #[serde(default)]
    pub dub: i32,

    #[serde(default)]
    pub original: i32,

    #[serde(default)]
    pub comment: i32,

    #[serde(default)]
    pub lyrics: i32,

    #[serde(default)]
    pub karaoke: i32,

    #[serde(default)]
    pub forced: i32,

    #[serde(default)]
    pub hearing_impaired: i32,

    #[serde(default)]
    pub visual_impaired: i32,

    #[serde(default)]
    pub clean_effects: i32,

    #[serde(default)]
    pub attached_pic: i32,
}

fn example_ffprobe_response() -> FfprobeResponse {
    FfprobeResponse {
        format: None,
        streams: None,
    }
}

impl<F> ServiceImpl<F>
where
    F: OperatorFactory,
{
    async fn _ffprobe(&self, request: FfprobeRequest) -> HandlerResult<FfprobeResponse> {
        let mut cmd = Command::new("ffprobe");

        // Force JSON output, suppress banner
        cmd.args(["-v", "quiet"]);
        cmd.args(["-print_format", "json"]);

        // Add requested sections
        if request.show_format {
            cmd.arg("-show_format");
        }
        if request.show_streams {
            cmd.arg("-show_streams");
        }

        // Input file
        cmd.arg(request.input.as_str());

        // Execute
        let output = cmd.output().await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);

            return Err(HandlerError::from(format!("ffprobe failed: {}", stderr)));
        }

        Ok(serde_json::from_slice(&output.stdout)?)
    }
}

fn parse_uri(uri: Url) -> (String, String) {
    let mut uri = uri;
    let path = uri.path().to_string();
    uri.set_path("");

    (uri.to_string(), path)
}

impl<F> Service for ServiceImpl<F>
where
    F: OperatorFactory,
{
    async fn ffmpeg(
        &self,
        ctx: Context<'_>,
        request: Json<FfmpegRequest>,
    ) -> HandlerResult<Json<FfmpegResponse>> {
        Ok(ctx
            .run(async || Ok(self._ffmpeg(request.into_inner()).await.map(Json)?))
            .await?)
    }

    async fn ffprobe(
        &self,
        ctx: Context<'_>,
        request: Json<FfprobeRequest>,
    ) -> HandlerResult<Json<FfprobeResponse>> {
        Ok(ctx
            .run(async || Ok(self._ffprobe(request.into_inner()).await.map(Json)?))
            .await?)
    }
}
