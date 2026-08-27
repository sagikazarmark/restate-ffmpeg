# syntax=docker/dockerfile:1
# check=skip=CopyIgnoredFile

FROM --platform=$BUILDPLATFORM tonistiigi/xx:1.9.0@sha256:c64defb9ed5a91eacb37f96ccc3d4cd72521c4bd18d5442905b95e2226b0e707 AS xx

FROM --platform=$BUILDPLATFORM rust:1.97.1-bookworm@sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97 AS base

ARG CARGO_CHEF_VERSION=0.1.77
RUN cargo install cargo-chef --version $CARGO_CHEF_VERSION --locked

COPY --from=xx / /

WORKDIR /usr/src/app


FROM base AS deps

COPY . .

RUN cargo chef prepare --recipe-path recipe.json


FROM base AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends clang lld && \
    rm -rf /var/lib/apt/lists/*

ARG TARGETPLATFORM

RUN xx-apt-get update && \
    xx-apt-get install -y --no-install-recommends gcc g++ libc6-dev pkg-config && \
    rm -rf /var/lib/apt/lists/*

COPY --from=deps /usr/src/app/recipe.json recipe.json

RUN xx-cargo chef cook --locked --release --recipe-path recipe.json

COPY . .

RUN xx-cargo build --locked --release --package restate-ffmpeg-endpoint --bin restate-ffmpeg
RUN xx-verify ./target/$(xx-cargo --print-target-triple)/release/restate-ffmpeg
RUN cp ./target/$(xx-cargo --print-target-triple)/release/restate-ffmpeg /usr/local/bin/restate-ffmpeg


FROM debian:13.6-slim@sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132

RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/restate-ffmpeg /usr/local/bin/

ENV RUST_LOG=info

EXPOSE 9080

USER 65532:65532

CMD ["restate-ffmpeg"]
