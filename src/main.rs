use std::{io, task, vec};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use anyhow::{anyhow, Result};
use cid::Cid;
use clap::Parser;
use fr32::Fr32Reader;
use futures_util::{FutureExt, TryStreamExt};
use futures_util::future::Map;
use hyper::{Body, Client, http, Request, Response, Server, Uri};
use hyper::body::Buf;
use hyper::client::connect::dns::{GaiAddrs, GaiFuture, GaiResolver, Name};
use hyper::client::HttpConnector;
use hyper::service::{make_service_fn, Service, service_fn};
use log::{debug, info, LevelFilter};
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Root};
use log4rs::encode::pattern::PatternEncoder;
use mimalloc::MiMalloc;
use multihash::Multihash;
use rand::Rng;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::time::Instant;
use tokio_util::io::StreamReader;
use url::Url;

use crate::bytes_amount::{PaddedBytesAmount, UnpaddedBytesAmount};
use crate::commitment_reader::CommitmentReader;

mod bytes_amount;
mod commitment_reader;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Clone)]
struct RoundRobin {
    inner: GaiResolver,
}

impl Service<Name> for RoundRobin {
    type Response = vec::IntoIter<SocketAddr>;
    type Error = io::Error;
    type Future = Map<GaiFuture, fn(Result<GaiAddrs, io::Error>) -> Result<Self::Response, io::Error>>;

    fn poll_ready(&mut self, cx: &mut task::Context<'_>) -> Poll<Result<(), io::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, name: Name) -> Self::Future {
        self.inner.call(name).map(|v| {
            match v {
                Ok(v) => {
                    let mut list: Vec<SocketAddr> = v.collect();

                    if list.len() > 1 {
                        let i = rand::thread_rng().gen_range(0..list.len());
                        list = vec![list[i]];
                    }

                    if !list.is_empty() {
                        debug!("ip select {}", list[0]);
                    }
                    Ok(list.into_iter())
                }
                Err(e) => Err(e)
            }
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3Config {
    pub host: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
}

pub fn get_object_url(key: &str, bucket: &Bucket, config: &S3Config) -> Result<Url> {
    let cred = Credentials::new(&config.access_key, &config.secret_key);
    let action = bucket.get_object(Some(&cred), key);
    let presigned_url_duration = Duration::from_secs(60 * 60);
    let url = action.sign(presigned_url_duration);
    Ok(url)
}

#[derive(Deserialize, Clone)]
struct Req {
    key: String,
    padded_piece_size: u64,
}

struct Context {
    s3: S3Config,
    bucket: Bucket,
    client: Client<HttpConnector<RoundRobin>>,
}

async fn handle(ctx: Arc<Context>, req: Request<Body>) -> Result<Response<Body>, http::Error> {
    let fut = async {
        let body = hyper::body::aggregate(req.into_body()).await?;
        let req: Req = serde_json::from_reader(body.reader())?;

        let t = Instant::now();

        let object_url = get_object_url(&req.key, &ctx.bucket, &ctx.s3)?;
        let resp = ctx.client.get(Uri::from_str(object_url.as_str())?).await?;

        if resp.status() != 200 {
            return Ok(resp);
        }
        let upsize = UnpaddedBytesAmount::from(PaddedBytesAmount(req.padded_piece_size));

        let mut reader = StreamReader::new(resp.into_body().map_err(|e| io::Error::new(io::ErrorKind::Other, e)));
        // 1GB
        let (mut tx, rx) = tokio::io::duplex(1073741824);

        let join = tokio::spawn(async move {
            tokio::io::copy(&mut reader, &mut tx).await
        });

        let reader = rx.chain(tokio::io::repeat(0)).take(upsize.0);
        let fr32_reader = Fr32Reader::async_new(reader);
        let mut commitment_reader = CommitmentReader::new(fr32_reader);

        commitment_reader.consume().await?;
        let cid_buff = commitment_reader.finish()?;
        join.await??;

        let hash = Multihash::wrap(0x1012, cid_buff.as_ref())?;
        let c = Cid::new_v1(0xf101, hash);

        info!("compute {} commp use {} secs", req.key, t.elapsed().as_secs());
        Result::<_, anyhow::Error>::Ok(Response::new(Body::from(c.to_string())))
    };

    match fut.await {
        Ok(resp) => Ok(resp),
        Err(e) => {
            Response::builder()
                .status(500)
                .body(Body::from(e.to_string()))
        }
    }
}

async fn exec(bind_addr: SocketAddr, s3_config: S3Config) -> Result<()> {
    let bucket = Bucket::new(
        Url::parse(&s3_config.host)?,
        UrlStyle::VirtualHost,
        s3_config.bucket.clone(),
        s3_config.region.clone(),
    )?;

    let connector = HttpConnector::new_with_resolver(RoundRobin { inner: GaiResolver::new() });

    let client = Client::builder()
        .build(connector);

    let ctx = Context {
        bucket,
        s3: s3_config,
        client,
    };
    let ctx = Arc::new(ctx);

    let make_service = make_service_fn(move |_| {
        let ctx = ctx.clone();

        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                handle(ctx.clone(), req)
            }))
        }
    });

    let server = Server::bind(&bind_addr).serve(make_service);
    server.await.map_err(|e| anyhow!(e))
}

fn logger_init() -> Result<()> {
    let pattern = if cfg!(debug_assertions) {
        "[{d(%Y-%m-%d %H:%M:%S)}] {h({l})} {f}:{L} - {m}{n}"
    } else {
        "[{d(%Y-%m-%d %H:%M:%S)}] {h({l})} {t} - {m}{n}"
    };

    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(pattern)))
        .build();

    let config = log4rs::Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(
            Root::builder()
                .appender("stdout")
                .build(LevelFilter::from_str(
                    &std::env::var("COMMP_SERVER_LOG").unwrap_or_else(|_| String::from("INFO")),
                )?),
        )?;

    log4rs::init_config(config)?;
    Ok(())
}

#[derive(Parser)]
#[command(version)]
struct Args {
    /// s3 config file path
    #[arg(short, long)]
    s3_config_path: String,

    #[arg(short, long)]
    bind_addr: SocketAddr,
}

fn main() -> Result<()> {
    let args: Args = Args::parse();

    let config = std::fs::read(args.s3_config_path)?;
    let s3_config: S3Config = toml::from_slice(&config)?;

    logger_init()?;

    let rt = tokio::runtime::Runtime::new()?;
    info!("Listening on http://{}", args.bind_addr);
    rt.block_on(exec(args.bind_addr, s3_config))
}
