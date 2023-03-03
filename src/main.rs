use std::{io, task, vec};
use std::cmp::min;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use anyhow::{anyhow, ensure, Result};
use cid::Cid;
use clap::Parser;
use futures_util::{FutureExt, StreamExt};
use futures_util::future::Map;
use hyper::{Body, Client, http, Request, Response, Server, Uri};
use hyper::body::Buf;
use hyper::client::connect::dns::{GaiAddrs, GaiFuture, GaiResolver, Name};
use hyper::client::HttpConnector;
use hyper::service::{make_service_fn, Service, service_fn};
use log::{debug, error, info, LevelFilter, warn};
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Root};
use log4rs::encode::pattern::PatternEncoder;
use mimalloc::MiMalloc;
use multihash::Multihash;
use parking_lot::Mutex;
use rand::Rng;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use serde::Deserialize;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use url::Url;

use crate::bytes_amount::{PaddedBytesAmount, UnpaddedBytesAmount};
use crate::commitment::Commitment;

mod bytes_amount;
mod commitment;
mod fr32_util;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
static BUFFER_QUEUE: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

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
    buff_size: usize,
    sem: Semaphore,
}

async fn handle(ctx: Arc<Context>, req: Request<Body>) -> Result<Response<Body>, http::Error> {
    let fut = async {
        let body = hyper::body::aggregate(req.into_body()).await?;
        let req: Req = serde_json::from_reader(body.reader())?;

        let upsize = UnpaddedBytesAmount::from(PaddedBytesAmount(req.padded_piece_size));
        ensure!(upsize.0 % 127 == 0);

        let _permit = ctx.sem.acquire().await?;
        let t = Instant::now();

        let object_url = get_object_url(&req.key, &ctx.bucket, &ctx.s3)?;
        let resp = ctx.client.get(Uri::from_str(object_url.as_str())?).await?;

        if resp.status() != 200 {
            return Ok(resp);
        }

        let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();

        let join = tokio::task::spawn_blocking(move || {
            let mut commitment = Commitment::new();
            let mut chunk = [0u8; 128];
            let mut read_data = 0;
            let mut remain = 0;

            while let Ok(mut buff) = rx.recv() {
                read_data += buff.len();
                let mut right = buff.as_slice();

                if remain != 0 {
                    let (l, r) = right.split_at(min(127 - remain, right.len()));
                    right = r;
                    chunk[remain..remain + l.len()].copy_from_slice(l);

                    if l.len() + remain == 127 {
                        remain = 0;
                        commitment.consume(&chunk);
                    } else {
                        remain += l.len();
                    }
                }

                while right.len() >= 127 {
                    let (l, r) = right.split_at(127);
                    chunk[0..127].copy_from_slice(l);
                    right = r;

                    commitment.consume(&chunk);
                }

                if right.len() != 0 {
                    chunk[..right.len()].copy_from_slice(right);
                    remain = right.len()
                }

                buff.clear();
                BUFFER_QUEUE.lock().push(buff);
            }

            if remain != 0 {
                for x in chunk[remain..].iter_mut() {
                    *x = 0;
                }
                read_data += 127 - remain;
                commitment.consume(&chunk);
            }

            let zero_size = upsize.0.saturating_sub(read_data as u64);
            ensure!(zero_size % 127 == 0);

            let common_hash = commitment::hash(&[0u8; 64]);

            for _ in 0..zero_size / 127 * 2 {
                commitment.consume_with_hash(common_hash);
            }
            commitment.finish()
        });

        let mut body = resp.into_body();

        let get_buf = || {
            BUFFER_QUEUE.lock().pop().unwrap_or_else(|| Vec::<u8>::with_capacity(ctx.buff_size))
        };

        let mut buff = get_buf();

        while let Some(res) = body.next().await {
            let packet = res?;

            if buff.len() + packet.len() > buff.capacity() {
                ensure!(ctx.buff_size >= packet.len());

                let buff = std::mem::replace(&mut buff, get_buf());
                tx.send(buff).map_err(|_| anyhow!("compute task has been shutdown"))?;
            }
            buff.extend(packet);
        }

        if !buff.is_empty() {
            tx.send(buff).map_err(|_| anyhow!("compute task has been shutdown"))?;
        }

        drop(tx);
        let cid_buff = join.await??;

        let hash = Multihash::wrap(0x1012, &cid_buff)?;
        let c = Cid::new_v1(0xf101, hash);

        info!("compute {} commp use {} secs", req.key, t.elapsed().as_secs());
        Result::<_, anyhow::Error>::Ok(Response::new(Body::from(c.to_string())))
    };

    match fut.await {
        Ok(resp) => Ok(resp),
        Err(e) => {
            error!("{}", e);

            Response::builder()
                .status(500)
                .body(Body::from(e.to_string()))
        }
    }
}

async fn exec(
    bind_addr: SocketAddr,
    s3_config: S3Config,
    buff_size: usize,
    parallel_tasks: usize,
) -> Result<()> {
    let bucket = Bucket::new(
        Url::parse(&s3_config.host)?,
        UrlStyle::VirtualHost,
        s3_config.bucket.clone(),
        s3_config.region.clone(),
    )?;

    let mut connector = HttpConnector::new_with_resolver(RoundRobin { inner: GaiResolver::new() });
    // 1GB
    connector.set_recv_buffer_size(Some(1073741824));

    let client = Client::builder()
        .http1_max_buf_size(buff_size)
        .build(connector);

    let sem = Semaphore::new(parallel_tasks);

    let ctx = Context {
        bucket,
        s3: s3_config,
        client,
        buff_size,
        sem,
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

    let server = Server::bind(&bind_addr)
        .serve(make_service)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.expect("failed to listen for event");
            warn!("shutdown server");
        });

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

    #[arg(long)]
    buff_size: usize,

    #[arg(short, long)]
    parallel_tasks: usize,
}

fn main() -> Result<()> {
    let args: Args = Args::parse();

    let config = std::fs::read(args.s3_config_path)?;
    let s3_config: S3Config = toml::from_slice(&config)?;

    logger_init()?;

    let rt = tokio::runtime::Runtime::new()?;
    info!("Listening on http://{}", args.bind_addr);
    rt.block_on(exec(args.bind_addr, s3_config, args.buff_size, args.parallel_tasks))
}
