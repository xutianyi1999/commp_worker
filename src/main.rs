use std::{io, task, vec};
use std::cmp::min;
use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::{Display, Formatter};
use std::io::Read;
use std::net::{IpAddr, SocketAddr};
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use anyhow::{anyhow, ensure, Result};
use chrono::Utc;
use cid::Cid;
use clap::{Parser, ValueEnum};
use futures_util::{FutureExt, StreamExt};
use futures_util::future::Map;
use hyper::{Body, Client, http, Method, Request, Response, Server, Uri};
use hyper::body::Buf;
use hyper::client::connect::dns::{GaiAddrs, GaiFuture, GaiResolver, Name};
use hyper::client::HttpConnector;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, Service, service_fn};
use log::{debug, error, info, LevelFilter, warn};
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Root};
use log4rs::encode::pattern::PatternEncoder;
use mimalloc::MiMalloc;
use multihash::Multihash;
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use prettytable::{row, Table};
use rand::Rng;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use serde::{Deserialize, Serialize};
use thread_priority::ThreadPriority;
use tokio::sync::Semaphore;
use url::Url;

use crate::bytes_amount::{PaddedBytesAmount, UnpaddedBytesAmount};
use crate::commitment::Commitment;

mod bytes_amount;
mod commitment;
mod fr32_util;

type TaskID = u32;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
static BUFFER_POOL: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());
static TASKS: Lazy<RwLock<HashMap<TaskID, Task>>> = Lazy::new(|| RwLock::new(HashMap::new()));

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

struct Bytes {
    inner: Vec<u8>,
}

impl Bytes {
    fn alloc(size: usize) -> Self {
        let inner = BUFFER_POOL.lock().pop().unwrap_or_else(|| Vec::<u8>::with_capacity(size));

        Bytes {
            inner
        }
    }
}

impl Drop for Bytes {
    fn drop(&mut self) {
        let mut buff = std::mem::take(&mut self.inner);
        buff.clear();

        BUFFER_POOL.lock().push(buff);
    }
}

impl Deref for Bytes {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Bytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
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

#[derive(Serialize, Deserialize, Clone)]
struct Req {
    key: String,
    padded_piece_size: u64,
}

struct Context {
    s3: HashMap<String, S3Config>,
    client: Client<HttpConnector<RoundRobin>>,
    buff_size: usize,
    sem: Semaphore,
}

#[derive(ValueEnum, Serialize, Deserialize, Copy, Clone, PartialEq)]
enum TaskState {
    Wait,
    Run,
}

impl Display for TaskState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskState::Wait => write!(f, "wait"),
            TaskState::Run => write!(f, "run")
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Task {
    req: Req,
    remote: IpAddr,
    state: TaskState,
    time: i64,
}

#[derive(Clone, Copy)]
enum Source<'a> {
    S3(&'a str),
    Local(&'a str),
}

impl<'a> TryFrom<&'a str> for Source<'a> {
    type Error = anyhow::Error;

    fn try_from(value: &'a str) -> std::result::Result<Self, Self::Error> {
        let s = match sscanf::sscanf!(value, "qiniu:{}", str) {
            Ok(path) => Source::S3(path),
            Err(_) => Source::Local(value)
        };
        Ok(s)
    }
}

fn match_bucket<'a>(key: &str, configs: &'a HashMap<String, S3Config>) -> Option<&'a S3Config> {
    configs.iter()
        .find(|(prefix, _)| key.starts_with(*prefix))
        .map(|(_, c)| c)
}

async fn add(
    ctx: Arc<Context>,
    req: Request<Body>,
    remote: IpAddr,
) -> Result<Response<Body>, http::Error> {
    let task_id = rand::random();

    let fut = async {
        let body = hyper::body::aggregate(req.into_body()).await?;
        let req: Req = serde_json::from_reader(body.reader())?;

        let upsize = UnpaddedBytesAmount::from(PaddedBytesAmount(req.padded_piece_size));
        ensure!(upsize.0 % 127 == 0);

        let t = Task {
            req: req.clone(),
            remote,
            state: TaskState::Wait,
            time: Utc::now().timestamp(),
        };

        TASKS.write().insert(task_id, t);
        let _permit = ctx.sem.acquire().await?;

        let t = Utc::now().timestamp();
        match TASKS.write().get_mut(&task_id) {
            None => return Err(anyhow!("Can't change task state")),
            Some(task) => {
                task.state = TaskState::Run;
                task.time = t;
            }
        };

        let (tx, rx) = std::sync::mpsc::channel::<Bytes>();

        let join = tokio::task::spawn_blocking(move || {
            let mut commitment = Commitment::new();

            let mut chunk = [0u128; 8];
            let chunk: &mut [u8; 128] = unsafe { std::mem::transmute(&mut chunk) };

            let mut compute_buff = [0u128; 8];
            let compute_buff: &mut [u8; 128] = unsafe { std::mem::transmute(&mut compute_buff) };

            let mut read_data = 0;
            let mut remain = 0;

            while let Ok(buff) = rx.recv() {
                read_data += buff.len();
                let mut right = buff.as_slice();

                if remain != 0 {
                    let (l, r) = right.split_at(min(127 - remain, right.len()));
                    right = r;
                    chunk[remain..remain + l.len()].copy_from_slice(l);

                    if l.len() + remain == 127 {
                        remain = 0;
                        commitment.consume(chunk, compute_buff);
                    } else {
                        remain += l.len();
                    }
                }

                while right.len() >= 127 {
                    let (l, r) = right.split_at(127);
                    chunk[0..127].copy_from_slice(l);
                    right = r;

                    commitment.consume(chunk, compute_buff);
                }

                if right.len() != 0 {
                    chunk[..right.len()].copy_from_slice(right);
                    remain = right.len()
                }
            }

            if remain != 0 {
                for x in chunk[remain..].iter_mut() {
                    *x = 0;
                }
                read_data += 127 - remain;
                commitment.consume(chunk, compute_buff);
            }

            let zero_size = upsize.0.saturating_sub(read_data as u64);
            ensure!(zero_size % 127 == 0);
            commitment.finish(zero_size / 127 * 2)
        });

        let source = Source::try_from(req.key.as_str())?;
        let buff_size = ctx.buff_size;

        match source {
            Source::S3(path) => {
                let s3= match_bucket(path, &ctx.s3).ok_or_else(|| anyhow!("corresponding s3 config of {} was not found", path))?;

                let bucket = Bucket::new(
                    Url::parse(&s3.host)?,
                    UrlStyle::VirtualHost,
                    s3.bucket.clone(),
                    s3.region.clone(),
                )?;

                let object_url = get_object_url(path, &bucket, s3)?;
                let resp = ctx.client.get(Uri::from_str(object_url.as_str())?).await?;

                if resp.status() != 200 {
                    return Ok(resp);
                }

                let mut body = resp.into_body();
                let mut buff = Bytes::alloc(buff_size);

                while let Some(res) = body.next().await {
                    let packet = res?;

                    if buff.len() + packet.len() > buff.capacity() {
                        ensure!(ctx.buff_size >= packet.len());

                        let buff = std::mem::replace(&mut buff, Bytes::alloc(buff_size));
                        tx.send(buff).map_err(|_| anyhow!("compute task has been shutdown"))?;
                    }
                    buff.extend(packet);
                }

                if !buff.is_empty() {
                    tx.send(buff).map_err(|_| anyhow!("compute task has been shutdown"))?;
                }

                drop(tx);
            }
            Source::Local(path) => {
                let path = path.to_string();

                let read_task = tokio::task::spawn_blocking(move || {
                    let mut file = std::fs::File::open(path)?;
                    const COPY_BUF_SIZE: usize = 65536;

                    let mut copy_buf = vec![0u8; COPY_BUF_SIZE];
                    let mut buff = Bytes::alloc(buff_size);

                    loop {
                        if buff.len() == buff.capacity() {
                            let old = std::mem::replace(&mut buff, Bytes::alloc(buff_size));
                            tx.send(old).map_err(|_| anyhow!("compute task has been shutdown"))?;
                        }

                        let len = file.read(&mut copy_buf[..min(COPY_BUF_SIZE, buff.capacity() - buff.len())])?;

                        if len == 0 {
                            break;
                        }
                        buff.extend_from_slice(&copy_buf[..len]);
                    }

                    if !buff.is_empty() {
                        tx.send(buff).map_err(|_| anyhow!("compute task has been shutdown"))?;
                    }
                    Result::<_, anyhow::Error>::Ok(())
                });
                read_task.await??;
            }
        }

        let cid_buff = join.await??;

        let hash = Multihash::wrap(0x1012, &cid_buff)?;
        let c = Cid::new_v1(0xf101, hash);

        info!("compute {} commp use {} secs", req.key, Utc::now().timestamp() - t);
        Result::<_, anyhow::Error>::Ok(Response::new(Body::from(c.to_string())))
    };

    let res = match fut.await {
        Ok(resp) => Ok(resp),
        Err(e) => {
            error!("{}", e);

            Response::builder()
                .status(500)
                .body(Body::from(e.to_string()))
        }
    };

    TASKS.write().remove(&task_id);
    res
}

#[derive(Serialize, Deserialize)]
struct Condition {
    remote: Vec<IpAddr>,
    state: Option<TaskState>,
    key: Vec<String>,
}

async fn info(req: Request<Body>) -> Result<Response<Body>, http::Error> {
    let fut = async {
        let body = hyper::body::aggregate(req.into_body()).await?;
        let cond: Condition = serde_json::from_reader(body.reader())?;

        let tasks = TASKS.read().clone();

        let tasks: Vec<Task> = tasks.into_values()
            .filter(|task| {
                if cond.remote.is_empty() {
                    return true;
                }
                cond.remote.contains(&task.remote)
            })
            .filter(|task| cond.state.map(|v| v == task.state).unwrap_or(true))
            .filter(|task| {
                if cond.key.is_empty() {
                    return true;
                }
                cond.key.contains(&task.req.key)
            })
            .collect();

        let bytes = serde_json::to_vec(&tasks)?;
        Result::<_, anyhow::Error>::Ok(Response::new(Body::from(bytes)))
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

async fn router(
    ctx: Arc<Context>,
    remote: IpAddr,
    req: Request<Body>,
) -> Result<Response<Body>, http::Error> {
    match req.uri().path() {
        "/add" => add(ctx, req, remote).await,
        "/info" => info(req).await,
        _ => {
            Response::builder()
                .status(404)
                .body(Body::empty())
        }
    }
}

fn info_call(
    limit: usize,
    dst: &str,
    cond: Condition,
) -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{}/info", dst))
        .body(Body::from(serde_json::to_vec(&cond)?))?;

    rt.block_on(async {
        let c = hyper::client::Client::new();
        let resp = c.request(req).await?;

        let (parts, body) = resp.into_parts();

        if parts.status != 200 {
            let bytes = hyper::body::to_bytes(body).await?;
            let msg = String::from_utf8(bytes.to_vec())?;
            return Err(anyhow!("HTTP response code: {}, message: {}", parts.status.as_u16(), msg));
        }

        let body = hyper::body::aggregate(body).await?;
        let tasks: Vec<Task> = serde_json::from_reader(body.reader())?;
        println!("count: {}", tasks.len());

        let now = Utc::now().timestamp();

        let mut table = Table::new();
        table.add_row(row!["KEY", "STATE", "REMOTE", "ELAPSED (SEC)"]);

        for x in tasks.into_iter().take(limit) {
            table.add_row(row![x.req.key, x.state, x.remote, now - x.time]);
        }

        table.printstd();
        Ok(())
    })
}

async fn daemon(
    bind_addr: SocketAddr,
    configs: HashMap<String, S3Config>,
    buff_size: usize,
    parallel_tasks: usize,
) -> Result<()> {
    let mut connector = HttpConnector::new_with_resolver(RoundRobin { inner: GaiResolver::new() });
    // 1GB
    connector.set_recv_buffer_size(Some(1073741824));

    let client = Client::builder()
        // 64MB
        .http1_max_buf_size(67108864)
        .build(connector);

    let sem = Semaphore::new(parallel_tasks);

    let ctx = Context {
        s3: configs,
        client,
        buff_size,
        sem,
    };
    let ctx = Arc::new(ctx);

    let make_service = make_service_fn(move |addr: &AddrStream| {
        let ctx = ctx.clone();
        let remote = addr.remote_addr().ip();

        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                router(ctx.clone(), remote, req)
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
enum Args {
    Daemon {
        /// s3 config file path
        #[arg(short, long)]
        s3_config_path: PathBuf,

        #[arg(short, long, default_value = "0.0.0.0:30000")]
        bind_addr: SocketAddr,

        #[arg(long, default_value_t = 536870912)]
        buff_size: usize,

        #[arg(short, long, default_value_t = 80)]
        parallel_tasks: usize,
    },
    Info {
        #[arg(short, long, default_value = "127.0.0.1:30000")]
        api_addr: String,

        #[arg(short, long)]
        remote: Vec<IpAddr>,

        #[arg(short, long)]
        state: Option<TaskState>,

        #[arg(short, long)]
        key: Vec<String>,

        #[arg(short, long, default_value_t = 20)]
        limit: usize,
    },
}

fn exec() -> Result<()> {
    let args: Args = Args::parse();

    match args {
        Args::Daemon {
            s3_config_path,
            bind_addr,
            buff_size,
            parallel_tasks,
        } => {
            let config = std::fs::read(s3_config_path)?;
            let configs: HashMap<String, S3Config> = toml::from_slice(&config)?;

            logger_init()?;

            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .on_thread_start(|| {
                    thread_priority::set_current_thread_priority(ThreadPriority::Max).expect("failed to set thread priority");
                })
                .build()?;

            info!("Listening on http://{}", bind_addr);
            rt.block_on(daemon(bind_addr, configs, buff_size, parallel_tasks))
        }
        Args::Info {
            api_addr,
            remote,
            state,
            key,
            limit
        } => {
            let cond = Condition {
                remote,
                state,
                key,
            };

            info_call(limit, &api_addr, cond)
        }
    }
}

fn main() -> ExitCode {
    match exec() {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{}", e);
            ExitCode::FAILURE
        }
    }
}
