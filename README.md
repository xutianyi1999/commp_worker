# commp_worker
### Usage
```shell
Usage: commp_worker.exe --s3-config-path <S3_CONFIG_PATH> --bind-addr <BIND_ADDR> --buff-size <BUFF_SIZE> --parallel-tasks <PARALLEL_TASKS>

Options:
  -s, --s3-config-path <S3_CONFIG_PATH>  s3 config file path
  -b, --bind-addr <BIND_ADDR>
      --buff-size <BUFF_SIZE>
  -p, --parallel-tasks <PARALLEL_TASKS>
  -h, --help                             Print help
  -V, --version                          Print version
```

example:
```shell
TOKIO_WORKER_THREADS=20 ./commp_worker -b "0.0.0.0:30000" -s ./s3.toml --buff-size 536870912 -p 80
```

### s3config.toml
```toml
host = "http://xxx"
bucket = "xxx"
region = "cn-east-1"
access_key = "xxx"
secret_key = "xxx"
```

### Build
```shell
cargo build --release
```
