# commp_worker
### Usage
```shell
Usage: commp_worker.exe <COMMAND>

Commands:
  daemon
  info
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

example:
```shell
TOKIO_WORKER_THREADS=20 ./commp_worker daemon -b "0.0.0.0:30000" -s ./s3.toml --buff-size 536870912 -p 80
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
