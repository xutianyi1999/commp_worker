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
ulimit -n 65535
sysctl -w vm.nr_hugepages=65536
export MIMALLOC_RESERVE_HUGE_OS_PAGES=128
TOKIO_WORKER_THREADS=20 ./commp_worker daemon -b "0.0.0.0:30000" -s ./s3.toml --buff-size 536870912 -p 80
```

### s3config.toml
```toml
["/root/f001"]
host = "http://xxx"
region = "cn-east-1"
bucket = "xxx"
access_key = "xxx"
secret_key = "xxx"

["/root/f002"]
host = "http://xxx"
region = "cn-east-1"
bucket = "xxx"
access_key = "xxx"
secret_key = "xxx"
```

### Build
```shell
cargo build --release
```
