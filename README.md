# commp_worker

Used to batch calculate Car file [Piece CID](https://spec.filecoin.io/systems/filecoin_files/piece/) from S3 object storage server, 
CommP Worker has higher computing throughput than official

In which scenario should this repo be used?
- You have a lot of Car files that need to calculate CommP
- Your Car files are stored on S3 storage

### Usage
```shell
Usage: commp_worker <COMMAND>

Commands:
  daemon
  info
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

CommP Worker startup script:
```shell
ulimit -n 65535
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

#### API
##### POST add

```shell
curl --location 'http://127.0.0.1:30000/add' \
--header 'Content-Type: application/json' \
--data '{
    "key": "qiniu:car/baga6ea4seaqlg4hu7x3mj4mezcsaviet7tl6gqbqbhpk2tu5holcv47gtqqcucq",
    "padded_piece_size": 268435456
}'
```
Request Data

`key`: Car file key

prefix of key

- `qiniu`: key on s3 storage
- `/`: local file system

`padded_piece_size`: The size of Car after filling with empty data

Response

Piece CID String

### Build
```shell
cargo build --release
```
