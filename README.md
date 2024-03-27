# Edge

## What is Edge?

An intelligent graph database. It targets 2 types of tasks: 

1. Executing scripts to read or write data.
2. Helping generate scripts. (Not ready yet)

## Quick start

```sh
edge [config.toml] --port 8005
```

config.toml
```toml
# name = "edge"
# ip = "0.0.0.0"
# port = 80
db_url = "mysql://user:pass@host/database"
# thread_num = 8
# log_level = "INFO"
```

Then it will serve at http://$ip:$port/$name

## Usage

curl http://$ip:$port/$name/execute -X POST --data "$->$ouput = = hello _"
