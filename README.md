# Edge
A graph database using 'gasm' languiage

# Quick start

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
Then it will serving at http://$ip:$port/$name

# Usage
curl http://$ip:$port/$name/execute -X POST --data "return any"
