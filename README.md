# Redis Safe Delete
----
a simple tool to delete huge key safely in redis


## Installing

```shell
go get -v github.com/shanicky/redis-safe-delete
```

## Using

```
redis-safe-delete --address localhost:6379 --key __a_huge_key__ [--count 20]
```