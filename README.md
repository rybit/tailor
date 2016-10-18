Simple tool to just tail a nats subject and dump it out to the std out

``` sh
glide install
```

example:
``` sh
go run main.go --tls \
   --tlscert /usr/local/etc/certs/nats-server.pem \
   --tlskey /usr/local/etc/certs/nats-server-key.pem \
   --tlscacert /usr/local/etc/certs/ca.pem \
   -s "nats://nats.lo:4222" "logs.>"
```
