JetStream

To see stream run nats-box

```
docker run --rm -it --network=host natsio/nats-box
```

Then to view stream content run

```
nats stream view ORDERS
```