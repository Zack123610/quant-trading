# quant-trading
For quantitative trading practice.

## Set up Kafka Container
1. Start a Kafka Container with Docker
```
docker run -d -p 9092:9092 --rm --name broker apache/kafka:latest
```
2. Open a shell in the broker container
```
docker exec --workdir /opt/kafka/bin/ -it broker sh
```
3. From inside the container, create a topic called `eurusd_bidask`:
```
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic eurusd_bidask
```