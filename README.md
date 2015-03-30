# node-kafka

Node.js binding for [librdkafka](https://github.com/edenhill/librdkafka).

Only connect and produce are implemented so far.  consume will be forthcoming (but no immediate need)

## KAFKA SETUP

Tested with kafka_2.8.0-0.8.0-beta1

From kafka folder

### start servers
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```
### create topic
```bash
bin/kafka-create-topic.sh --zookeeper localhost:2181 --replica 1 --partition 1 --topic test
```
### check topic
```bash
bin/kafka-list-topic.sh --zookeeper localhost:2181
```
### consumer
```bash
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning
```
### test producer
```bash
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
```

## BUILD
### Configure
```bash
node-gyp configure
```

### Initial build
```bash
node-gyp build
```

### Rebuild
```bash
node-gyp rebuild
```

## EXAMPLE
```javascript
var kafka = require("./lib/kafka");

var producer = new kafka.Producer({
  brokers: "localhost:9092",
  partition: 0,
  topic: "test"
});

producer.connect(function() {
  producer.send('message', function(err) {
    ...
  }).on("sent", function(err) {
    ...
  }).on("delivery", function(err, length) {
    ...
  }).on("error", function(err) {
    ...
  });
})
```

## TEST
```bash
node example.js
node example2.js
```

## LICENSE
See LICENSE, and LICENSE.* for dependencies
