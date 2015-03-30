# node-kafka

Node.js binding for [librdkafka](https://github.com/edenhill/librdkafka).

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
  zookeeper: "localhost:2181",
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

Please see examples directory for more.  This project needs more testing and further direction.

## TEST
```bash
node example.js
node example2.js
```

## LICENSE
See LICENSE, and LICENSE.* for dependencies
