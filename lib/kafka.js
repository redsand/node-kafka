
var events = require('events');
var async = require('async');
var zk = require('./zookeeper'), Zookeeper = zk.Zookeeper;

var kafkaBindings;

if (require('fs').existsSync(__dirname + '/../build')) {
  // normal situation
  kafkaBindings = require(__dirname + "/../build/Release/rdkafkaBinding");
} else {
  // the build folder was never created. Default to the precompiled build for cloudfoundry v1.
  kafkaBindings = require(__dirname + "/../_build/Release/rdkafkaBinding");
}

var initialize = kafkaBindings.initialize;
var configure = kafkaBindings.configure;
var topic_configure = kafkaBindings.topic_configure;
var connect = kafkaBindings.connect;
var produce = kafkaBindings.produce;
var consume = kafkaBindings.consume;
var pause = kafkaBindings.pause;
var resume = kafkaBindings.resume;
var stop = kafkaBindings.stop;
var disconnect = kafkaBindings.disconnect;
var setDebug = kafkaBindings.setDebug;

function Consumer(config) {

  this.zookeeper = config.zookeeper || undefined;
  this.brokers = config.brokers || undefined;

  // the following items need to be configured..

  this.clientId = config.clientId || "node-kafka";
  this.groupId = config.groupId || "node-kafka";
  this.debug = config.debug || undefined; // debug=metadata,topic,msg,broker
  this.autoCommit = config.autoCommit || true;
  this.autoCommitIntervalMs = config.autoCommitIntervalMs || 5000;
  this.fetchMaxWaitMs = config.fetchMaxWaitMs || 100;
  this.fetchMinBytes = config.fetchMinBytes || 1;
  this.fetchMaxBytes = config.fetchMaxBytes || 1048576;
  this.compressionCodec = config.compressionCodec || "snappy"; // options are snappy, none or gzip

  this.offsetStoreMethod = config.offsetStoreMethod || undefined;


  // end items that need to be configured

  this.topics = config.topics || [ ];

  this.zkOptions = config.zkOptions || { };

  this.emitter = new events.EventEmitter();
}

Consumer.prototype.removeAllListeners = function(r) {
	return this.emitter.removeAllListeners(r);
}

Consumer.prototype.pause = function(cb) {

	console.log('Testing pause!!!');

	if(!this._rk) {
		throw(new Error("not connected yet"));
	}

	if(!cb) cb = function() { };

	pause(this._rk, cb);
}

Consumer.prototype.resume = function(cb) {

	if(!this._rk) {
		throw(new Error("not connected yet"));
	}

	if(!cb) cb = function() { };

	resume(this._rk, cb);
}

Consumer.prototype.stop = function(cb) {

	if(!this._rk) {
		throw(new Error("not connected yet"));
	}

	if(!cb) cb = function() { };

	stop(this._rk, cb);
}

Consumer.prototype.consume = function(topics, cb) {

	// start our consume process
	// gives time between connect and msg processing to assing on handlers for data
	// for each topic

	if(!this._rk) {
		throw(new Error("not connected yet"));
	}

	if(typeof(this.topics) == 'string') {
		this.topics = [ this.topics ];
	}

	var counter = this.topics.length;

	var localCb = function(topic) {
		// set topic
		// get topic from list
		// add rkTopic reference (will allow us to pause it?)
		console.log('Inside local cb...! ' + counter);
		console.log(topic);

		counter = counter - 1;
		if(cb) {
			console.log('Calling cb');
			cb(topic);
		}
	}

	var self = this;

	var handler = function(x) {

			topic = self.topics[x];
			console.log('Starting consume on topic');
			console.log(topic);
			if(typeof(topic) == 'string') {
				// offset default should be from beginning
				consume(self._rk, topic, -1 , -1, -1, self.emitter, localCb);
			} else {
				console.log('Topic: ' + topic.topic);
				// assume object
				if('topic' in topic && 'partition' in topic && 'offset' in topic && 'offset_max' in topic ) {
					console.log('Honoring all offsets');
					console.log(topic);
					consume(self._rk, topic['topic'], topic['partition'], topic['offset'] , topic['offset_max'], self.emitter, localCb); 
				} else if('topic' in topic && 'partition' in topic && 'offset' in topic) {
					console.log('Honoring all offsets except max');
					consume(self._rk, topic['topic'], topic['partition'], topic['offset'] , -1, self.emitter, localCb); 
				} else if('topic' in topic && 'partition' in topic) {
					console.log('Honoring all using current offset');
					consume(self._rk, topic['topic'], topic['partition'], -1, -1, self.emitter, localCb); 
				} else if('topic' in topic) {
					console.log('Just a topic');
					// should throw an exception
					console.log(topic);
    					throw(new Error("unable to add topic"));
					//consume(self._rk, topic.topic, -1 , -1, -1, self.emitter, localCb);// -1 offset will set to current
				}
			}

	};

	callList = [ ]
	for(x in this.topics) {
		callList.push( handler.bind(null, x) );
		//console.log('Consuming topic in parallel');
		//handler(x);
	}

	//console.log(callList);
	async.parallel(
		callList
	);


/*
	// synchronous

	for(x in this.topics) {

			topic = self.topics[x];
			console.log('Starting consume on topic');
			console.log(topic);
			if(typeof(topic) == 'string') {
				// offset default should be from beginning
				consume(self._rk, topic, -1, -1, -1, self.emitter, localCb);
			} else {
				console.log('Topic: ' + topic.topic);
				// assume object
				if(topic.topic && topic.partition >= 0 && topic.offset >= 0 && topic.offset_max >= 0) {
					console.log('Honoring all offsets');
					consume(self._rk, topic.topic, topic.partition, topic.offset , topic.offset_max, self.emitter, localCb); 
				} else if(topic.topic && topic.partition >= 0 && topic.offset >= 0) {
					console.log('Honoring all offsets except max');
					consume(self._rk, topic.topic, topic.partition, topic.offset , -1, self.emitter, localCb); 
				} else if(topic.topic && topic.partition >= 0) {
					console.log('Honoring all using current offset');
					consume(self._rk, topic.topic, topic.partition, -1, -1, self.emitter, localCb);// -1 offset will set to current
				} else if(topic.topic) {
					console.log('Just a topic');
					consume(self._rk, topic.topic, -1 , -1, -1, self.emitter, localCb);// -1 offset will set to current
				}
			}
	}

*/

}

Consumer.prototype.connect = function(cb) {
  var self = this;


  this._rk = initialize();

  if(!this._rk)
    throw(new Error("unable to initialize"));

  /* global */
  if(this.debug)
	configure(this._rk, "debug", self.debug);

  configure(this._rk, "client.id", self.clientId);
  configure(this._rk, "compression.codec", self.compressionCodec);

  /* consumer specific */
  configure(this._rk, "fetch.wait.max.ms", self.fetchMaxWaitMs);
  configure(this._rk, "fetch.min.bytes", self.fetchMinBytes);
  configure(this._rk, "fetch.message.max.bytes", self.fetchMaxBytes);
  configure(this._rk, "group.id", this.groupId);

  /* consumer and topic config specific */
  // requires 0.8.1+ if broker, else file is used
  if(self.offsetStoreMethod)
	  topic_configure(this._rk, "offset.store.method", self.offsetStoreMethod); // broker or file is it...
  topic_configure(this._rk, "auto.commit.enable", self.autoCommit);
  topic_configure(this._rk, "auto.commit.interval.ms", self.autoCommitIntervalMs);

  mode = 1; // 1 == brokers mode, 2 == zookeeper mode
  if(this.zookeeper && !this.brokers) { // zookeper mode only
	mode = 2;
	// use nodejs zookeeper to get list of brokers
	// add maint thread to periodically update brokers
	var zk = this.zk = new Zookeeper(this.zookeeper, this.zkOptions);
	zk.once('init', function(brokers) {

		self.brokers = "";
		Object
		.keys(brokers)
		.some(function (key, index) {
			var broker = brokers[key];
			var addr = broker.host + ':' + broker.port;
			self.brokers += addr + ',';
		});

		self.brokers = self.brokers.substring(0, self.brokers.length-1);

		// before we connect!, let's just configure

		/* global */
		configure(self._rk, "metadata.broker.list", self.brokers);

		// 2 == consumer mode
		connect(self._rk, 2, function(err) {
			cb(err);
		});
	});

	zk.on('error', function(err) {
		console.log('Zookeeper error! ' + err);
	});

	return;
  }

  // 2 == consumer mode
  configure(this._rk, "metadata.broker.list", this.brokers);

  connect(this._rk, 2, function(err) {
	    cb(err);
  });
}

Consumer.prototype.on = function(key, cb) {
	this.emitter.on(key, cb);
}

Consumer.prototype.disconnect = function(cb) {
  var self = this;
  disconnect(self._rk, function(err) {
    console.log('Inside callback');
    if(cb)
	    cb(err);
  });
};

function Producer(config) {
  this.partition = config.partition || -1; // send to UA, distribute even amongst partitions
  this.brokers = config.brokers || "localhost:9092";
  this.clientId = config.clientId || "node-kafka";
  this.compressionCodec = config.compressionCodec || "snappy"; // options are snappy, none or gzip
  this.topic = config.topic;
}

Producer.prototype.connect = function(cb) {
  var self = this;

  console.log('producer connect');
  this._rk = initialize();
  console.log('init...');

  if(!this._rk)
    throw(new Error("unable to initialize"));

  /* global */
  if(this.debug)
	configure(this._rk, "debug", self.debug);

  configure(this._rk, "client.id", self.clientId);
  configure(this._rk, "compression.codec", self.compressionCodec);

  mode = 1; // 1 == brokers mode, 2 == zookeeper mode
  if(this.zookeeper && !this.brokers) { // zookeper mode only
	mode = 2;
	// use nodejs zookeeper to get list of brokers
	// add maint thread to periodically update brokers
	var zk = this.zk = new Zookeeper(this.zookeeper, this.zkOptions);
	zk.once('init', function(brokers) {

		self.brokers = "";
		Object
		.keys(brokers)
		.some(function (key, index) {
			var broker = brokers[key];
			var addr = broker.host + ':' + broker.port;
			self.brokers += addr + ',';
		});
		self.brokers = self.brokers.substring(0, self.brokers.length-1);

		configure(self._rk, "metadata.broker.list", self.brokers);

		// 1 == producer mode
		connect(self._rk, 1, function(err) {
			cb(err);
		});

	});

	zk.on('error', function(err) {
		console.log('Zookeeper error! ' + err);
	});

	return;
  }

  configure(this._rk, "metadata.broker.list", this.brokers);

  if(mode == 1) {
	// 1 == producer mode
     connect(this._rk, 1, function(err) {
	    cb(err);
     });
  }
}

Producer.prototype.disconnect = function(cb) {
  var self = this;
  disconnect(self._rk, function(err) {
    console.log('Inside callback');
    if(cb)
	    cb(err);
  });
};

Producer.prototype.send = function(message, topic, partition, cb) {
  var self = this;
  var emitter;

  if (topic && topic.constructor && topic.call && topic.apply) {
	cb = topic;
	topic = undefined;
	partition = undefined;
  } else if (partition && partition.constructor && partition.call && partition.apply) {
	cb = partition;
	partition = undefined;
  }

  if (self._rk) {
    emitter = new events.EventEmitter();
    if (cb) {
      emitter.on("sent", cb);
    }
    if (typeof message === "object") {
      message = JSON.stringify(message);
    }
    produce(this._rk, topic || this.topic, message, partition || this.partition, emitter, function(err) {
      if (err) {
        emitter.emit("error", err);  // if error, also emit 'error'
      } else
	    emitter.emit("done", true );
    });
    return emitter;
  } else {
    throw(new Error("not connected yet"));
  }
};

exports.Producer = Producer;
exports.Consumer = Consumer;
exports.setDebug = setDebug;

setDebug(true);
