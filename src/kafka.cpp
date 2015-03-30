

//! New code, Copyright 2015 HAWK Network Defense, Inc.
//! Tim Shelton


#include <v8.h>
#include <node.h>
#include <node_version.h>
#include <node_buffer.h>
#include <map>
#include <iostream>
#include <stdexcept>
#include <sstream>
#include <vector>
#include <string>
#include <queue>
#include <syslog.h>
#include <stdarg.h>
#include <cstdlib>
#include <limits.h>
#include <string.h>


#include <rdkafkacpp.h>

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
#include <uv.h>
#endif


using namespace v8;
using namespace node;


//unsigned int QUEUE_MAX_SIZE = 25000;


static bool debug_state = false;

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
static Handle<Value>
setDebug(const Arguments &args) {
  HandleScope scope;
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
void setDebug(const FunctionCallbackInfo<Value>&args) {
  Isolate* isolate = Isolate::GetCurrent();
  HandleScope scope(isolate);
#endif

  
  debug_state = args[0]->BooleanValue();
  
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  return scope.Close(Undefined());
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  args.GetReturnValue().Set(Undefined(isolate));
#endif

}
  
static void debug(const char* format, ...) {
  va_list args;
  va_start(args, format);
  if (debug_state) {
    time_t rawtime;
    struct tm* timeinfo;
    char timestamp[80];

    time(&rawtime);
    timeinfo = localtime(&rawtime);
    strftime(timestamp, 80, "kafka - %D %H:%M:%S - ", timeinfo);
    printf(timestamp);
    vprintf(format, args);
  }
  va_end(args);
}

struct ConsumeBaton;

struct ConfigureBaton {

  // global
  RdKafka::Conf *conf;
  RdKafka::Conf *tconf;
  int mode;

  // producer specific...
  RdKafka::Producer *producer;


  // consumer specific...
  // RdKafka::Consumer *consumer;
  // this was only 1 topic at a time, lets go big or go home!!!!

  // lookup array of topics to manage....
  // topic, partition#
  std::map <std::string, std::pair<RdKafka::Consumer *, RdKafka::Topic *> > *topicMap;

  std::map <std::string , uv_thread_t*> *threadMap;

  uv_mutex_t _EmitAsyncMessageMutex;
  uv_async_t _EmitAsyncMessage;
  std::queue<RdKafka::Message *> _EmitAsyncMessageQueue;

  uv_mutex_t _EmitAsyncErrorMutex;
  uv_async_t _EmitAsyncError;
  std::queue<std::string> _EmitAsyncErrorQueue;

  uv_mutex_t _EmitAsyncDoneMutex;
  uv_async_t _EmitAsyncDone;
  std::queue<std::string> _EmitAsyncDoneQueue;

  bool global_run;
  bool global_pause;

};

struct ConnectBaton {
  Persistent<Function> callback;
  bool error;
  std::string error_message;

  struct ConfigureBaton *config;
  
  uv_async_t async;
};



struct PollBaton {
  struct ConfigureBaton *config;
  uv_async_t async;
};

void poll_events(void* arg) {
  PollBaton* baton = static_cast<PollBaton*>(arg);
  
  debug("poll_events start\n");
  if(baton->config->mode == 1) {
	if(baton->config->producer)
		baton->config->producer->poll(5000);
  } else if(baton->config->mode == 2) {
  } else {
	debug("Unable to find connection assigned within poll baton, unable to poll");
  }

  baton->async.data = baton;
  uv_async_send(&baton->async);
  debug("poll_events end\n");
}



void ConnectAsyncWork(uv_work_t* req) {
  // no v8 access here!
  debug("ConnectAsyncWork start\n");
  ConnectBaton* baton = static_cast<ConnectBaton*>(req->data);

  std::string errstr;

  // mode
  if(baton->config->mode == 1) {
	baton->config->producer = RdKafka::Producer::create(baton->config->conf, errstr);
	if(!baton->config->producer) {
		baton->error = true;
		baton->error_message = errstr;
	}
  } else {
	// do nothing...
  }

  PollBaton* poll_baton = new PollBaton();
  poll_baton->config = baton->config;
  poll_baton->async = baton->async;
  uv_thread_t poll_events_id;
  uv_thread_create(&poll_events_id, poll_events, poll_baton);
  
  debug("ConnectAsyncWork end\n");
}

void connectFree(uv_handle_t* w) {
  debug("connectFree start\n");
  ConnectBaton* baton = (ConnectBaton*)w->data;
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10

  debug("connectFree disposing... \n");
  //baton->callback.Dispose();

#endif
  debug("connectFree deleting baton... %p \n", baton);
  //delete baton;
  debug("connectFree unreferencing w (wtf that is)... \n");
  uv_unref(w);
  debug("connectFree end\n");
}


void ConnectAsyncAfter(uv_work_t* req) {
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  HandleScope scope;
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  Isolate* isolate = Isolate::GetCurrent();
  HandleScope scope(isolate);
#endif

    ConnectBaton* baton = static_cast<ConnectBaton*>(req->data);

    debug("ConnectAsyncAfter start\n");
    if (baton->error) {
      baton->error = false;
      debug("ConnectAsyncAfter: error detected\n");

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
      Local<Value> err = Exception::Error(String::New(baton->error_message.c_str()));
      Local<Value> argv[] = { err };
      debug("Calling callback inside connection...\n");
      baton->callback->Call(Context::GetCurrent()->Global(), 1, argv);
      debug("Calling callback inside connection: done...\n");
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
      Local<Value> err = Exception::TypeError(String::NewFromUtf8(isolate,baton->error_message.c_str()));
      Local<Value> argv[] = { err };
      debug("Calling callback inside connection...\n");
      Local<Function>::New(isolate, baton->callback)->Call(isolate->GetCurrentContext()->Global(), 1, argv);
      debug("Calling callback inside connection: done...\n");
#endif

    } else {

      debug("Calling callback inside connection...\n");
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10

      Local<Value> argv[] = {
        Local<Value>::New(Null())
      };
      baton->callback->Call(Context::GetCurrent()->Global(), 1, argv);
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12

      Local<Value> argv[] = {
        Null(isolate)
      };
      Local<Function>::New(isolate, baton->callback)->Call(isolate->GetCurrentContext()->Global(), 1, argv);
#endif
      debug("Calling callback inside connection: done...\n");
    }

    baton->async.data = baton;
    uv_close((uv_handle_t*)&baton->async, connectFree);
    debug("ConnectAsyncAfter end\n");
}


// this can never be called because uv_close has been called
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
void ConnectAsyncCallback(uv_async_t* w, int revents) {
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
void ConnectAsyncCallback(uv_async_t* w) {
#endif
  debug("connectAsyncCallback start\n");
  uv_unref((uv_handle_t*)w);

  // this is a poll baton...

  PollBaton* baton = (PollBaton*)w->data;
  w->data = 0;
  if (baton) {
    //delete baton;
  } else {
    debug("connectAsyncCallback with no baton");
  }
  debug("connectAsyncCallback end\n");
}


// brokers, partition, topic
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
static Handle<Value>
connect(const Arguments &args) {
  HandleScope scope;
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
void
connect(const FunctionCallbackInfo<Value>&args) {
  Isolate* isolate = Isolate::GetCurrent();
  HandleScope scope(isolate);
#endif
  
  Local<Object> obj = args[0]->ToObject();
  ConfigureBaton *configure_baton = (ConfigureBaton *)(v8::External::Cast(*(obj->GetInternalField(0)))->Value());

  // connect(this._rk, 2, function(err) {
  ConnectBaton* baton = new ConnectBaton();

  int mode = static_cast<int>(args[1]->NumberValue());

  Local<Function> callback = Local<Function>::Cast(args[2]);
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  baton->callback = Persistent<Function>::New(callback);
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12

  baton->callback.Reset(isolate, callback); // = Persistent<Function>::New(isolate, callback);
#endif
  
  baton->error = false;

  if(mode != 1 && mode != 2) {
	// no connect mode provided, wtf
	// throw error when i learn how
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
	Local<Value> err = Exception::Error(String::New("Invalid mode specified, modes are: 'consumer' and 'producer'"));
	Local<Value> argv[] = { err };
	baton->callback->Call(Context::GetCurrent()->Global(), 1, argv);
  	return scope.Close(Undefined());
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
      Local<Value> err = Exception::TypeError(String::NewFromUtf8(isolate,"Invalid mode specified, modes are: 'consumer' and 'producer'"));
      Local<Value> argv[] = { err };
      debug("Calling callback inside connection...\n");
      Local<Function>::New(isolate, baton->callback)->Call(isolate->GetCurrentContext()->Global(), 1, argv);
      debug("Calling callback inside connection: done...\n");
      args.GetReturnValue().Set(Undefined(isolate));
#endif
  }

  configure_baton->mode = mode;

  baton->config = configure_baton;
  
  debug("connecting, brokers already configured...\n");
  std::string errstr;
  std::string es;

  // poll thread
  uv_async_init(uv_default_loop(), &baton->async, ConnectAsyncCallback);
  uv_ref((uv_handle_t*)&baton->async);


  // connect thread
  uv_work_t* req = new uv_work_t();
  req->data = baton;
  int status = uv_queue_work(uv_default_loop(), req, ConnectAsyncWork, (uv_after_work_cb)ConnectAsyncAfter);
  
  if (status != 0) {
    fprintf(stderr, "connect: couldn't queue work");
    debug("connect: couldn't queue work\n");

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
    Local<Value> err = Exception::Error(String::New("Could not queue work"));
    Local<Value> argv[] = { err };
    baton->callback->Call(Context::GetCurrent()->Global(), 1, argv);
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
      Local<Value> err = Exception::TypeError(String::NewFromUtf8(isolate,"Could not queue work"));
      Local<Value> argv[] = { err };
      debug("Calling callback inside connection...\n");
      Local<Function>::New(isolate, baton->callback)->Call(isolate->GetCurrentContext()->Global(), 1, argv);
#endif
    uv_close((uv_handle_t*)&baton->async, connectFree);
  }  
  
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  return scope.Close(Undefined());
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  args.GetReturnValue().Set(Undefined(isolate));
#endif
}


#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
static Handle<Value>
disconnect(const Arguments &args) {
  HandleScope scope;
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
void disconnect(const FunctionCallbackInfo<Value>&args) {
  Isolate* isolate = Isolate::GetCurrent();
  HandleScope scope(isolate);
#endif

  debug("disconnect: start...\n");

  Local<Object> obj = args[0]->ToObject();

  // args[1] == cast to rk type

  debug("disconnect: got obj...\n");

  ConfigureBaton *baton = (ConfigureBaton *)(v8::External::Cast(*(obj->GetInternalField(0)))->Value());
  //debug("disconnect: got connect obj...\n");

  // This will actually be a producer/consumer baton? since connect baton ends

  // end our consumer thread

  // for each thread running.... join thread

/*
  for each map iterator, do the following
*/

  typedef std::map<std::string, std::pair<RdKafka::Consumer *, RdKafka::Topic *> >::const_iterator it_type;

  for(it_type iterator = baton->topicMap->begin(); iterator != baton->topicMap->end(); iterator++) {
    debug("Getting partition from key: %s\n", iterator->first.c_str());
    std::string partition_str = iterator->first.substr( iterator->first.find(":") + 1, iterator->first.length() );
    debug("Stopping partition %s for topic %s\n", partition_str.c_str(), iterator->second.second->name().c_str());
    iterator->second.first->stop(iterator->second.second, strtol(partition_str.c_str(), 0, 0));
  }

  baton->global_run = false;
  debug("disconnect: removing threads...\n");

  typedef std::map <std::string , uv_thread_t*>::const_iterator it_type2;

  for(it_type2 iterator = baton->threadMap->begin(); iterator != baton->threadMap->end(); iterator++) {
	uv_thread_join(iterator->second);
	delete iterator->second;
  }


  // we need to really delete our consumer or producer tasks (wherever we've stored them)
  debug("disconnect: baton lookup...\n");

  if(!baton) {
	debug("disconnect: no baton found...\n");
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
	return scope.Close(Undefined());
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
	args.GetReturnValue().Set(Undefined(isolate));
#endif
  }

  if(baton->mode == 1) {
	debug("disconnect: deleting producer....\n");

	while (baton->producer->outq_len() > 0) {
		debug("disconnect: waiting for producer to finish....\n");
		baton->producer->poll(50);
	}
  } else if(baton->mode == 2) {
	// for each topic and partition
	// stop topic/partition
	// then poll for 1 second total....
	debug("disconnect: disconnecting consumer....\n");

	/* signal each consumer thread that we're ending */
	/* wait for all to be completed */

  }

  RdKafka::wait_destroyed(5000);

  // Local<Value> err = Exception::Error(String::New("Invalid mode specified, modes are: 'consumer' and 'producer'"));
  Local<Function> callback = Local<Function>::Cast(args[1]);
  Local<Value> argv[] = { };

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  Persistent<Function> callback_p;
  callback_p = Persistent<Function>::New(callback);
  callback_p->Call(Context::GetCurrent()->Global(), 0, argv);
  return scope.Close(Undefined());
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  callback->Call(isolate->GetCurrentContext()->Global(), 0, argv);
  args.GetReturnValue().Set(Undefined(isolate));
#endif

}

struct ConsumeBaton {
  Persistent<Function> callback;
  Persistent<Object> emitter;

  std::string topic;
  bool error;
  std::string error_message;
  int32_t partition;
  int64_t offset;
  int64_t offset_max;

  bool run;
  bool pause;
  struct ConfigureBaton *config;

  uv_async_t async;
};

struct ProduceBaton {
  Persistent<Function> callback;
  Persistent<Object> emitter;
  std::string message;
  std::string topic;
  int32_t partition;
  size_t delivered_length;
  bool error;
  std::string error_message;

  struct ConfigureBaton *config;
  
  uv_async_t async;
};

class DeliveryReportCb : public RdKafka::DeliveryReportCb {
	private:
		ProduceBaton *baton;

	public:
		DeliveryReportCb() {
			this->baton = NULL;
		}

		void baton_set(ProduceBaton *baton) {
			this->baton = baton;
		}

		void dr_cb (RdKafka::Message &message) {
			// this failed? send notificaiton...
			debug("Message delivery for (%u bytes): %s", message.len() , message.errstr().c_str());
			this->baton->async.data = this->baton;
			uv_async_send(&baton->async);
		}
};

void produceFree(uv_handle_t* w) {
  debug("produceFree start\n");
  uv_unref(w);
  ProduceBaton* baton = (ProduceBaton*)w->data;
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  //baton->callback.Dispose();
  //baton->emitter.Dispose();
#endif
  delete baton;
  debug("produceFree end\n");
}


#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
static void produceAsyncDelivery(uv_async_t* w, int revents) {
  HandleScope scope;
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
static void produceAsyncDelivery(uv_async_t* w) {
  Isolate* isolate = Isolate::GetCurrent();
  HandleScope scope(isolate);
#endif
  debug("produceAsyncDelivery start\n");
  
  ProduceBaton* baton = (ProduceBaton*)w->data;
  
  if (baton) {
    debug("produceAsyncDelivery with baton\n");
  } else {
    debug("produceAsyncDelivery without baton\n");
  }
  
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  if (baton && baton->emitter->IsObject()) {
    debug("produceAsyncDelivery: emitter is an object\n");
    Local<Function> emit = Local<Function>::Cast(baton->emitter->Get(String::New("emit"))); 
    debug("emit is a method %d\n", emit->IsFunction());
    if (baton->error) {
      Local<Value> argv[] = {
        Local<Value>::New(String::New("error")),
        Exception::Error(String::New(baton->error_message.c_str()))
      };
      emit->Call(baton->emitter, 2, argv);
    } else {
      Local<Value> argv[] = {
        Local<Value>::New(String::New("delivery")),
        Local<Value>::New(Integer::New(baton->delivered_length))
      };
      emit->Call(baton->emitter, 2, argv);
    }
  }
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  Local<Object> emitter = Local<Object>::New(isolate, baton->emitter);
  debug("produceAsyncDelivery: emitter is an object\n");
  Local<Function> emit = Local<Function>::Cast(emitter->Get(Handle<Value>(String::NewFromUtf8(isolate,"emit"))));
  debug("emit is a method %d\n", emit->IsFunction());
    if (baton->error) {
      Local<Value> argv[] = {
        Local<Value>::New(isolate, String::NewFromUtf8(isolate, "error")),
        Exception::TypeError(String::NewFromUtf8(isolate,baton->error_message.c_str()))
      };
      emit->Call(emitter, 2, argv);

    } else {
      Local<Value> argv[] = {
        Local<Value>::New(isolate, String::NewFromUtf8(isolate, "delivery")),
        Local<Value>::New(isolate, Integer::New(isolate, baton->delivered_length))
      };
      emit->Call(emitter, 2, argv);
    }

#endif

  if (baton) {
    debug("cleanup produce baton\n");
    uv_close((uv_handle_t*)&baton->async, produceFree);
  }
  debug("produceAsyncDelivery end\n");
}

void ProduceAsyncWork(uv_work_t* req) {
  // no v8 access here!
  std::string errstr;

  debug("ProduceAsyncWork start\n");

  ProduceBaton* baton = static_cast<ProduceBaton*>(req->data);
  
  RdKafka::Topic *topic = RdKafka::Topic::create(baton->config->producer, baton->topic, baton->config->tconf, errstr);

  if(!topic) {
	baton->error = true;
    	baton->error_message = errstr;
  } else {
	  RdKafka::ErrorCode resp = baton->config->producer->produce(topic, baton->partition < 0 ? RdKafka::Topic::PARTITION_UA : baton->partition, RdKafka::Producer::RK_MSG_COPY /* Copy payload */, const_cast<char *>(baton->message.c_str()), baton->message.length(), NULL, NULL);

	  if (resp != RdKafka::ERR_NO_ERROR) {
	    baton->error = true;
	    baton->error_message = RdKafka::err2str(resp);
	    debug("Error %s\n", baton->error_message.c_str());
	  } else
		  debug("sent %ld\n", baton->message.size());

	  baton->config->producer->poll(0);

	  //delete topic;
  }

  debug("ProduceAsyncWork end\n");
}

void ProduceAsyncAfter(uv_work_t* req) {

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  HandleScope scope;
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  Isolate* isolate = Isolate::GetCurrent();
  HandleScope scope(isolate);
#endif

  ProduceBaton* baton = static_cast<ProduceBaton*>(req->data);
  debug("ProduceAsyncAfter start\n");
 
 
  if (baton->error) {
    debug("ProduceAsyncAfter: error detected\n");
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
    Local<Value> err = Exception::Error(String::New(baton->error_message.c_str()));
    Local<Value> argv[] = { err };
    baton->callback->Call(Context::GetCurrent()->Global(), 1, argv);

#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
    Local<Value> err = Exception::TypeError(String::NewFromUtf8(isolate,baton->error_message.c_str()));
    Local<Value> argv[] = { err };
    Local<Function>::New(isolate, baton->callback)->Call(isolate->GetCurrentContext()->Global(), 1, argv);
#endif

  } else {

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10

    Local<Value> argv[] = {
      Local<Value>::New(Null())
    };
    baton->callback->Call(Context::GetCurrent()->Global(), 1, argv);
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
    Local<Value> argv[] = { Undefined(isolate) };
    Local<Function>::New(isolate, baton->callback)->Call(isolate->GetCurrentContext()->Global(), 1, argv);
#endif
  }
  
  debug("ProduceAsyncAfter end\n");
}


#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
static Handle<Value>
produce(const Arguments &args) {
  HandleScope scope;
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
void
produce(const FunctionCallbackInfo<Value>&args) {
  Isolate* isolate = Isolate::GetCurrent();
  HandleScope scope(isolate);
#endif

  
  ProduceBaton* produce_baton = new ProduceBaton();
  
  Local<Object> obj = args[0]->ToObject();

  ConfigureBaton *configure_baton = (ConfigureBaton *)(v8::External::Cast(*(obj->GetInternalField(0)))->Value());

  produce_baton->config = configure_baton;
  // get connection object, and use this async instead?
  
  v8::String::Utf8Value param1(args[1]->ToString());
  produce_baton->topic = std::string(*param1);

  v8::String::Utf8Value param2(args[2]->ToString());
  produce_baton->message = std::string(*param2);
  
  produce_baton->partition = static_cast<int32_t>(args[3]->NumberValue());
  
  Local<Object> emitter = Local<Object>::Cast(args[4]);
  Local<Function> callback = Local<Function>::Cast(args[5]);


#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  produce_baton->emitter = Persistent<Object>::New(emitter);
  produce_baton->callback = Persistent<Function>::New(callback);

#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  produce_baton->emitter.Reset(isolate, emitter);
  produce_baton->callback.Reset(isolate, callback);
#endif

  produce_baton->error = false;

  debug("rk, %p\n", configure_baton->producer);
  debug("partition %d\n", produce_baton->partition);
  debug("message, %ld, %s\n", produce_baton->message.length(), produce_baton->message.c_str());

  std::string errstr;

  DeliveryReportCb  dr_cb;

  dr_cb.baton_set(produce_baton);

  if (configure_baton->conf->set("dr_cb", &dr_cb, errstr) != RdKafka::Conf::CONF_OK) {
    debug("produce: unable to set internal callback, %s", errstr.c_str());

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
    Local<Value> err = Exception::Error(String::New(errstr.c_str()));
    Local<Value> argv[] = { err };
    produce_baton->callback->Call(Context::GetCurrent()->Global(), 1, argv);
    return scope.Close(Undefined());
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
      Local<Value> err = Exception::TypeError(String::NewFromUtf8(isolate,errstr.c_str()));
      Local<Value> argv[] = { err };
      debug("Calling callback inside connection...\n");
      Local<Function>::New(isolate, produce_baton->callback)->Call(isolate->GetCurrentContext()->Global(), 1, argv);
#endif
  }

  uv_async_init(uv_default_loop(), &produce_baton->async, produceAsyncDelivery);
  uv_ref((uv_handle_t*)&produce_baton->async);
  
  uv_work_t* req = new uv_work_t();
  req->data = produce_baton;
  int status = uv_queue_work(uv_default_loop(), req, ProduceAsyncWork, (uv_after_work_cb)ProduceAsyncAfter);
  
  debug("produce uv_queue_work %d\n", status);

  if (status != 0) {
    fprintf(stderr, "produce uv_queue_work error\n");
    debug("produce: couldn't queue work\n");

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
    Local<Value> err = Exception::Error(String::New("Could not queue work for produce"));
    Local<Value> argv[] = { err };
    produce_baton->callback->Call(Context::GetCurrent()->Global(), 1, argv);
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
      Local<Value> err = Exception::TypeError(String::NewFromUtf8(isolate,"Could not queue work for produce"));
      Local<Value> argv[] = { err };
      debug("Calling callback inside connection...\n");
      Local<Function>::New(isolate, produce_baton->callback)->Call(isolate->GetCurrentContext()->Global(), 1, argv);
#endif

    uv_close((uv_handle_t*)&produce_baton->async, produceFree);
  }

  debug("produce: done\n");

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  return scope.Close(Undefined());
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  args.GetReturnValue().Set(Undefined(isolate));
#endif
}



void consumeFree(uv_handle_t* w) {
  debug("consumeFree start\n");
  uv_unref(w);
  ConsumeBaton* baton = (ConsumeBaton*)w->data;
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  //baton->callback.Dispose();
  //baton->emitter.Dispose();
#endif
// not good to delete baton?
  //delete baton;
  debug("consumeFree end\n");
}


static void consumeAsyncReceive(void *param) {
  debug("consumerAsyncReceive start\n");

  std::string topic;
  int partition = 0;
  int64_t offset=0;
 
  ConsumeBaton* baton = static_cast<ConsumeBaton*>(param);

  topic = baton->topic;

  partition = baton->partition;
  offset=baton->offset;
  if(partition < 0) partition = 0;

  debug("consumeAsyncReceive Topic here: %s\n", topic.c_str());
  
  std::string errstr;
  
  debug("Opening topic: %s, partition: %d, offset: %ld\n", static_cast<const char *>(topic.c_str()), partition, offset);
  std::stringstream sstm;
  sstm << topic << ":" << partition;
  std::string key = sstm.str();

  debug("Topic key: %s\n", key.c_str());

  RdKafka::Topic *rktopic;
  RdKafka::Consumer *rkconsumer;
  std::pair< RdKafka::Consumer *, RdKafka::Topic *> valuePair;

  try {
  	valuePair = baton->config->topicMap->at(key);
	rkconsumer = valuePair.first;
	rktopic = valuePair.second;
  } 
  catch (std::out_of_range &e) {
	debug("No rk topic found... creationg one\n");

	rkconsumer = RdKafka::Consumer::create(baton->config->conf, errstr);
	if(!rkconsumer) {
		debug("Unable to create consumer: %s\n", errstr.c_str());

		uv_mutex_lock(&baton->config->_EmitAsyncErrorMutex);
		baton->config->_EmitAsyncErrorQueue.push(errstr);
		uv_async_send(&baton->config->_EmitAsyncError);
		uv_mutex_unlock(&baton->config->_EmitAsyncErrorMutex);

	        return;
	} 


  	rktopic = RdKafka::Topic::create(rkconsumer, topic, baton->config->tconf, errstr);

	if(!rktopic) {
		debug("Unable to create topic on consumer : %s\n", errstr.c_str());
		uv_mutex_lock(&baton->config->_EmitAsyncErrorMutex);
		baton->config->_EmitAsyncErrorQueue.push(errstr);
		uv_async_send(&baton->config->_EmitAsyncError);
		uv_mutex_unlock(&baton->config->_EmitAsyncErrorMutex);
	        return;
	} else {
		std::pair<RdKafka::Consumer *, RdKafka::Topic *> pair;
		pair.first = rkconsumer;
		pair.second = rktopic;
		baton->config->topicMap->insert(std::pair<std::string, std::pair<RdKafka::Consumer *, RdKafka::Topic *> >(key, pair));
	}
  }

  debug("Starting on: %s, partition: %d, offset: %ld\n", static_cast<const char *>(topic.c_str()), partition, offset);
  RdKafka::ErrorCode resp = rkconsumer->start(rktopic, partition, offset);

  if (resp != RdKafka::ERR_NO_ERROR) {
	    debug("Error %s\n", baton->error_message.c_str());
	    uv_mutex_lock(&baton->config->_EmitAsyncErrorMutex);
	    baton->config->_EmitAsyncErrorQueue.push(RdKafka::err2str(resp));
	    uv_mutex_unlock(&baton->config->_EmitAsyncErrorMutex);
	    uv_async_send(&baton->config->_EmitAsyncError);
            return;
  } 


  // while true, or we're told to stop
  while(baton->config->global_run && baton->run) {

	if(baton->config->global_pause | baton->pause) {
		debug("We are set as paused for topic: %s, partition: %d....\n", static_cast<const char *>(topic.c_str()), partition);
		usleep(1000);
		continue;
	}

	debug("Waiting for value from topic: %s, partition: %d....\n", static_cast<const char *>(topic.c_str()), partition);
	RdKafka::Message *msg = rkconsumer->consume(rktopic, partition, 1000);

	RdKafka::ErrorCode err = msg->err();
	if(err == RdKafka::ERR__TIMED_OUT) {
		debug("Message timed out....\n");
		usleep(1000);
	} else if(err == RdKafka::ERR_NO_ERROR) {
		if(msg->offset() < offset) {
			debug("OFFSET IGNOREEDFSDLKFJSDLFKJDSKLFS:JDFKDSFS\n");
			delete msg;
			continue;
		}
		/* Real message */
		//baton->offset = msg->offset();
		if(baton->offset_max >= 0 && msg->offset() >= baton->offset_max) {
			debug("offset max has been reached, ending...: %ld >= %ld\n", msg->offset(), baton->offset_max);
			baton->run = false;
			delete msg;
			break;
		}

		debug("Emitting message: %s %d\n", topic.c_str(), msg->partition());
		// debug("Emitting message: %s %d: %s\n", topic.c_str(), msg->partition(), static_cast<const char *>(msg->payload(), msg->len() ) );

/*
		//bool hitLimit = false;

		while(baton->config->_EmitAsyncMessageQueue.size() >= QUEUE_MAX_SIZE) {
			//hitLimit = true;
			usleep(100);
		}

		if(hitLimit)	
			debug("Hit our limit!!!!\n");
*/


		uv_mutex_lock(&baton->config->_EmitAsyncMessageMutex);
		baton->config->_EmitAsyncMessageQueue.push(msg);
		baton->config->_EmitAsyncMessage.data = baton;
		uv_async_send(&baton->config->_EmitAsyncMessage);
		uv_mutex_unlock(&baton->config->_EmitAsyncMessageMutex);


		// no delete, handle in emit

	} else if(err ==  RdKafka::ERR__PARTITION_EOF) {
			/* Last message */
			// offset_max set and handle
			/*
			if (exit_eof)
				baton->config->global_run = false;
			*/
			debug("Inside partition eof....\n");
			if(baton->offset_max >= 0) {
				debug("max is specified, ending (%ld)\n", baton->offset_max);
				baton->run = false;
			}
			delete msg;
	} else {
			/* Errors */
			debug("Error consuming: %s\n", msg->errstr().c_str());
			uv_mutex_lock(&baton->config->_EmitAsyncErrorMutex);
			baton->config->_EmitAsyncErrorQueue.push(msg->errstr());
			baton->config->_EmitAsyncError.data = baton;
			uv_async_send(&baton->config->_EmitAsyncError);
			uv_mutex_unlock(&baton->config->_EmitAsyncErrorMutex);
			baton->run = false;
			delete msg;
	}
	rkconsumer->poll(0);
  }

  rkconsumer->stop(rktopic, partition);
  rkconsumer->poll(1000);

  //! Done!!!!

  while(!baton->config->_EmitAsyncMessageQueue.empty()) {
	debug("EmitDone waiting, msg queue still full, %ld\n", baton->config->_EmitAsyncMessageQueue.size() );
	uv_mutex_lock(&baton->config->_EmitAsyncMessageMutex);
	baton->config->_EmitAsyncMessage.data = baton;
	uv_async_send(&baton->config->_EmitAsyncMessage);
	uv_mutex_unlock(&baton->config->_EmitAsyncMessageMutex);
  }

  uv_mutex_lock(&baton->config->_EmitAsyncDoneMutex);
  baton->config->_EmitAsyncDoneQueue.push(key);
  //baton->config->_EmitAsyncDoneQueue.push(baton);
  baton->config->_EmitAsyncDone.data = baton;
  uv_async_send(&baton->config->_EmitAsyncDone);
  uv_mutex_unlock(&baton->config->_EmitAsyncDoneMutex);

  debug("Completed consuming on: %s, partition: %d, offset: %ld\n", static_cast<const char *>(topic.c_str()), partition, offset);

}

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
void EmitDone(uv_async_t* w, int status) {
  HandleScope scope;
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
void EmitDone(uv_async_t* w) {
  Isolate* isolate = Isolate::GetCurrent();
  HandleScope scope(isolate);
#endif

	ConsumeBaton* consume_baton = static_cast<ConsumeBaton*>(w->data);

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
	Local<Function> emit = Local<Function>::Cast(consume_baton->emitter->Get(String::New("emit")));

#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
	Local<Object> emitter = Local<Object>::New(isolate, consume_baton->emitter);
	Local<Function> emit = Local<Function>::Cast(emitter->Get(Handle<Value>(String::NewFromUtf8(isolate,"emit"))));
#endif

	if(!consume_baton->config->_EmitAsyncDoneQueue.empty()) {

                Handle<ObjectTemplate> templ = ObjectTemplate::New();

                Local<Object> obj = templ->NewInstance();

		uv_mutex_lock(&consume_baton->config->_EmitAsyncDoneMutex);
		std::string msg = consume_baton->config->_EmitAsyncDoneQueue.front();
		consume_baton->config->_EmitAsyncDoneQueue.pop();
		uv_mutex_unlock(&consume_baton->config->_EmitAsyncDoneMutex);

		//debug("Got message %p\n", msg);

		std::string partition_str = msg.substr( msg.find(":") + 1, msg.length() );
		std::string topic = msg.substr( 0, msg.find(":") );
		debug("Parsed done topic: %s with partition: %s\n", topic.c_str(), partition_str.c_str());

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
                obj->Set(Handle<Value>(String::New("topic")), Handle<Value>(String::New(topic.c_str())));
                obj->Set(Handle<Value>(String::New("partition")), Handle<Value>(Integer::New(static_cast<int>(strtol(partition_str.c_str(),0,0)))));
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
                obj->Set(Handle<Value>(String::NewFromUtf8(isolate, "topic")), Handle<Value>(String::NewFromUtf8(isolate, topic.c_str())));
                obj->Set(Handle<Value>(String::NewFromUtf8(isolate, "partition")), Handle<Value>(Integer::New(isolate, static_cast<int>(strtol(partition_str.c_str(),0,0)))));
#endif

		debug("EmitDone\n");
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10

		Local<Value> argv2[] = {
			Local<Value>::New(String::New("done")),
			Local<Value>::New(obj)
		};

		emit->Call(consume_baton->emitter, 2, argv2);
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12

		Local<Value> argv2[] = {
			Local<Value>::New(isolate, String::NewFromUtf8(isolate, "done")),
			Local<Value>::New(isolate, obj)
		};

		emit->Call(emitter, 2, argv2);
#endif

	}

	// segfaults... use after free likely
	//uv_close((uv_handle_t*)&consume_baton->async, consumeFree);

}

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
void EmitMessage(uv_async_t* w, int status) {
  HandleScope scope;
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
void EmitMessage(uv_async_t* w) {
  Isolate* isolate = Isolate::GetCurrent();
  HandleScope scope(isolate);
#endif


	debug("Starting emit msg.... %p\n", w->data);

	ConsumeBaton* consume_baton = static_cast<ConsumeBaton*>(w->data);

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
	Local<Function> emit = Local<Function>::Cast(consume_baton->emitter->Get(String::New("emit")));

#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
	Local<Object> emitter = Local<Object>::New(isolate, consume_baton->emitter);
	Local<Function> emit = Local<Function>::Cast(emitter->Get(Handle<Value>(String::NewFromUtf8(isolate,"emit"))));
#endif

	unsigned long iter = 0;
	debug("Global run %d and baton %d\n", consume_baton->config->global_run, consume_baton->run);
	while (consume_baton->config->global_run && consume_baton->run) {

                Handle<ObjectTemplate> templ = ObjectTemplate::New();

                Local<Object> obj = templ->NewInstance();


		uv_mutex_lock(&consume_baton->config->_EmitAsyncMessageMutex);
		RdKafka::Message* msg = consume_baton->config->_EmitAsyncMessageQueue.front();
		consume_baton->config->_EmitAsyncMessageQueue.pop();
		uv_mutex_unlock(&consume_baton->config->_EmitAsyncMessageMutex);


#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
                obj->Set(Handle<Value>(String::New("topic")), Handle<Value>(String::New(msg->topic()->name().c_str())));
                obj->Set(Handle<Value>(String::New("value")), Handle<Value>(String::New(static_cast<const char *>(msg->payload()), msg->len() )));
                obj->Set(Handle<Value>(String::New("offset")), Handle<Value>(Integer::New(static_cast<int64_t>(msg->offset()))));
                obj->Set(Handle<Value>(String::New("partition")), Handle<Value>(Integer::New(static_cast<int>(msg->partition()))));
                obj->Set(Handle<Value>(String::New("length")), Handle<Value>(Integer::New(static_cast<int>(msg->len()))));

                if(msg->key()) {
                        obj->Set(Handle<Value>(String::New("key")), Handle<Value>(String::New(static_cast<const char *>((*msg->key()).c_str()))));
                }
		
		Local<Value> argv2[] = {
			Local<Value>::New(String::New("message")),
			Local<Value>::New(obj)
		};

		debug("calling emit msg....\n");

		emit->Call(consume_baton->emitter, 2, argv2);

#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12

                obj->Set(Handle<Value>(String::NewFromUtf8(isolate, "topic")), Handle<Value>(String::NewFromUtf8(isolate, msg->topic()->name().c_str())));

		char *tmp = strndup(static_cast<const char *>(msg->payload()), msg->len());

                obj->Set(Handle<Value>(String::NewFromUtf8(isolate, "value")), Handle<Value>(String::NewFromUtf8(isolate, tmp)));
		free(tmp);


                obj->Set(Handle<Value>(String::NewFromUtf8(isolate, "offset")), Handle<Value>(Integer::New(isolate, static_cast<int64_t>(msg->offset()))));

                obj->Set(Handle<Value>(String::NewFromUtf8(isolate, "partition")), Handle<Value>(Integer::New(isolate, static_cast<int>(msg->partition()))));

                obj->Set(Handle<Value>(String::NewFromUtf8(isolate, "length")), Handle<Value>(Integer::New(isolate, static_cast<int>(msg->len()))));

                if(msg->key()) {
                        obj->Set(Handle<Value>(String::NewFromUtf8(isolate, "key")), Handle<Value>(String::NewFromUtf8(isolate, static_cast<const char *>((*msg->key()).c_str()))));
                }
		Local<Value> argv2[] = {
			Local<Value>::New(isolate, String::NewFromUtf8(isolate, "message")),
			Local<Value>::New(isolate, obj)
		};

		emit->Call(emitter, 2, argv2);

		//MakeCallback(isolate, consume_baton->emitter, Handle<Value>(String::NewFromUtf8(isolate, "emit")), 2, argv2);
#endif

		delete msg;


		if(iter++ > 1000)
			break;

	}

	debug("Ending emit msg....\n");

	// if our queue is stil full, should we resind another async?

}


#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
void EmitError(uv_async_t* w, int status) {
  HandleScope scope;
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
void EmitError(uv_async_t* w) {
  Isolate* isolate = Isolate::GetCurrent();
  HandleScope scope(isolate);
#endif


	ConsumeBaton* consume_baton = static_cast<ConsumeBaton*>(w->data);


#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
	Local<Function> emit = Local<Function>::Cast(consume_baton->emitter->Get(String::New("emit")));

#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
	Local<Object> emitter = Local<Object>::New(isolate, consume_baton->emitter);
	Local<Function> emit = Local<Function>::Cast(emitter->Get(Handle<Value>(String::NewFromUtf8(isolate,"emit"))));
#endif

	if (!consume_baton->config->_EmitAsyncErrorQueue.empty()) {

                Handle<ObjectTemplate> templ = ObjectTemplate::New();

                Local<Object> obj = templ->NewInstance();

		uv_mutex_lock(&consume_baton->config->_EmitAsyncErrorMutex);
		std::string msg = consume_baton->config->_EmitAsyncErrorQueue.front();
		consume_baton->config->_EmitAsyncErrorQueue.pop();
		uv_mutex_unlock(&consume_baton->config->_EmitAsyncErrorMutex);

		debug("Received error from topics: %s\n", msg.c_str());



#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
		Local<Value> argv2[] = {
			Local<Value>::New(String::New("error")),
			Local<Value>::New(String::New(msg.c_str()))
		};

		emit->Call(consume_baton->emitter, 2, argv2);

#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
		Local<Value> argv2[] = {
			Local<Value>::New(isolate, String::NewFromUtf8(isolate, "error")),
			Local<Value>::New(isolate, String::NewFromUtf8(isolate, msg.c_str()))
		};

		emit->Call(emitter, 2, argv2);
#endif
	}

	//uv_close((uv_handle_t*)&consume_baton->async, consumeFree);
}


#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
static Handle<Value>
consume(const Arguments &args) {
  HandleScope scope;
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
void consume(const FunctionCallbackInfo<Value>&args) {
  Isolate* isolate = Isolate::GetCurrent();
  HandleScope scope(isolate);
#endif

  
  // this isn't being perma saved... should save/storage and track somehow for reference...
  // this would give us a list of topics we're consuming in parallel
  ConsumeBaton* consume_baton = new ConsumeBaton();
  
  Local<Object> obj = args[0]->ToObject();

  ConfigureBaton *baton = (ConfigureBaton *)(v8::External::Cast(*(obj->GetInternalField(0)))->Value());

  consume_baton->config = baton;
  consume_baton->pause = false;
  consume_baton->run = true;

  // get connection object, and use this async instead?

  v8::String::Utf8Value param1(args[1]->ToString());
  consume_baton->topic = std::string(*param1);

  consume_baton->partition = static_cast<int32_t>(args[2]->NumberValue());
  if(consume_baton->partition < 0) {
	debug("Not allowed to set UA partition inside consumer, using first partition\n");
	consume_baton->partition = 0;
  }

  consume_baton->offset = static_cast<int64_t>(args[3]->NumberValue());

  if(consume_baton->offset < 0) {
	debug("Setting offset of stored\n");
	consume_baton->offset = RdKafka::Topic::OFFSET_STORED;
  } else if(consume_baton->offset == 0) {
	debug("Setting offset of beginning\n");
	consume_baton->offset = RdKafka::Topic::OFFSET_BEGINNING;
  } // else offset is an exact #
   else
	debug("Setting specific offset: %ld\n", consume_baton->offset);

  consume_baton->offset_max = static_cast<int64_t>(args[4]->NumberValue());

  Local<Object> emitter = Local<Object>::Cast(args[5]);
  Local<Function> callback = Local<Function>::Cast(args[6]);


#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  consume_baton->emitter = Persistent<Object>::New(emitter);
  consume_baton->callback = Persistent<Function>::New(callback);

#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  consume_baton->emitter.Reset(isolate, emitter);
  consume_baton->callback.Reset(isolate, callback);
#endif

  consume_baton->error = false;

  debug("consumer topic, %s\n", consume_baton->topic.c_str());
  debug("consumer partition %d\n", consume_baton->partition);

  std::string errstr;

/*
  EventCb  event_cb;

  event_cb.baton_set(consume_baton);

  if (baton->conf->set("event_cb", &event_cb, errstr) != RdKafka::Conf::CONF_OK) {
    debug("consume: unable to set internal callback, %s", errstr.c_str());
    Local<Value> err = Exception::Error(String::New(errstr.c_str()));
    Local<Value> argv[] = { err };
    consume_baton->callback->Call(Context::GetCurrent()->Global(), 1, argv);
    return scope.Close(Undefined());
  }
*/

/*
  uv_async_init(uv_default_loop(), &consume_baton->async, consumeAsyncReceive);

  debug("Finished setting event cb, setting async receiver loop.\n");
  debug("consumer topic, %s\n", consume_baton->topic.c_str());

  consume_baton->async.data = consume_baton;

  uv_async_send(&consume_baton->async);
*/

  // create thread for consumption



  uv_mutex_init(&baton->_EmitAsyncMessageMutex);
  uv_mutex_init(&baton->_EmitAsyncErrorMutex);
  uv_mutex_init(&baton->_EmitAsyncDoneMutex);


  // setup listeners for emits
  baton->_EmitAsyncMessage.data = consume_baton;
  uv_async_init(uv_default_loop(), &baton->_EmitAsyncMessage, EmitMessage);
  uv_unref((uv_handle_t*)&baton->_EmitAsyncMessage); // allow the event loop to exit while this is running

  baton->_EmitAsyncError.data = consume_baton;
  uv_async_init(uv_default_loop(), &baton->_EmitAsyncError, EmitError);
  uv_unref((uv_handle_t*)&baton->_EmitAsyncError); // allow the event loop to exit while this is running

  baton->_EmitAsyncDone.data = consume_baton;
  uv_async_init(uv_default_loop(), &baton->_EmitAsyncDone, EmitDone);
  uv_unref((uv_handle_t*)&baton->_EmitAsyncDone); // allow the event loop to exit while this is running

  debug("Creating background thread...");

  uv_thread_t *thread_id = new uv_thread_t;

  uv_thread_create(thread_id, consumeAsyncReceive, consume_baton);

  std::stringstream sstm;
  sstm << consume_baton->topic << ":" << consume_baton->partition;
  std::string key = sstm.str();

  baton->threadMap->insert(std::pair<std::string, uv_thread_t *>(key, thread_id));


  // call local callback 
  debug("consume: done\n");

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  Local<Value> argv[] = {
	Local<Value>::New(Null())
  };
  consume_baton->callback->Call(Context::GetCurrent()->Global(), 1, argv);

  return scope.Close(Undefined());
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12

  Local<Value> argv[] = {
	Local<Value>::New(isolate, Undefined(isolate))
  };

  Local<Function>::New(isolate, consume_baton->callback)->Call(isolate->GetCurrentContext()->Global(), 1, argv);


  args.GetReturnValue().Set(Undefined(isolate));
#endif

}


#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
static Handle<Value>
resume(const Arguments &args) {
  HandleScope scope;
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
void resume(const FunctionCallbackInfo<Value>&args) {
  Isolate* isolate = Isolate::GetCurrent();
  HandleScope scope(isolate);
#endif

  debug("resume: begin\n");

  Local<Object> obj = args[0]->ToObject();

  // only works if its a consumer...
  ConfigureBaton *baton = (ConfigureBaton *)(v8::External::Cast(*(obj->GetInternalField(0)))->Value());
  
  if(!baton) {
    debug("consume: unable to get connection baton\n");
    //Local<Value> err = Exception::Error(String::New("Unable to get connection baton"));
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  return scope.Close(Undefined());
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  args.GetReturnValue().Set(Undefined(isolate));
#endif

  }

  baton->global_pause = false;


  Local<Function> callback = Local<Function>::Cast(args[1]);
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10

  Persistent<Function> p_callback = Persistent<Function>::New(callback);
  Local<Value> suc[] = {  Local<Value>::New(Null())  };
  p_callback->Call(Context::GetCurrent()->Global(), 1, suc);
  debug("resume: done\n");
  return scope.Close(Undefined());
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  Local<Value> suc[] = {  Local<Value>::New(isolate, Null(isolate))  };
  callback->Call(isolate->GetCurrentContext()->Global(), 1, suc);
  args.GetReturnValue().Set(Undefined(isolate));
#endif

}

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
static Handle<Value>
pause(const Arguments &args) {
  HandleScope scope;
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
void pause(const FunctionCallbackInfo<Value>&args) {
  Isolate* isolate = Isolate::GetCurrent();
  HandleScope scope(isolate);
#endif

  debug("pause: begin\n");

  Local<Object> obj = args[0]->ToObject();

  // only works if its a consumer...
  ConfigureBaton *baton = (ConfigureBaton *)(v8::External::Cast(*(obj->GetInternalField(0)))->Value());
  
  if(!baton) {
    debug("consume: unable to get connection baton\n");
    //Local<Value> err = Exception::Error(String::New("Unable to get connection baton"));

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  return scope.Close(Undefined());
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  args.GetReturnValue().Set(Undefined(isolate));
#endif

  }

  baton->global_pause = true;

  Local<Function> callback = Local<Function>::Cast(args[1]);
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10

  Persistent<Function> p_callback = Persistent<Function>::New(callback);
  Local<Value> suc[] = {  Local<Value>::New(Null())  };
  p_callback->Call(Context::GetCurrent()->Global(), 1, suc);
  debug("resume: done\n");
  return scope.Close(Undefined());
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  Local<Value> suc[] = {  Local<Value>::New(isolate, Null(isolate))  };
  callback->Call(isolate->GetCurrentContext()->Global(), 1, suc);
  args.GetReturnValue().Set(Undefined(isolate));
#endif

}

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
static Handle<Value>
stop(const Arguments &args) {
  HandleScope scope;
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
void stop(const FunctionCallbackInfo<Value>&args) {
  Isolate* isolate = Isolate::GetCurrent();
  HandleScope scope(isolate);
#endif

  debug("stop: begin\n");

  Local<Object> obj = args[0]->ToObject();

  // only works if its a consumer...
  ConfigureBaton *baton = (ConfigureBaton *)(v8::External::Cast(*(obj->GetInternalField(0)))->Value());
  
  if(!baton) {
    debug("consume: unable to get connection baton\n");
    //Local<Value> err = Exception::Error(String::New("Unable to get connection baton"));


#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  return scope.Close(Undefined());
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  args.GetReturnValue().Set(Undefined(isolate));
#endif

  }

  Local<Function> callback = Local<Function>::Cast(args[1]);

/*
  for each map iterator, do the following
*/

  typedef std::map<std::string, std::pair<RdKafka::Consumer *, RdKafka::Topic *> >::const_iterator it_type;

  for(it_type iterator = baton->topicMap->begin(); iterator != baton->topicMap->end(); iterator++) {
    debug("Getting partition from key: %s\n", iterator->first.c_str());
    std::string partition_str = iterator->first.substr( iterator->first.find(":") + 1, iterator->first.length() );
    debug("Stopping partition %s for topic %s\n", partition_str.c_str(), iterator->second.second->name().c_str());
    iterator->second.first->stop(iterator->second.second, strtol(partition_str.c_str(), 0, 0));
  }

  baton->global_run = false;

/*
  typedef std::map <std::string , uv_thread_t*>::const_iterator it_type2;

  for(it_type2 iterator = baton->threadMap->begin(); iterator != baton->threadMap->end(); iterator++) {
	uv_thread_join(iterator->second);
	delete iterator->second;
  }
*/

  debug("stop: done\n");

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  Persistent<Function> p_callback = Persistent<Function>::New(callback);
  Local<Value> suc[] = {  Local<Value>::New(Null())  };
  p_callback->Call(Context::GetCurrent()->Global(), 1, suc);
  return scope.Close(Undefined());
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  Local<Value> suc[] = {  Local<Value>::New(isolate, Null(isolate))  };
  callback->Call(isolate->GetCurrentContext()->Global(), 1, suc);
  args.GetReturnValue().Set(Undefined(isolate));
#endif

}



#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
static Handle<Value>
initialize(const Arguments &args) {
  HandleScope scope;
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
void initialize(const FunctionCallbackInfo<Value>&args) {
  Isolate* isolate = Isolate::GetCurrent();
  HandleScope scope(isolate);
#endif

  std::string errstr;

  debug("Initializing...\n");

  ConfigureBaton *configure_baton = new ConfigureBaton();

  configure_baton->global_run = true;
  configure_baton->global_pause = false;
  configure_baton->conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  configure_baton->tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
  configure_baton->topicMap = new std::map <std::string, std::pair<RdKafka::Consumer *, RdKafka::Topic *> >;
  configure_baton->threadMap = new std::map <std::string, uv_thread_t *>;


  // set snappy by default! 
  if (configure_baton->conf->set("compression.codec", "snappy", errstr) != RdKafka::Conf::CONF_OK) {
	std::string es;
	debug("Error: %s", errstr.c_str());
	es = "Unable to specify default compression codec: 'snappy': ";
	es.append(errstr);

	// should do something about error to inform the user...
	// now we just return nuffin to show something bad happened

	delete configure_baton->tconf;
	delete configure_baton->conf;
	delete configure_baton;


#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  Local<Value> err = Exception::Error(String::New(es.c_str()));
  return scope.Close(err);
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  Local<Value> err = Exception::Error(String::NewFromUtf8(isolate, es.c_str()));
  args.GetReturnValue().Set(err);
#endif

  }
  
  // build our global struct to hold our tracking values....
  // begin by generating config/topic config

  debug("Saving our object...\n");

  Local<ObjectTemplate> rk_template = ObjectTemplate::New();
  rk_template->SetInternalFieldCount(1);
  Local<Object> obj = rk_template->NewInstance();
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  obj->SetInternalField(0, v8::External::New((void *)configure_baton));
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  obj->SetInternalField(0, v8::External::New(isolate, (void *)configure_baton));
#endif
  // we have our configuration baton!!

  debug("Initialization completed...\n");

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  return scope.Close(obj);
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  args.GetReturnValue().Set(obj);
#endif

}

#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
static Handle<Value>
configure(const Arguments &args) {
  HandleScope scope;
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
void configure(const FunctionCallbackInfo<Value>&args) {
  Isolate* isolate = Isolate::GetCurrent();
  HandleScope scope(isolate);
#endif


  Local<Object> obj = args[0]->ToObject();
  ConfigureBaton *baton = (ConfigureBaton *)(v8::External::Cast(*(obj->GetInternalField(0)))->Value());


  // arg1 is string
  v8::String::Utf8Value param1(args[1]->ToString());
  std::string key = std::string(*param1);

  // arg2 convert to string and set value
  v8::String::Utf8Value param2(args[2]->ToString());
  std::string value = std::string(*param2);

  std::string errstr;

  debug("Configuring %s: %s\n", key.c_str(), value.c_str());

  if (baton->conf->set(key.c_str(), value.c_str(), errstr) != RdKafka::Conf::CONF_OK) {
	debug("Error configuring: %s", errstr.c_str());
  	std::string es;
	es = "Unable to specify configuration key '";
	es.append(key);
	es.append("': ");
	es.append(errstr);
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
	Local<Value> err = Exception::Error(String::New(es.c_str()));
  	return scope.Close( err );
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
	Local<Value> err = Exception::TypeError(String::NewFromUtf8(isolate,es.c_str()));
	args.GetReturnValue().Set(err);
#endif

  }


#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  return scope.Close(Undefined());
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  args.GetReturnValue().Set(Undefined(isolate));
#endif

}


#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
static Handle<Value>
topic_configure(const Arguments &args) {
  HandleScope scope;
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
void topic_configure(const FunctionCallbackInfo<Value>&args) {
  Isolate* isolate = Isolate::GetCurrent();
  HandleScope scope(isolate);
#endif

  Local<Object> obj = args[0]->ToObject();
  ConfigureBaton *baton = (ConfigureBaton *)(v8::External::Cast(*(obj->GetInternalField(0)))->Value());

  // arg1 is string
  v8::String::Utf8Value param1(args[1]->ToString());
  std::string key = std::string(*param1);

  // arg2 convert to string and set value
  v8::String::Utf8Value param2(args[2]->ToString());
  std::string value = std::string(*param2);

  std::string errstr;

  debug("Topic Configuring %s: %s\n", key.c_str(), value.c_str());
  if (baton->tconf->set(key.c_str(), value.c_str(), errstr) != RdKafka::Conf::CONF_OK) {
	debug("Topic Error configuring: %s", errstr.c_str());
  	std::string es;
	es = "Unable to specify topic configuration key '";
	es.append(key);
	es.append("': ");
	es.append(errstr);
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
	Local<Value> err = Exception::Error(String::New(es.c_str()));
  	return scope.Close( err );
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
	Local<Value> err = Exception::TypeError(String::NewFromUtf8(isolate,es.c_str()));
	args.GetReturnValue().Set(err);
#endif
  }


#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  return scope.Close(Undefined());
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  args.GetReturnValue().Set(Undefined(isolate));
#endif

}

extern "C" void init(Handle<Object> target) {
#if NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 10
  HandleScope scope;
#elif NODE_MAJOR_VERSION == 0 && NODE_MINOR_VERSION == 12
  Isolate* isolate = Isolate::GetCurrent();
  HandleScope scope(isolate);
#endif
  
  // Set up a message delivery report callback.
  // It will be called once for each message, either on succesful delivery to broker,
  // or upon failure to deliver to broker.

  
  NODE_SET_METHOD(target, "setDebug", setDebug);  
  NODE_SET_METHOD(target, "initialize", initialize);
  NODE_SET_METHOD(target, "configure", configure);
  NODE_SET_METHOD(target, "topic_configure", topic_configure);
  NODE_SET_METHOD(target, "connect", connect);
  NODE_SET_METHOD(target, "disconnect", disconnect);
  NODE_SET_METHOD(target, "produce", produce);
  NODE_SET_METHOD(target, "consume", consume);
  NODE_SET_METHOD(target, "pause", pause);
  NODE_SET_METHOD(target, "resume", resume);
  NODE_SET_METHOD(target, "stop", stop);
}

NODE_MODULE(rdkafkaBinding, init);
