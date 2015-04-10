
var assert = require('assert');
var _ = require('lodash');
var vasync = require('vasync');
var assert = require('assert');
var fs = require('fs');
var Threads= require('webworker-threads');
var browserify = require('browserify');
var Cubicle = require('./cubicle');
var Readable = require('stream').Readable
var streamBuffers = require("stream-buffers");

var ConsoleLogger = require('./logger');

var logger = new ConsoleLogger('Paykoun/Context');


var PaykounContext = function(workQueueMgr){
  this._allWorkers = {};
  this.threadPools = {};
  this.workQueues = {};
  this._workQueueMgr = workQueueMgr;
  this.workersByName = {};
  this.poolByWorkerName = {};
  this.cubicleByWorkerName = {};
};

PaykounContext.prototype.registerWorker = function(worker) {
  assert.ok(worker.name, "Can't register a worker without a name");

  this._allWorkers[worker.name] = worker;
};


PaykounContext.prototype.onNewJob = function(job, done) {

};

PaykounContext.prototype.run = function(done) {
  // body...
  var self = this;
  
  var queues = [];
  var pools = {};

  _.each(self._allWorkers, function(worker, key){
    // For each worker we need to create a work queue
    var workQueue = self._workQueueMgr.createQueue(worker.name);
    worker.setWorkQueue(workQueue);

    var isDuplicateWorker = self.workersByName[worker.name];
    assert.ok(!isDuplicateWorker, "Duplicate worker name : " + worker.name);

    self.workersByName[worker.name] = worker;

    queues.push(workQueue);

    // Which pools will we need
    var poolSpec = worker.threadPool();
    if (!poolSpec) {
      poolSpec = {
        name: "Default",
        poolSize: 10
      };
    }

    var pool = self.threadPools[poolSpec.name]
    if (pool) {
      pool.workers.push(worker);
    } else {
      pool = {
        workers: [worker],
        name: poolSpec.name,
        poolSize: poolSpec.poolSize
      }

      self.threadPools[poolSpec.name] = pool;
    }

  }.bind(this));

  this._workQueueMgr.on('ready', function(err){
    
    var arg = {
      queues: queues,
      threadPools: _.values(self.threadPools)
    }

    vasync.pipeline({
      'funcs': [
        this.connectWorkQueues.bind(this),
        this.createThreadPools.bind(this),
        this.startCubicles.bind(this)
      ],
      'arg': arg
    }, function(err, res){
      if (err) {
        // Connecting some work queues failed, let's destroy the ones that succeeded

        _.each(self.workQueues, function(queue){
          queue.destroy();
        });

        self.workQueues = null;

        callback(err, null);

        return;
      };
      done(err, null);
    });

  }.bind(this));


  this._workQueueMgr.on('error', function(err){
    logger.error('An error occurred on the WorkQueueMgr', err);
  });

  this._workQueueMgr.connect();
}

PaykounContext.prototype.startCubicles = function() {
  _.each(this.workersByName, function(worker, name){
    var threadPool = this.poolByWorkerName[name];
    
    assert.ok(threadPool, "A worker needs to have a pool to run in");

    var cubicle = new Cubicle(worker, threadPool);
    this.cubicleByWorkerName[name] = cubicle;

    // This binds the workQueue to the worker
    cubicle.start();

  }.bind(this));
};

PaykounContext.prototype.connectWorkQueues = function(params, callback) {
  var self = this;

  // initialize each work queues
  vasync.forEachParallel({
    'func': self.connectWorkQueue.bind(this),
    'inputs': params.queues,
  }, function (err, results) {
    callback(err, null);
  })
};

PaykounContext.prototype.connectWorkQueue = function (workQueue, callback){
  var self = this;

  workQueue.once('ready', function(){
    self.workQueues[workQueue.name] = workQueue;

    callback();
  });

  workQueue.once('error', function(err){
    callback(err);
  });

  workQueue.start();
}

PaykounContext.prototype.createThreadPools = function(params, callback) {
  var self = this;
  
  logger.info(params.threadPools);

  // initialize each work queues
  vasync.forEachParallel({
    'func': this.createThreadPool.bind(this),
    'inputs': params.threadPools,
  }, function (err, results) {
    if (err) {
      var errorMsg = 'Failed to initilize ' + results.nerrors + ' thread pools :';
  
      _.each(results.operations, function(op){
        if (op.err) {
          errorMsg += '\n   - Failed to initilize pool with error : ' + op.err;
        }
      });

      logger.error(errorMsg);

      // Destroy all thread pools, even those that succeeded
      logger.error('Destroy all thread pools, even those that succeeded');

      _.each(results.successes, function(threadPool){
        threadPool.destroy(true);
      });

      callback(err, null);

      return;
    }

    callback(null, results.successes);
  })
};

PaykounContext.prototype.createThreadPool = function(pool, callback) {
  var self = this;

  var threadPool = Threads.createPool(pool.poolSize);

  var poolName = pool.name;

  var workFunctions = {}

  _.each(pool.workers, function(worker){
    self.poolByWorkerName[worker.name] = threadPool;
  });

  var toEval = createBootstapFunc(pool.workers);

  var writable = new streamBuffers.WritableStreamBuffer({
      initialSize: (100 * 1024),      // start as 100 kilobytes. 
      incrementAmount: (50 * 1024)    // grow by 50 kilobytes each time buffer overflows. 
  });

  var readable = new Readable();
  readable.push(toEval);
  readable.push(null);

  var brows = browserify(readable);
  brows.require(__dirname + '/pool_thread.js', {expose: 'paykoun'});


  brows.bundle().on('end', function(){
    var bundle = writable.getContentsAsString("utf8");
    
    //fs.writeFileSync("/Users/diallo/Downloads/bundle.js", bundle);

    var errors = [];
    var threads = [];

    threadPool.all.eval(bundle, function(err, returnVal){
      threads.push(this);

      if (err) {
        errors.push(err);

        logger.error("Error creating threadpool : " + err);
      }

      if (threads.length >= pool.poolSize) {
        process.nextTick(function(){

          if (errors.length) {
            logger.error("Errors during thread pool "+ poolName + " initialization : ", errors);

            logger.warn("Destroy the thread pool ungracefully");

            threadPool.destroy(true);
            threadPool = null;

            callback(new Error('We failed to initialize '+errors.length+' threads for thread pool '+ poolName));

            return;
          }

          callback(null, threadPool);
        });
      }
    });
  }).pipe(writable);

  
};


function createBootstapFunc(poolWorkers){
  var template = fs.readFileSync(__dirname + '/workers.tpl');

  var compiledTpl = _.template(template);

  var workers = {};
  _.each(poolWorkers, function(worker){
    workers[worker.name] = {
      name: worker.name,
      workFuncStr: worker.workFunc().toString()
    };
  });

  var toEval = compiledTpl({
    'workers': workers
  });

  return toEval;
}


module.exports = PaykounContext;