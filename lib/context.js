
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
var JobRunner = require('./job_runner');

var StatsD = require('node-statsd');

var logger = new ConsoleLogger('Paykoun/Context');


var PaykounContext = function(workQueueMgr){
  this._allWorkers = {};
  this._allWorkersFactories = {};
  this.jobRunners = {};
  this.workQueues = {};
  this._workQueueMgr = workQueueMgr;
  this.workersByName = {};
  this.runnerByWorkerName = {};
  this.cubicleByWorkerName = {};
  this.isRunning = false;
}

PaykounContext.prototype.useStatsD = function(params){
  var options = params || {};
  this.statsdClient = new StatsD(options || {});;
};

PaykounContext.prototype.useLogger = function(newLogger){
  logger = newLogger;
};

PaykounContext.prototype.createRunnerFactory = function(runOpts, done){
  return JobRunner.create(runOpts, done);
};

PaykounContext.prototype.registerWorker = function(workerFact) {
  assert.ok(_.isFunction(workerFact.instantiate), "instantiate should be a function");
  assert.ok(workerFact.name, "Can't register a worker without a name");

  this._allWorkersFactories[workerFact.name] = workerFact;
  this._allWorkers[workerFact.name] = workerFact.instantiate();
};

PaykounContext.prototype.run = function(done) {
  // body...
  var self = this;

  this.isRunning = true;
  
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
    var runnerSpec = {
      name: worker.isolationGroup(),
      concurrency: worker.concurrency()
    };

    if (!runnerSpec) {
      runnerSpec = {
        name: "Default",
        concurrency: 10
      };
    }

    var pool = self.jobRunners[runnerSpec.name]
    if (pool) {
      pool.workers.push(worker);
    } else {
      pool = {
        workers: [worker],
        name: runnerSpec.name,
        concurrency: runnerSpec.concurrency
      }

      self.jobRunners[runnerSpec.name] = pool;
    }

  }.bind(this));

  this._workQueueMgr.on('ready', function(err){
    
    var arg = {
      queues: queues,
      jobRunners: _.values(self.jobRunners)
    }

    vasync.pipeline({
      'funcs': [
        this.connectWorkQueues.bind(this),
        this.createJobRunners.bind(this),
        this.startCubicles.bind(this)
      ],
      'arg': arg
    }, function(err, res){
      if (err) {

        // This will allow to retry running a context
        self.isRunning = false;

        // Connecting some work queues failed, let's destroy the ones that succeeded
        _.each(self.workQueues, function(queue){
          queue.stop();
        });

        self.workQueues = null;

        done(err, null);

        return;
      };
      
      done(err, null);
    });

  }.bind(this));


  this._workQueueMgr.on('error', function(err){
    logger.error('An error occurred on the WorkQueueMgr', err);

    return done(err, null);
  });

  this._workQueueMgr.connect();
}

PaykounContext.prototype.destroy = function() {
  _.each(this.cubicleByWorkerName, function(cubicle){
    cubicle.destroy();
  }.bind(this));
};

PaykounContext.prototype.startCubicles = function(params, done) {
  _.each(this.workersByName, function(worker, name){
    var threadPool = this.runnerByWorkerName[name];
    
    assert.ok(threadPool, "A worker needs to have a pool to run in");

    var cubicle = new Cubicle(worker, threadPool);

    cubicle.useLogger(logger);
    
    this.cubicleByWorkerName[name] = cubicle;

    if (this.statsdClient) {
      cubicle.setStatsD(this.statsdClient);
    };

    // This binds the workQueue to the worker
    cubicle.start();

  }.bind(this));

  return done(null, null);
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

PaykounContext.prototype.createJobRunners = function(params, callback) {
  var self = this;
  
  logger.info(params.jobRunners);

  var runnerFactory = (this.type);

  runnerFactory = _.wrap(this.createRunnerFactory, function(wrapped, opts, done){

    var cloneOpts = _.clone(opts);

    var firstWorker = opts.workers[0];

    cloneOpts.type = firstWorker.isolationPolicy();

    wrapped(cloneOpts, function(err, jobRunner){
      
      if (!err) {
        var workers = opts.workers;
        _.each(workers, function(worker){
          this.runnerByWorkerName[worker.name] = jobRunner;
        }.bind(this))
      }

      done(err, jobRunner);
    }.bind(self));
  })

  // initialize each work queues
  vasync.forEachParallel({
    'func': runnerFactory.bind(this),
    'inputs': params.jobRunners,
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

module.exports = PaykounContext;