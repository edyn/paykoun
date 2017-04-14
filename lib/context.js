
'use strict';

var assert = require('assert');
var _ = require('lodash');
var vasync = require('vasync');
var Cubicle = require('./cubicle');

var ConsoleLogger = require('./logger');
var JobRunner = require('./job_runner');

var StatsD = require('node-statsd');

var logger = new ConsoleLogger('Paykoun/Context');

var HELPERS_PREFIX = '$_';

var PaykounContext = function(workQueueMgr){
  this.allWorkers = {};
  this.allWorkersFactories = {};
  this.jobRunners = {};
  this.workQueues = {};
  this.workQueueManager = workQueueMgr;
  this.workersByName = {};
  this.runnerByWorkerName = {};
  this.cubicleByWorkerName = {};
  this.isRunning = false;
};

PaykounContext.prototype.useStatsD = function(params){
  var options = params || {};
  this.statsdClient = new StatsD(options || {});
};

PaykounContext.prototype.useLogger = function(newLogger){
  logger = newLogger;
};

PaykounContext.prototype.createRunnerFactory = function(runOpts, done){
  return JobRunner.create(runOpts, done);
};

PaykounContext.prototype.registerWorker = function(workerFact) {
  assert.ok(_.isFunction(workerFact.instantiate), 'instantiate should be a function');
  assert.ok(workerFact.name, 'Can\'t register a worker without a name');

  this.allWorkersFactories[workerFact.name] = workerFact;

  workerFact.extend({
    $getHelper: this.getHelpers().$getHelper
  });

  var worker = workerFact.instantiate();
  
  this.allWorkers[workerFact.name] = worker;
};

PaykounContext.prototype.useHelper = function(name, helper) {
  this.initHelpersContext();

  assert.ok(name, 'you need to provide a name for this helper');
  assert.ok(helper, 'you need to provide a helper');
  assert.ok(_.isFunction(helper) || _.isObject(helper), 'Helper needs to be an object or a function');

  if (this.helpers[name]) {
    logger.error('Trying to register the same helper on the same context twice.');

    throw new Error('You can only register a helper once');
  }

  this.helpers[HELPERS_PREFIX + name] = helper;
};

PaykounContext.prototype.initHelpersContext = function() {
  if (this.helpers) {
    return;
  }

  this.helpers = {};
  this.helpers.$getHelper = _.bind(function $getHelper(name){
    var key = HELPERS_PREFIX + name;

    var helper = this.helpers[key];

    if (!helper) {
      throw new Error('Trying to use an unregistered helper function');
    }

    return helper;
  }, this);
};

PaykounContext.prototype.run = function(done) {
  // body...
  var self = this;

  this.isRunning = true;

  var queues = [];

  _.each(self.allWorkers, function(worker){
    // For each worker we need to create a work queue
    var workQueue = self.workQueueManager.createQueue(worker.name);
    worker.setWorkQueue(workQueue);

    var isDuplicateWorker = self.workersByName[worker.name];
    assert.ok(!isDuplicateWorker, 'Duplicate worker name : ' + worker.name);

    self.workersByName[worker.name] = worker;

    queues.push(workQueue);

    // Which pools will we need
    var runnerSpec = {
      name: worker.isolationGroup(),
      concurrency: worker.concurrency()
    };

    if (!runnerSpec) {
      runnerSpec = {
        name: 'Default',
        concurrency: 10
      };
    }

    var pool = self.jobRunners[runnerSpec.name];
    if (pool) {
      pool.workers.push(worker);
    } else {
      pool = {
        workers: [worker],
        name: runnerSpec.name,
        concurrency: runnerSpec.concurrency
      };

      self.jobRunners[runnerSpec.name] = pool;
    }

  });

  this.workQueueManager.on('ready', function(){

    var arg = {
      queues: queues,
      jobRunners: _.values(self.jobRunners)
    };

    vasync.pipeline({
      'funcs': [
        this.connectWorkQueues.bind(this),
        this.createJobRunners.bind(this),
        this.startCubicles.bind(this)
      ],
      'arg': arg
    }, function(vasynErr){
      if (vasynErr) {

        // This will allow to retry running a context
        self.isRunning = false;

        // Connecting some work queues failed, let's destroy the ones that succeeded
        _.each(self.workQueues, function(queue){
          queue.stop();
        });

        self.workQueues = null;

        done(vasynErr, null);

        return;
      }

      done(vasynErr, null);
    });

  }.bind(this));


  this.workQueueManager.on('error', function(err){
    logger.error('An error occurred on the WorkQueueMgr', err);

    return done(err, null);
  });

  this.workQueueManager.connect();
};

PaykounContext.prototype.getHelpers = function() {
  this.initHelpersContext();

  return this.helpers;
};

PaykounContext.prototype.destroy = function() {
  _.each(this.cubicleByWorkerName, function(cubicle){
    cubicle.destroy();
  });
};

PaykounContext.prototype.startCubicles = function(params, done) {
  _.each(this.workersByName, function(worker, name){
    var threadPool = this.runnerByWorkerName[name];

    assert.ok(threadPool, 'A worker needs to have a pool to run in');

    var cubicle = new Cubicle(worker, threadPool);

    cubicle.useLogger(logger);
    this.cubicleByWorkerName[name] = cubicle;

    if (this.statsdClient) {
      cubicle.setStatsD(this.statsdClient);
    }

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
    'inputs': params.queues
  }, function (err) {
    callback(err, null);
  });
};

PaykounContext.prototype.connectWorkQueue = function (workQueue, callback){
  var self = this;

  var wasReady = false;

  function onError(err){
    if (wasReady) {
      logger.error('Received an error after being ready, throwing an exception');

      throw new Error('WorkQueue in inconsistent state');
    }

    callback(err, null);

    return;
  }

  function onReady(){
    self.workQueues[workQueue.name] = workQueue;

    callback(null, null);

    wasReady = true;
  }

  workQueue.once('ready', onReady);

  workQueue.once('error', onError);

  workQueue.start();
};

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
        }.bind(this));
      }

      done(err, jobRunner);
    }.bind(self));
  });

  // initialize each work queues
  vasync.forEachParallel({
    'func': runnerFactory.bind(this),
    'inputs': params.jobRunners
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
  });
};

module.exports = PaykounContext;
