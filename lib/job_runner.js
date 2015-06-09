
var util = require('util');
var _ = require('lodash');
var assert = require('assert');
var vasync = require('vasync');
var EventEmitter = require('events').EventEmitter;
var browserify = require('browserify');
var Threads= require('webworker-threads');
var fs = require('fs');
var ConsoleLogger = require('./logger');
var Readable = require('stream').Readable
var streamBuffers = require("stream-buffers");

var logger = new ConsoleLogger('Paykoun/JobRunner');

function createRunner(opts, done){
  assert.ok(_.has(opts, 'type'), "'type' option is required");

  if (opts.type == 'vasync') {
    return new VAsyncRunner(opts, done);
  } else if(opts.type == 'thread'){
    return new ThreadRunner(opts, done);
  }

  throw new Error("The passed runner is not supported : " + opts.type);
}

function JobRunner(opts){
  assert.ok(_.has(opts, 'concurrency'), "'concurrency' option is required");
  assert.ok(_.has(opts, 'name'), "'name' option is required");
  assert.ok(_.isString(opts.type), "'type' option should be a string");
  assert.ok(_.isNumber(opts.concurrency), "'concurrency' option should be a number");
  assert.ok(_.isString(opts.name), "'name' option should be a string");

  this.type = opts.type;
  this.name = opts.name;
  this.concurrency = opts.concurrency;
}

JobRunner.prototype.runJob = function(job) {
  throw new Error('Job runner should override this');
};

JobRunner.prototype.useLogger = function(newLogger){
  logger = newLogger;
};

util.inherits(JobRunner, EventEmitter);



VAsyncRunner.prototype.getStats = function() {
  return {
    pendings: 0,
    queued: 0,
  };
};

function VAsyncRunner(){
  var args = [];
  Array.prototype.push.apply( args, arguments );

  var opts = args.shift();
  var onDone = args.shift();

  this.workers = {};

  _.each(opts.workers, function(worker){
    this.workers[worker.name] = worker;
  }.bind(this));

  JobRunner.call(this, opts);

  assert.ok(_.isFunction(onDone), 'You need to pass a callback when creating a JobRunner');

  process.nextTick(function(){
    //console.log(typeof this)
    this.queue = vasync.queue(this.dispatchJob.bind(this), opts.concurrency);

    onDone(null, this);
  }.bind(this));
}

util.inherits(VAsyncRunner, EventEmitter);

VAsyncRunner.prototype.dispatchJob = function(job, done) {

  var worker = this.workers[job.workerName];
  if (!worker) {
    logger.warn("Trying to run job, but such a worker doesnt exit", job);

    process.nextTick(function(){
      this.emit('job_failed', new Error("Trying to run job, but such a worker doesnt exit"));
    }.bind(this));

    return ;
  }

  try{
    worker.workFunc(job, function(err, result){
      
      done(err, result);

    }.bind(this));
  } catch(ex){
    logger.error("An error occured while running the job: " + ex);

    done(ex);
  }
  
};

VAsyncRunner.prototype.useLogger = function(newLogger){
  logger = newLogger;
};

VAsyncRunner.prototype.runJob = function(job) {

  this.queue.push(job, function(err, res){
    var result = {
      result: res,
      id: job.id
    };

    if (err) {
      this.emit('job_failed', err, result);

      return;
    };

    this.emit('job_done', result);
  }.bind(this));
};

VAsyncRunner.prototype.getStats = function() {
  return {
    pendings: this.queue.npending,
    queued: this.queue.queued.length,
  };
};

function ThreadRunner(){
  var args = [];
  Array.prototype.push.apply( args, arguments );

  var opts = args.shift();
  var onDone = args.shift();

  JobRunner.call(this, opts);

  assert.ok(_.isFunction(onDone), 'You need to pass a callback when creating a JobRunner');

  this.uncompleteJobsCallback = {};
  this.threadPool = null;
  this.queue = vasync.queue(this.dispatchJob.bind(this), opts.concurrency);

  wrappedDone = _.wrap(onDone, function(func, err, res){
    
    this.threadPool.on('job_failed', function(err, job){
    
      console.log("Job id: ", job.id);

      var done = this.uncompleteJobsCallback[job.id];
      if (done) {
        delete this.uncompleteJobsCallback[job.id];

        done(err, job);
      };
    }.bind(this));


    this.threadPool.on('job_done', function(jobJSON){
      var job = JSON.parse(jobJSON);

      var done = this.uncompleteJobsCallback[job.id];
      if (done) {
        delete this.uncompleteJobsCallback[job.id];

        done(null, null);
      };
    }.bind(this));

    func(err, res);

  }.bind(this));

  this.createThreadPool(opts, wrappedDone);
}

ThreadRunner.prototype.dispatchJob = function(job, done) {
  this.threadPool.any.emit('do_job', JSON.stringify(job));

  this.uncompleteJobsCallback[job.id] = done;
};

ThreadRunner.prototype.runJob = function(job) {
  this.queue.push(job);
};

ThreadRunner.prototype.on = function(event, listener) {
  this.threadPool.on(event, listener);
};

ThreadRunner.prototype.getStats = function() {
  return {
    pendings: this.queue.npending,
    queued: this.queue.queued.length,
  };
};

ThreadRunner.prototype.useLogger = function(newLogger){
  logger = newLogger;
};

ThreadRunner.prototype.createThreadPool = function(pool, callback) {
  var self = this;

  var threadPool = Threads.createPool(pool.concurrency);

  var poolName = pool.name;

  var workFunctions = {}

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

    //console.log(bundle);

    var errors = [];
    var threads = [];

    threadPool.all.eval(bundle, function(err, returnVal){
      threads.push(this);

      if (err) {
        errors.push(err);

        logger.error("Error creating threadpool : " + err);
      }

      if (threads.length >= pool.concurrency) {

        process.nextTick(function(){

          if (errors.length) {
            logger.error("Errors during thread pool "+ poolName + " initialization : ", errors);

            logger.warn("Destroy the thread pool ungracefully");

            threadPool.destroy(true);
            threadPool = null;

            callback(new Error('We failed to initialize '+errors.length+' threads for thread pool '+ poolName));

            return;
          }

          self.threadPool = threadPool;

          callback(null, self);
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
      workFuncStr: worker.workFunc.toString()
    };
  });

  var toEval = compiledTpl({
    'workers': workers
  });

  return toEval;
}


module.exports = {
  create: createRunner
}
