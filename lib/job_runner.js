
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

util.inherits(JobRunner, EventEmitter);


JobRunner.prototype.runJob = function(job) {
  throw new Error('Job runner should override this');
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

  worker.workFunc(job, function(err, result){
  
    done(err, result);
  }.bind(this));

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

function ThreadRunner(){
  var args = [];
  Array.prototype.push.apply( args, arguments );

  var opts = args.shift();
  var onDone = args.shift();

  JobRunner.call(this, opts);

  assert.ok(_.isFunction(onDone), 'You need to pass a callback when creating a JobRunner');

  this.threadpool = null;

  this.createThreadPool(opts, onDone);
}

ThreadRunner.prototype.runJob = function(job) {
  this.threadPool.any.emit('do_job', JSON.stringify(job));
};

ThreadRunner.prototype.on = function(event, listener) {
  this.threadPool.on(event, listener);
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
