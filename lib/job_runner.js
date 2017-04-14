
var util = require('util');
var _ = require('lodash');
var assert = require('assert');
var vasync = require('vasync');
var EventEmitter = require('events').EventEmitter;
var fs = require('fs');
var ConsoleLogger = require('./logger');

var logger = new ConsoleLogger('Paykoun/JobRunner');

function createRunner(opts, done){
  assert.ok(_.has(opts, 'type'), "'type' option is required");

  if (opts.type == 'vasync') {
    return new VAsyncRunner(opts, done);
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
    var boundToContext = _.bind(worker.workFunc, worker.runContext());

    boundToContext(job, function(err, result){

      done(err, result);

    }.bind(this));
  } catch(ex){
    logger.error("An error occured while running the job: " + ex);

    done(ex);
  }

};

VAsyncRunner.prototype.destroy = function(){
  this.queue.kill();
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

module.exports = {
  create: createRunner
}
