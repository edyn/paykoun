'use strict';

/* global setTimeout */

var assert = require('assert');
var _ = require('lodash');
var events = require('events');
var util = require('util');
var sinon = require('sinon');

var RegularContext = require('./context');

var Job = require('ikue').Job;

function TestJob(context, name, data){
  Job.call(this, name, data);
  this.context = context;
}

util.inherits(TestJob, Job);

TestJob.prototype.send = function(done){
  this.context.dispatchJob(this, done);
};

var PaykounContext = function(){
  this.workersByName = {};
  this.allWorkersSpecs = {};
  this.eventbus = new events.EventEmitter();
};

util.inherits(PaykounContext, events.EventEmitter);

PaykounContext.prototype.registerWorker = function(workerFact) {
  assert.ok(_.isFunction(workerFact.instantiate), 'instantiate should be a function');
  assert.ok(workerFact.name, 'Can\'t register a worker without a name');
  var isDuplicateWorker = this.workersByName[workerFact.name];
  assert.ok(!isDuplicateWorker, 'Duplicate worker name : ' + workerFact.name);

  this.allWorkersSpecs[workerFact.name] = workerFact;
  this.workersByName[workerFact.name] = workerFact.instantiate();
};

PaykounContext.prototype.run = function(done) {
  // body...
  var self = this;

  _.each(self.workersByName, function(worker){
    var triggers = worker.triggers();

    _.each(triggers, function(trigger){
      self.eventbus.on(trigger, function(job){
        var boundFunc = _.bind(self.runJobInContext, self, worker);

        boundFunc(job);
      });
    });

  });

  process.nextTick(function(){
    return done(null, null);
  });
};

PaykounContext.prototype.initRunContext = function(){
  RegularContext.prototype.initRunContext.call(this);
};

PaykounContext.prototype.getRunContext = function(){
  var here = RegularContext.prototype.getRunContext.call(this);
  return here;
};

PaykounContext.prototype.useHelper = function(name, helper){
  RegularContext.prototype.useHelper.call(this, name, helper);
};

PaykounContext.prototype.destroy = function() {
  this.eventbus.removeAllListeners();
  this.workersByName = null;
  this.allWorkersSpecs = null;
  this.eventbus = null;
};

PaykounContext.prototype.dispatchJob = function(job, done) {
  var self = this;
  process.nextTick(function(){
    if (_.isFunction(done)) {
      done(null, null);
    }

    _.extend(job.data, {id: job.id});

    process.nextTick(function(){
      self.eventbus.emit(job.type, job.data);
    });
  });
};

PaykounContext.prototype.runJobInContext = function(worker, job) {
  var self = this;

  var onDone = function onDone(err, result){
    self.emit('job_done', job, err, result);
  };

  var object = {
    done: onDone
  };

  var onDoneStub = sinon.stub(object, 'done', object.done);

  var runCtx = self.getRunContext();

  _.bind(worker.workFunc, runCtx)(job, onDoneStub);
};


function JobQueue(context){
  this.jobs = {};
  this.context = context;
  this.flushing = false;
}

PaykounContext.prototype.queue = function() {
  return new JobQueue(this);
};

JobQueue.prototype.pushJob = function(event, params) {
  if (this.flushing) {
    throw new Error('You cannot push a job when the JobQueue is beeing flushed');
  }

  var job = new TestJob(this.context, event, params);

  assert.ok(job.id, 'A job should have an id');

  this.jobs[job.id] = job;

  return job;
};

JobQueue.prototype.flush = function(done) {
  var self = this;

  this.flushing = true;

  _.each(this.jobs, function(job){
    job.send();
  });

  // Invoked when the event 'job_done' is emitted byt the context
  /* eslint-disable no-unused-vars */
  function onJobDone(job, err, result){
    delete self.jobs[job.id];
  }
  /* eslint-enable no-unused-vars*/

  this.context.on('job_done', onJobDone);

  // invoked when either there is a timeout or the flushing succeed
  /* eslint-disable no-unused-vars*/
  function onFlushingDone(err, res){
    self.context.removeListener('job_done', onJobDone);
    done(err, res);
  }
  /* eslint-enable no-unused-vars */

  var counter = 20;

  var INTERVAL = 2;

  function checkQueue() {
    counter--;
    if (self.jobs > 0 && counter > 0) {
      setTimeout(checkQueue, INTERVAL);

      return;
    } else if(counter === 0) {
      onFlushingDone(new Error('Flushing the queue timed out'));

      return;
    }

    onFlushingDone();
    return;
  }

  setTimeout(checkQueue, INTERVAL);
};

module.exports = PaykounContext;
