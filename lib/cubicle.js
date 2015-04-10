var _ = require('lodash');
var uuid = require('node-uuid');

var ConsoleLogger = require('./logger');

var logger = new ConsoleLogger('Paykoun/Cubicle');

function Cubicle(worker, threadPool){
  this.worker = worker;
  this.queue = worker.workQueue;
  this.pendingJobs = {};
  this.threadPool = threadPool;
}

Cubicle.prototype.start = function() {
  var self = this;

  var triggers = this.worker.getTriggers();

  _.each(triggers, function(trigger){

    // Register to receive the jobs
    var onNewJobFunc = _.bind(this.onNewJob, this);

    this.queue.eventBus.on(trigger, function(job, done){
      onNewJobFunc(job, done);
    });   
  }.bind(this));

  this.threadPool.on('job_done', function(jobJSON){

    var job = JSON.parse(jobJSON);
    var data = self.pendingJobs[job.id];

    self.pendingJobs[job.id] = null;

    if (data) {
      data.done(null, null);
    }

    return;
  });
  
  this.threadPool.on('job_failed', function(err){

    logger.trace('Job failed : ' + err);

    var data = this.pendingJobs[job.id];

    if (data) {
      clearTimeout(data.timeoutHandle);

      data.done(new Error('Job with id : ' + job.id + 'failed. Reason : ' + err));
    }

    this.pendingJobs[job.id] = null;

  }.bind(this));

};

Cubicle.prototype.pause = function() {
  this.paused = true;
};

Cubicle.prototype.resume = function() {
  this.paused = false;
};

Cubicle.prototype.onNewJob = function(job, done) {
  var self = this;

  job.id = uuid.v4();

  if (this.paused) {
    logger.trace("Pausing feature for Cubicle not yet implemented.");
    throw new Error("Pausing feature not supported on Cubicle");
  };

  job['workerName'] = this.worker.name;

  var handle = setTimeout(function(){

    if (this.pendingJobs[job.id]) {
      self.onJobTimeout(job);
    }

  }.bind(this), this.worker.timeout());

  this.pendingJobs[job.id] = {
    job:job,
    done: done,
    timeoutHandle: handle
  };

  this.threadPool.any.emit('do_job', JSON.stringify(job));

};

Cubicle.prototype.onJobTimeout = function(job) {
  logger.trace("Job Timeout");

  var data = this.pendingJobs[job.id];

  if (data) {
    data.done(new Error('Job with id : ' + job.id + 'timed out'));
  }

  this.pendingJobs[job.id] = null;
};

module.exports = Cubicle;