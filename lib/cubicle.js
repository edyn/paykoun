var _ = require('lodash');
var uuid = require('node-uuid');

var ConsoleLogger = require('./logger');

var logger = new ConsoleLogger('Paykoun/Cubicle');

function Cubicle(worker, jobRunner){
  this.worker = worker;
  this.queue = worker.workQueue;
  this.pendingJobs = {};
  this.jobRunner = jobRunner;
}

Cubicle.prototype.start = function() {
  var self = this;

  var triggers = this.worker.triggers();

  _.each(triggers, function(trigger){

    // Register to receive the jobs
    var onNewJobFunc = _.bind(this.onNewJob, this);

    this.queue.eventBus.on(trigger, function(job, done){
      logger.trace('On new Job : ' + job);

      onNewJobFunc(job, done);
    });   
  }.bind(this));

  this.jobRunner.on('job_done', function(jobJSON){
    
    logger.trace('Job done : ' + jobJSON);

    var job = null;

    if (_.has(jobJSON, 'id')) {
      job = jobJSON;
    } else if(_.isString(jobJSON)){
      var tmp = JSON.parse(jobJSON);
      if (_.has(tmp, 'id')) {

      } else {
        logger.error("There is problem here, the returned response from the job runner doesn't contains the job.id : " + jobJSON);
      }
    }

    if (!job) {
      return ;
    };

    var data = self.pendingJobs[job.id];

    self.pendingJobs[job.id] = null;

    if (data) {
      data.done(null, null);
    }

    return;
  });
  
  this.jobRunner.on('job_failed', function(err, job){

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
    logger.error("Pausing feature for Cubicle not yet implemented.");
    throw new Error("Pausing feature not supported on Cubicle yet");
  };

  job['workerName'] = this.worker.name;

  var timeoutHandle = setTimeout(function(){

    if (this.pendingJobs[job.id]) {
      self.onJobTimeout(job);
    }

  }.bind(this), this.worker.timeout());

  this.pendingJobs[job.id] = {
    job:job,
    done: done,
    timeoutHandle: timeoutHandle
  };

  this.jobRunner.runJob(job);

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