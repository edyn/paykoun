var _ = require('lodash');
var uuid = require('uuid');

var ConsoleLogger = require('./logger');

function Cubicle(worker, jobRunner){
  this.worker = worker;
  this.queue = worker.workQueue;
  this.pendingJobs = {};
  this.jobRunner = jobRunner;
  this.statsd = null;
  this.intervalHandle = null;
  this.logger = new ConsoleLogger('Paykoun/Cubicle:' + jobRunner.name + '#' + worker.name);
}

Cubicle.prototype.useLogger = function(newLogger){
  this.logger = newLogger;

  this.jobRunner.useLogger(newLogger);
};

Cubicle.prototype.setStatsD = function(statsdClient) {
  this.statsd = statsdClient;
};

Cubicle.prototype._prefix = function(name) {
  return 'paykoun.' + this.worker.name + '.' + name;
};

Cubicle.prototype._prefix = function(name) {
  return 'paykoun.' + this.worker.name + '.' + name;
};

Cubicle.prototype._prefixContext = function(name) {
  return 'paykoun.' + this.worker.isolationGroup().replace(':', '_') + '.' + name;
};

Cubicle.prototype.start = function() {
  var self = this;

  var triggers = this.worker.triggers();

  _.each(triggers, function(trigger){

    // Register to receive the jobs
    var onNewJobFunc = _.bind(this.onNewJob, this);

    this.queue.eventBus.on(trigger, function(job, done){

      job.id = uuid.v4();

      self.logger.debug({jobId: job.id}, 'On new Job');

      onNewJobFunc(job, done);

      if (self.statsd) {
        var now = Math.round(new Date());

        if (job.ikue_headers && job.ikue_headers.sent_at) {
          self.statsd.timing(self._prefix('latency'), now - job.ikue_headers.sent_at);  
        };
        
        self.statsd.increment(self._prefix('new_job'));
        self.statsd.increment(self._prefixContext('new_job'));
        self.statsd.gauge(self._prefix('pending_jobs'), _.size(this.pendingJobs));
      };

    });   
  }.bind(this));

  this.jobRunner.on('job_done', function(jobJSON){

    if (self.statsd) {
      self.statsd.increment(self._prefix('job_done'));
      self.statsd.increment(self._prefixContext('job_done'));
      self.statsd.gauge(self._prefix('pending_jobs'), _.size(this.pendingJobs));
    }

    var job = null;

    if (_.has(jobJSON, 'id')) {
      job = jobJSON;
    } else if(_.isString(jobJSON)){
      var tmp = JSON.parse(jobJSON);
      if (_.has(tmp, 'id')) {
        job = tmp;
      } else {
        self.logger.error("There is problem here, the returned response from the job runner doesn't contains the job.id : " + jobJSON);
      }
    }

    self.logger.trace('Job done : ' + jobJSON);

    if (!job) {
      self.logger.error("There is problem here, the returned response from the job runner doesn't contains the job.id : " + jobJSON);

      return ;
    };

    var data = self.pendingJobs[job.id];

    if (self.statsd) {
      var hrTime = process.hrtime()
      var microsecs = hrTime[0] * 1000000 + hrTime[1] / 1000;

      if (data) {
        self.statsd.timing(self._prefix('job_duration'), microsecs - data.start_time);
      }
    }

    delete self.pendingJobs[job.id];

    if (data) {
      data.done(null, null);
    }

    return;
  });
  
  this.jobRunner.on('job_failed', function(err, job){

    if (self.statsd) {
      self.statsd.increment(self._prefix('job_failed'));
      self.statsd.increment(self._prefixContext('job_failed'));
      self.statsd.gauge(self._prefix('pending_jobs'), _.size(this.pendingJobs));
    }

    self.logger.trace('Job failed : ' + err);

    var data = this.pendingJobs[job.id];

    if (data) {
      clearTimeout(data.timeoutHandle);

      data.done(new Error('Job with id : ' + job.id + 'failed. Reason : ' + err));
    }

    delete this.pendingJobs[job.id];

  }.bind(this));


  this.intervalHandle = setInterval(function(){
    if (self.statsd) {
      self.statsd.gauge(self._prefix('pending_jobs'), _.size(this.pendingJobs));

      var stats = this.jobRunner.getStats();

      _.each(stats, function(value, key){
        self.statsd.gauge(self._prefix('queue_' + key), value);
      }.bind(this));
    }

  }.bind(this), 1000);

};

Cubicle.prototype.destroy = function() {
  this.destroyed = true;

  clearInterval(this.intervalHandle);
  this.jobRunner.removeAllListeners();
  this.jobRunner.destroy();
  this.queue.stop();

  var triggers = this.worker.triggers();

  _.each(triggers, function(trigger){
    this.queue.eventBus.removeAllListeners();
  }.bind(this));
};

Cubicle.prototype.onNewJob = function(job, done) {
  var self = this;

  if (this.destroyed) {
    self.logger.warn("Cubicle is paused.");

    return done(new Error("Work queue is not ready, it was already destroyed"), null);
  };

  job['workerName'] = this.worker.name;

  var timeoutHandle = setTimeout(function(){

    if (this.pendingJobs[job.id]) {
      self.onJobTimeout(job);
    }

  }.bind(this), this.worker.timeout());

  var hrTime = process.hrtime()
  var microsecs = hrTime[0] * 1000000 + hrTime[1] / 1000;

  this.pendingJobs[job.id] = {
    job:job,
    done: done,
    timeoutHandle: timeoutHandle,
    start_time: microsecs
  };

  this.jobRunner.runJob(job);

};

Cubicle.prototype.onJobTimeout = function(job) {
  this.logger.trace("Job Timeout");

  if (this.statsd) {
    this.statsd.increment(this._prefix('job_timed_out'));
    this.statsd.increment(this._prefixContext('job_timed_out'));
    this.statsd.gauge(this._prefix('pending_jobs'), _.size(this.pendingJobs));
  }

  var data = this.pendingJobs[job.id];

  if (data) {
    data.done(new Error('Job with id : ' + job.id + 'timed out'));
  }

  delete this.pendingJobs[job.id];
};

module.exports = Cubicle;