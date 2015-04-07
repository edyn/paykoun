var EventEmitter= require('events').EventEmitter;
var util = require('util');
var sinon = require('sinon');
var _ = require('lodash');
var Job = require('ikue').Job;

function MockMgr(){
  this.failConnecting = false;
  this.queues = [];

}

util.inherits(MockMgr, EventEmitter);

MockMgr.prototype.createQueue = function(queueName) {
  var self = this;

  var queue = new EventEmitter();

  queue = _.extend(queue, {
    name: queueName,
    start: function(){
      process.nextTick(function(){
        queue.emit('ready');
      });
    },
    eventBus: new EventEmitter(),
    createJob: function(type, data){
      var job = new Job(type, data);
      job.workQueue(this);

      return job;
    },

    pushJob: function(job, done){
      setTimeout(function(){
        this.eventBus.emit(job.type, job.data);

        process.nextTick(function(){
          if (job.fail) {

            done(new Error("Job ("+ job.id + ") failed to run"));

            return;
          };

          done(null, job.result);
        })
      }.bind(self), 1);
    }
  });

  this.queues.push(queue);

  return queue;
};


MockMgr.prototype.shouldFailConnecting = function(fail) {
  this.failConnecting = true;

  if (typeof(fail) != 'undefined') {
    this.failConnecting = fail;
  }
};

MockMgr.prototype.connect = function() {
  setTimeout(function(){
    if (this.failConnecting) {
      this.emit('error', new Error("Unable to connect"));
    } else {
      this.emit('ready');
    }
  }.bind(this), 1);
};

module.exports = MockMgr;