
var assert = require('assert');
var _ = require('lodash');
var vasync = require('vasync');
var EventBus = require('ikue').EventBus;

var PaykounContext = function(workQueueMgr){
  this._allWorkers = {};
  this._workQueueMgr = workQueueMgr;
};

PaykounContext.prototype.registerWorker = function(worker) {
  assert.ok(worker.name, "Can't registrer a worker without a name");

  this._allWorkers[worker.name] = worker;
};


PaykounContext.prototype.run = function() {
  // body...
  var self = this;

  this._workQueueMgr.connect();
  this._workQueueMgr.on('ready', function(err){
    var queues = [];

    _.each(self._allWorkers, function(worker, key){
      // For each worker we need to create a work queue

      var eventbus = new EventBus("Event bus for " + worker.name);
      var workQueue = self._workQueueMgr.createQueue(worker.name, eventbus);
      worker.setWorkQueue(workQueue);

      queues.push(workQueue);

    }.bind(this));

    // initialize each work queues
    vasync.forEachParallel({
      'func': connectWorkQueue,
      'inputs': queues,
    }, function (err, results) {

    });


  });
};

function connectWorkQueue(workQueue, callback){
  
  workQueue.once('ready', function(){
    callback();
  });

  workQueue.once('error', function(err){
    callback(err);
  });

  workQueue.start();
}

module.exports = PaykounContext;