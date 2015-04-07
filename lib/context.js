
var assert = require('assert');
var _ = require('lodash');
var vasync = require('vasync');
var assert = require('assert');
var fs = require('fs');
var Threads= require('webworker-threads');

var PaykounContext = function(workQueueMgr){
  this._allWorkers = {};
  this.threadPool = null;
  this._workQueueMgr = workQueueMgr;
  this.workersByName = {};
};

PaykounContext.prototype.registerWorker = function(worker) {
  assert.ok(worker.name, "Can't register a worker without a name");

  this._allWorkers[worker.name] = worker;
};


PaykounContext.prototype.run = function() {
  // body...
  var self = this;
  
  this._workQueueMgr.on('ready', function(err){
    var queues = [];

    _.each(self._allWorkers, function(worker, key){
      // For each worker we need to create a work queue
      var workQueue = self._workQueueMgr.createQueue(worker.name);
      worker.setWorkQueue(workQueue);

      var isDuplicateWorker = self.workersByName[worker.name];
      assert.ok(!isDuplicateWorker, "Duplicate worker name : " + worker.name);

      self.workersByName[worker.name] = worker;

      queues.push(workQueue);

      vasync.waterfall([
        function(callback){
          setImmediate(function(){
            callback(null, queues);
          });
        },
        self.connectWorkQueues,
        self.createThreadPools
      ], function(err, res){

      });

    }.bind(this));

  }.bind(this));

  this._workQueueMgr.connect();
}

PaykounContext.prototype.createThreadPools = function(arg, callback) {
  this.threadPool= Threads.createPool(100);

  var workFunctions = {}
  this.threadPool.load(__dirname + '/pool_thread.js', function(){
    var toEval = createBootstapFunc(this.workersByName);

    this.threadPool.eval(toEval);
  }.bind(this));
};

PaykounContext.prototype.connectWorkQueues = function(queues, callback) {
  // initialize each work queues
  vasync.forEachParallel({
    'func': connectWorkQueue,
    'inputs': queues,
  }, function (err, results) {
    callback(err);
  })
};


function createBootstapFunc(workersByName){
  var template = fs.readFileSync(__dirname + '/workers.tpl');

  var compiledTpl = _.template(template);

  var workers = {};
  _.each(workersByName, function(worker, name){
    workers[name] = worker.workFunc().toString();
  });

  var toEval = compiledTpl({
    workers: workers
  });

  return toEval;
}

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