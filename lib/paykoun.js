
var PaykounContext = require('./context');
var ConsoleLogger = require('./logger');
var _ = require('lodash');
var assert = require('assert');

var logger = new ConsoleLogger('Paykoun/Paykoun');

var defaultPoolName = "Default";

var PaykounWorker = function(name, workerDef){
  this.name = name;

  this.validateDef(workerDef);

  var def = _.assign({
    isolationPolicy: 'vasync',
    concurrency: 20,
    isolationGroup: 'DefaultIsolationGroup',
    timeout: 10*1000 // 10 secs
  }, workerDef);

  var runContext = {};

  function getRunContext(){
    return runContext;
  }

  return {
    name: name,

    // this object is used as this when running the worker 'work' function.
    runContext: getRunContext,

    instantiate: function(){

      return {
        name: name,
        
        // this object is used as this when running the worker 'work' function.
        runContext: getRunContext,

        workQueue: null,

        setWorkQueue: function(queue){
          queue.triggers = this.triggers();
          this.workQueue = queue;
        },

        timeout: function(){
          return def.timeout;
        },

        triggers: function(){
          var triggers = ['trigger_'+name];
          
          if (_.isFunction(def.triggers)) {
            triggers = def.triggers();

            assert.ok(_.isArray(triggers), "Bad worker definition for worker "+name+", *triggers* is a function but not returning an array");
          } else if(_.isString(def.triggers)){
            triggers = [def.triggers];
          } else if(_.isArray(def.triggers)){
            triggers = def.triggers;
          }

          return triggers;
        },

        threadPool: function(){
          if (this.isolationPolicy() !== 'thread') {
            assert.ok(false, "Should never call worker.threadPool() on a worker whose isolation policy is not 'thread'");
            
            return;
          }

          return {
            name: this.isolationGroup(),
            poolSize: this.concurrency(),
          };
        },

        isolationPolicy: function(){
          assert.ok(_.isString(def.isolationPolicy));

          return def.isolationPolicy;
        },

        concurrency: function(){
          assert.equal(typeof (def.concurrency), 'number');
          assert.equal(Math.floor(def.concurrency), def.concurrency);

          return def.concurrency;
        },

        isolationGroup: function(){
          return def.isolationGroup + ':' + this.isolationPolicy();
        },

        workFunc: def.work
      }
    },

    extend: function(objDef){
      assert(objDef, 'You need to pass a non null object hash to extend');
      assert(_.isObject(objDef), 'You need to pass an object hash to extend');

      _.extend(runContext, objDef);

      var funcs = {};
      _.forOwn(objDef, function(value, key) {
        if (_.isFunction(value)) {
          var boundFunc = _.bind(value, runContext);
          
          funcs[key] = boundFunc;
        }
      });

      // Overwrite functions with functions bound to the runContext
      _.extend(runContext, funcs);
    }
  };
}

PaykounWorker.prototype.validateDef = function(def) {
  assert.equal(typeof def.work, 'function', 'work function is required')
};

PaykounWorker.prototype.execute = function(params) {

}

var Paykoun = {

  createContext: function(workQueueMgr, type){
    var context = new PaykounContext(workQueueMgr);

    return context;
  },

  createWorker: function(name, definition){
    var workerFactory = new PaykounWorker(name, definition);

    return workerFactory;
  },

  createTestContext: function(){
    var PaykounTestContext = require('./test_context');
    var context = new PaykounTestContext();

    return context;
  }
};


module.exports = Paykoun;