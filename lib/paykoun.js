
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

  return {
    name: name,
    instantiate: function(){

      return {
        name: name,
        
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
    context.setType(_.isString(type) || 'thread');

    return context;
  },

  createWorker: function(name, definition){
    var workerFactory = new PaykounWorker(name, definition);

    return workerFactory;
  },

  /*
  * Using this function you can collect all workers classes that has been defined
  * in a spectific directory. Any call to this fonction is recursive.
  * 
  * It will ignore any non worker module.
  *
  * *Note* After a second thought, I don't really like the idea of this function, the reason
  *         being the only way to ensure a file is a worker is by running it. I am not confortable
          with the idea of running any file in a directory. It is not elegant and it is insecure too. 
          I prefere that we only use 'PaykounContext.addWorker'. There might be a way to register a worker easily
          Like we are currently doing using an index.js file
  */  
  gatherWorkers: function(dir){

  }
};


module.exports = Paykoun;