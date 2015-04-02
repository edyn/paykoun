
var PaykounContext = require('./context');

function PaykounWorker = function(name, def){
  this.name = name;

  this.validateDef(def);

  return {
    instantiate: function(){

      return {
        name: name,
        doWork: function(job, done){
          if (def.workSync) {
            var val = def.workSync(job);
            done();
          }
        }
      }
    }
  };
}

PaykounWorker.prototype.validateDef = function(def) {

};

PaykounWorker.prototype.execute = function(params) {

}

var Paykoun = {

  createContext: function(workQueueMgr){
    var context = new PaykounContext(workQueueMgr);

    return context;
  },

  createWorker: function(name, definition){
    var workerFactory = new PaykounWorker(name, definition);

    return workerFactory;
  }

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