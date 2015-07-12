


/**
* 
*/


var context = Paykoun.createContext(workQueueMgr);

var statd = context.statd();

statd.report('heartbeat');
statd.event('eventName');

context.statd()
  .host('')
  .port(14253)
  .namespace('paykoun1');

var workers = Paykoun.gatherWorkers('./workers');
// workers is an array like [{/* Whatever the structure is*/}]



context.addWorker(
  Paykoun.createWorker('NameWorker', {
    work: function(job, done){
      var vasync = require('vasync');

      // We could load the env inside the worker
      var hello = process.env.VARIABLE

      vasync.something();
      say(hello),
    }
  })
);

function addWorker(worker, ){

}

context.addWorkers(workers);

/**
* Group workers by isolation policy. We need to be able to override
*   - Run some on node's run loop
*   - Create thread pools when needed and load the code
* Register to receive jobs from the work queue
*   - whenever it receive a job to run, it need to track metrics like
*      + How long it took to complete
*      + How long IO took
*
*/
context.run();

