
var Paykoun = require('./../lib/paykoun');
var WorkQueueMgr = require('ikue').WorkQueueMgr;
var PaykounContext = require( './../lib/context');

var queueMgr = new WorkQueueMgr({
  component: 'consumer', 
  amqp: {
    url: "amqp://guest:guest@localhost:5672/bench"
  },
  name: 'Benchmark'
});


var isProducer = process.env.PRODUCER || false;

if (!isProducer) {
  var context = Paykoun.createContext(queueMgr);

  var fakeWorkFunc = function(data, done){
    //var wait = Math.floor((Math.random() * 100) + 1);

    console.log("Hello 1");

    done(null, 'bobo'); 
    
    return;
  }

  var fakeWorkFunc2 = function(data, done){
    //var wait = Math.floor((Math.random() * 100) + 1);

    console.log("Hello 2 : "+ data.name);

    done(null, 'bobo'); 
    
    return;
  }

  /*context.registerWorker(Paykoun.createWorker("Worker", {
    isolationPolicy: 'vasync',
    concurrency: 1000,
    triggers: ['event1'],
    work: fakeWorkFunc,
    timeout: 2000,
  }));*/

  context.registerWorker(Paykoun.createWorker("Worker2", {
    isolationPolicy: 'thread',
    concurrency: 2,
    triggers: ['event1'],
    work: fakeWorkFunc2,
    timeout: 2000,
  }));

  context.run(function(err){
    console.log(arguments);
  });

} else {
  var workQueue = queueMgr.createQueue('Queue1');

  queueMgr.connect();
  queueMgr.on('ready', function(){
    workQueue.start();

    workQueue.triggers = ['event2'];

    workQueue.on('ready', function(){
      setInterval(function() {
        job = workQueue.createJob('event1', {name: "Diallo"});
        job.send();
      }, process.env.PRODUCER_INTERVAL || 10000);
    });
  });
}