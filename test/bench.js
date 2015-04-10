
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
    var wait = Math.floor((Math.random() * 100) + 1);

    console.log("Hello", new Date().toString());

    done(null, null); 
    
    return;
  }

  context.registerWorker({
    name: "Consumer",
    setWorkQueue: function(queue){
      queue.triggers = this.getTriggers();
      this.workQueue = queue;
    },
    workFunc: function(){
      return fakeWorkFunc.toString();
    },
    threadPool: function(){
      return {
        name: "Worker1",
        poolSize: 100,
      };
    },
    timeout: function(){
      return 10000;
    },
    getTriggers: function(){
      return ['event1'];
    }
  });

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
      while(true){
        job = workQueue.createJob('event1', {name: "Diallo"});
        job.send();
      }
    });
  });
}