var manager = new WorkQueueManager({component: 'consumer'});
var workerOne = manager.createQueue(['event1', 'event2'], eventBus);
var workerAllEvent = manager.createQueue(eventBus);

eventBus.on('event1', function(job, done){
  if (worker.paused) {
    job.requeue(done);
    return;
  };

  worker.run(job, done);
});