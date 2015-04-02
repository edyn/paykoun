exports = Paykoun.createWorker('UniqueWorkerName', {

  /**
  * Declare a list of events that should trigger this worker
  * The specific event(s) will be available inside the 'job' object as : job.eventName
  * 
  * @see work
  * @see recurrence
  *<
  * @todo How are we going to deal with events-params discrepancy  
  */
  triggers: function(){ // This can also be an array or a string instead of a function
    return ['eventName1', 'eventName2'];
  },

  /*
  * This declare the recurence of the job if it is a recurrent job.
  * Should not be declared if the job should only run when triggered by an event.
  * @see 'triggers'
  */
  recurrence: {
    interval: 60*60*1000, // Recurrence in milliseconds or 'often'
    canMissRun: true // Is false by default ?
  },


  /*
  * This function is triggered whenever a new job has to be done
  * 
  * @job  An object containing the job to be done. Contains any metadata needed
  * @done a callback to be run when we are done doing the job
  *       This param is optional, if the function doesn't declare this param, it is assumed
  *       to be a syncronous function and will be run on the declared thread pool
  *
  * @return
  *       Can return nothing, a Job object or a string representing an event name
  *       You can use this feature to chain jobs.
  *
  */
  work: function(job, done){
    // Do what we are supposed to do
  },

  workSync: function(job){

  },

  /**
  * This indicate the context in which we want the worker to run.
  *
  * The goal of isolation is to prevent a specific worker to take up more resources than it should
  * We will be able to specify the resources we want a worker to take dynamically.
  * In this way, whenever a specific part of our system is buggy or failing we will be able to point to it.
  * We can also rest assured that a single component of our app will never take the whole service down.
  *
  * One of the important resource is CPU (that we can map to a timeout).
  */
  isolationPolicy: 'pool', // Can also be 'none' (and 'process' maybe ?)

  /**
  * Needed when using the 'pool isolation policy'. Will default to 'DefaultThreadPool' when not specified}
  */
  poolName: 'ComponentPoolName'
});