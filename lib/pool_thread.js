
var _ = require('lodash');
var assert = require('assert');

var workers = {};

var myInnerVar = "inital";
var count = 0;

function registerWorker(name, workFunc){

  assert.ok(_.isString(workFunc), "Unable to register a worker without a name");
  assert.ok(_.isFunction(workFunc), "You are trying to register a worker with the same name");

  workers[name] = workFunc;
}


thread.on('do_job', function(paramsAsJSON){
  var params = JSON.parse(paramsAsJSON);

  var workFunc = workers[params.workerName];


  assert.ok(workFunc, "Cannot find workFunc for " + params.workerName);

  try {

    workFunc(params, function(err, res){

      if (err) {
        thread.emit('job_failed', err.toString());
      } else {
        var ret = {
          result: res,
          id: params.id
        };

        thread.emit('job_done', JSON.stringify(ret));
      }
    });
  } catch(e){
    thread.emit('job_failed', e.toString());
  }

});


module.exports.registerWorker = registerWorker;