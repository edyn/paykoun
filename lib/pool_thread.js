
var assert = require('assert');

var workersFuncs = {};

function registerWorker(name, workFunc){
  assert.ok(name, "Unable to register a worker without a name");
  assert.ok(workersFuncs[name], "You are trying to register a worker with the same name");

  workersFuncs[name] = workFunc;

}