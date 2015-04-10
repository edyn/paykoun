
var paykoun = require('paykoun');

function bootstrap(){
  var ret = {
    <% var keys = Object.keys(workers); %>
    <% for(var i = 0; i < keys.length; i++){ %>
      <% var worker = workers[keys[i]]; %>
      '<%= keys[i] %>': <%= worker.workFuncStr %>,
    <%} %>
  };

  

  return ret;
}

var workers = bootstrap();

var keys = Object.keys(workers);

keys.forEach(function(name){
  paykoun.registerWorker(name, workers[name]);
});