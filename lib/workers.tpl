
function bootstrap(){
  var ret = {
    <% var keys = Object.keys(workers); %>
    <% for(var i = 0; keys.length; i++){ %>
      <% var worker = workers[keys[i]]; %>
      '<%= worker.name %>': workers.workFuncStr,
    <%} %>
  };

  return ret;
}