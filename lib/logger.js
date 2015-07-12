

var logLevels = ['info', 'warn', 'error', 'info', 'trace', 'debug'];

function ConsoleLogger(name){
  logLevels.forEach(function(level){
    this[level] = function(msg){
      if (!process.env.DEBUG || process.env.DEBUG === 'no') return;

      console.log(new Date()+ ' # ' + name +' '+ level + " : "+msg);
    }.bind(this);
  }.bind(this));
}

module.exports = ConsoleLogger