'use strict';

var paykounPath = '../../lib/paykoun';


var chai = require('chai'); // assertion library
var expect = chai.expect;
var should = chai.should;
var sinon = require('sinon');
var sinonChai = require("sinon-chai");
chai.use(sinonChai);

var Paykoun = require(paykounPath);

var WorkQueueMgr = require('ikue').WorkQueueMgr;

describe('Paykoun', function(){
  var queueMgr;

  beforeEach(function(){
    queueMgr = new WorkQueueMgr({
      component: "Comp1"
    });
  })

  describe('PaykounContext', function(){
    
    it('Should create context correctly', function(){
      var context = Paykoun.createContext(queueMgr);
      expect(context.registerWorker).to.exist;
    });

    it('Running a context should create work queues', function(){
      var context = Paykoun.createContext(queueMgr);
      expect(context.registerWorker).to.exist;

      var triggers = ['event5', 'event4', 'event3'];
      var setWorkQueueSpy = sinon.spy(function(queue){
        queue.triggers = triggers;
      });

      context.registerWorker({
        name: "Worker1",
        setWorkQueue: setWorkQueueSpy
      });

      context.run();
    });


  });
});