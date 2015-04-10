'use strict';

var paykounPath = '../../lib/paykoun';


var rewire = require('rewire'); // rewiring library

var PaykounContext = rewire( '../../lib/context');

var chai = require('chai'); // assertion library
var expect = chai.expect;
var should = chai.should;
var sinon = require('sinon');
var util = require('util');
var sinonChai = require('sinon-chai');
var MockMgr = require('./mock/ikue');
chai.use(sinonChai);

var Paykoun = rewire(paykounPath);

var WorkQueueMgr = require('ikue').WorkQueueMgr;

describe('Paykoun', function(){
  var queueMgr;

  beforeEach(function(){
    queueMgr = new MockMgr();

    Paykoun.__set__("PaykounContext", PaykounContext);
  })

  describe('PaykounContext', function(){


    beforeEach(function(){
    });

    it('Should create context correctly', function(){
      var context = Paykoun.createContext(queueMgr);
      expect(context.registerWorker).to.exist;
    });

    it('Running a context should create work queues', function(done){
      var context = Paykoun.createContext(queueMgr);
      expect(context.registerWorker).to.exist;

      var triggers = ['event5', 'event4', 'event3'];
      var setWorkQueueSpy = sinon.spy(function(queue){
        queue.triggers = triggers;
        this.workQueue = queue;
      });

      var fakeWorkFunc = function(job, done){
        var util = require('util');

        console.log(util.format("Hello %s", job.data.name));
      }

      context.registerWorker({
        name: "Worker1",
        setWorkQueue: setWorkQueueSpy,
        workFunc: function(){
          return fakeWorkFunc.toString();
        },
        threadPool: function(){
          return {
            name: "Worker1 Pool",
            poolSize: 1,
          };
        },
        getTriggers: function(){
          return ['event1'];
        }
      });

      /*context.registerWorker({
        name: "Worker2",
        setWorkQueue: setWorkQueueSpy,
        workFunc: function(){
          return fakeWorkFunc.toString();
        },
        threadPool: function(){
          return {
            name: "Worker2 Pool",
            poolSize: 1,
          }
        },
        getTriggers: function(){
          return ['event2'];
        }
      });*/

      context.run(function(err){
        if (err) {

        };
      });

      queueMgr.on('ready', function(){
        expect(queueMgr.queues.length).to.eql(1);
        
        var queue1 = queueMgr.queues[0];

        expect(queue1.name).to.eql("Worker1");
      });

    });

  
    it('If connecting a queue failed, we should destroy all queues and no thread should be created', function(done){

    });

    it('If creating a thread pool failed, we should destroy all queues and destroy other thread pools', function(done){

    });


    it('Later test', function(done){
      var context = Paykoun.createContext(queueMgr);
      expect(context.registerWorker).to.exist;

      var triggers = ['event5', 'event4', 'event3'];
      var setWorkQueueSpy = sinon.spy(function(queue){
        queue.triggers = triggers;
        this.workQueue = queue;
      });

      context.registerWorker({
        name: "Worker1",
        setWorkQueue: setWorkQueueSpy
      });

      context.registerWorker({
        name: "Worker2",
        setWorkQueue: setWorkQueueSpy
      });

      context.run();

      queueMgr.on('ready', function(){

        done();
      });

    });


  });
});