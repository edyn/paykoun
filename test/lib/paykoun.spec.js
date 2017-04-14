'use strict';

var paykounPath = '../../lib/paykoun';


var rewire = require('rewire'); // rewiring library

var PaykounContext = rewire( '../../lib/context');

var chai = require('chai'); // assertion library
var expect = chai.expect;
var assert = chai.assert;
var should = chai.should;
var sinon = require('sinon');
var util = require('util');
var vasync = require('vasync');
var _ = require('lodash');
var sinonChai = require('sinon-chai');
var MockMgr = require('./mock/ikue');
chai.use(sinonChai);

var Paykoun = rewire(paykounPath);

var WorkQueueMgr = require('ikue').WorkQueueMgr;


function waitForQueues(arg, done){

  var queues = arg;

  if (!_.isArray(arg)) {
    queues = [arg];
  };

  vasync.forEachParallel({
    func: function(queue, callback){
      queue.on('ready', function(){
        callback();
      });
    },
    inputs: queues
  }, function(err, res){
    done(err, res);
  });
}

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
      }

      context.registerWorker(Paykoun.createWorker("Worker1", {
        isolationPolicy: 'thread',
        concurrency: 1,
        triggers: ['event1'],
        work: fakeWorkFunc
      }));

      context.registerWorker(Paykoun.createWorker("Worker2", {
        isolationPolicy: 'thread',
        concurrency: 1,
        triggers: ['event2'],
        work: fakeWorkFunc
      }));

      context.run(function(err){
        if (err) {

        };
      });

      queueMgr.on('ready', function(){
        expect(queueMgr.queues.length).to.eql(2);
        
        var queue1 = queueMgr.queues[0];

        expect(queue1.name).to.eql("Worker1");

        waitForQueues(queueMgr.queues, function(err, res){
          assert.ok(queueMgr.queues[0].eventBus);
          assert.ok(queueMgr.queues[1].eventBus);

          done(err);
        });
      });

    });
  
    it('If connecting a queue failed, we should destroy all queues and no thread should be created');

    it('If creating a thread pool failed, we should destroy all queues and destroy other thread pools');
  });
});
