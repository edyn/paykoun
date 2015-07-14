'use strict';

var paykounPath = '../../lib/paykoun';

var chai = require('chai'); // assertion library
var expect = chai.expect;
var assert = chai.assert;
var sinon = require('sinon');

var sinonChai = require('sinon-chai');
chai.use(sinonChai);

var Paykoun = require(paykounPath);

/* global beforeEach, describe */
describe('Paykoun Test Context', function(){

  var context = null;
  var queue = null;
  var sayHelloWorker = null;
  var failingWorkerFunc = null;
  var sayHelloSpy = null;

  beforeEach(function(done){
    context = Paykoun.createTestContext();
    queue = context.queue();

    sayHelloSpy = sinon.spy();

    sayHelloWorker = sinon.spy(function(job, onJobDone){

      var $sayHello = this.$getHelper('sayHello');

      $sayHello(job.name);

      onJobDone(null, null);
    });

    context.useHelper('sayHello', sayHelloSpy);

    context.registerWorker(Paykoun.createWorker('SayHelloWorker', {
      triggers: ['sayHello'],
      work: sayHelloWorker
    }));

    failingWorkerFunc = sinon.spy(function(job, onJobDone){
      try {
        this.$getHelper('unexistingHelper');
      } catch (e){
        return onJobDone(e, null);
      }

      onJobDone(null, null);
    });

    context.registerWorker(Paykoun.createWorker('FailingWorker', {
      triggers: ['sayHello'],
      work: failingWorkerFunc
    }));

    context.run(done);
  });

  it('should allow us to use a registered helper', function(done){
    context.dontMock('sayHello');

    queue.pushJob('sayHello', {name: 'Diallo'});

    queue.flush(function(){
      sayHelloSpy.called.should.equal(true, 'The helper should have been called');

      var helperCall = sayHelloSpy.firstCall;
      expect(helperCall).to.not.have.thrown();
      expect(helperCall.args[0]).to.equal('Diallo');
      done();
    });

  });

  it('should throw an error when trying to use an unregistered helper', function(done){
    context.dontMock('unexistingHelper');
    
    queue.pushJob('sayHello', {name: 'Diallo'});

    queue.flush(function(){
      assert(failingWorkerFunc.called);

      var onDoneCall = failingWorkerFunc.firstCall.args[1];

      assert(onDoneCall.calledOnce);
      expect(onDoneCall.firstCall.args[0]).to.match(/Error: Trying to use an unregistered helper function/);
      done();
    });

  });
});
