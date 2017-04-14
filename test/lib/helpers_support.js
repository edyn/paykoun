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
  var funcSpy = null;

  beforeEach(function(done){
    context = Paykoun.createTestContext();
    queue = context.queue();

    sayHelloSpy = sinon.spy();
    funcSpy = sinon.spy();

    sayHelloWorker = sinon.spy(function(job, onJobDone){

      var $sayHello = this.$getHelper('sayHello');

      var $objectHelper = this.$getHelper('objectHelper');

      $objectHelper.func();

      $sayHello(job.name);

      onJobDone(null, null);
    });

    context.useHelper('sayHello', sayHelloSpy);
    context.useHelper('objectHelper', {
      func: funcSpy
    });

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
      triggers: ['failingTrigger'],
      work: failingWorkerFunc
    }));

    context.run(done);
  });

  it('should allow us to use a registered helper', function(done){
    context.dontMock('sayHello');

    queue.pushJob('sayHello', {name: 'Hello world'});

    queue.flush(function(){
      sayHelloSpy.called.should.equal(true, 'The helper should have been called');

      var helperCall = sayHelloSpy.firstCall;
      expect(helperCall).to.not.have.thrown();
      expect(helperCall.args[0]).to.equal('Hello world');
      done();
    });

  });

  // Maybe in the long run we want to stub everything? or provide for a way that avoid
  // setting non function helpers?
  it('should only stub function helpers', function(done){
    queue.pushJob('sayHello', {name: 'Hello world'});

    queue.flush(function(){
      sayHelloSpy.called.should.equal(true, 'The helper should have been called');

      var helperCall = sayHelloSpy.firstCall;
      expect(helperCall).to.not.have.thrown();
      expect(helperCall.args[0]).to.equal('Hello world');
      done();
    });

  });

  it('should throw an error when trying to use an unregistered helper', function(done){
    context.dontMock('unexistingHelper');

    queue.pushJob('failingTrigger', {name: 'Hello world'});

    queue.flush(function(){
      assert(failingWorkerFunc.called);

      var onDoneCall = failingWorkerFunc.firstCall.args[1];

      assert(onDoneCall.calledOnce);
      expect(onDoneCall.firstCall.args[0]).to.match(/Error: Trying to use an unregistered helper function/);
      done();
    });

  });
});
