'use strict';

var paykounPath = '../../lib/paykoun';

var chai = require('chai'); // assertion library
var expect = chai.expect;
var should = chai.should;
var assert = chai.assert;
var sinon = require('sinon');

var sinonChai = require('sinon-chai');
chai.use(sinonChai);
chai.use(should);

var Paykoun = require(paykounPath);

/* global beforeEach, describe */

describe('Paykoun Test Context', function(){

  var context = null;
  var queue = null;
  var workerOneSpy = null;
  var workerOneSpy2 = null;

  beforeEach(function(done){
    context = Paykoun.createTestContext();
    queue = context.queue();

    workerOneSpy = sinon.stub();
    workerOneSpy2 = sinon.stub();

    context.registerWorker(Paykoun.createWorker('Worker1', {
      triggers: ['event1'],
      work: workerOneSpy,
      timeout: 10000
    }));

    context.registerWorker(Paykoun.createWorker('Worker2', {
      triggers: ['event2'],
      work: workerOneSpy2,
      timeout: 10000
    }));

    context.run(done);
  });

  it('should dispatch job to the correct worker only', function(done){
    queue.pushJob('event2', {name: 'Diallo'});
    queue.flush(function(){
      assert(workerOneSpy2.calledOnce);
      done();
    });

  });

  it('should not dispatch job to the wrong worker', function(done){
    queue.pushJob('event2', {name: 'Diallo'});

    queue.flush(function(){
      workerOneSpy.called.should.equal(false);
      done();
    });

  });

  it('should create jobs and push them correctly', function(done){
    var job = queue.pushJob('event1', {name: 'Diallo'});
    queue.flush(function(){
      var call = workerOneSpy.getCall(0);

      expect(call.args[0]).to.have.property('id', job.id);
      expect(typeof call.args[1]).to.match(/function/);
      done();
    });
  });

  it('should allow us to assert on the outcome of the job execution', function(done){

    workerOneSpy.callsArgWith(1, 'hello', 'world');

    queue.pushJob('event1', {name: 'Diallo'});
    queue.flush(function(){
      var call = workerOneSpy.firstCall;
      var onDoneSpy = call.args[1];

      expect(typeof onDoneSpy).to.match(/function/);
      assert(onDoneSpy.called);
      expect(onDoneSpy).to.have.been.calledWith('hello', 'world');

      done();
    });
  });

  it('should dispatch jobs', function(done){
    queue.pushJob('event1', {name: 'Diallo'});
    queue.pushJob('event1', {name: 'Paul'});

    queue.flush(function(){
      assert(workerOneSpy.called);

      var firstCall = workerOneSpy.getCall(0);
      var secondCall = workerOneSpy.getCall(1);

      expect(firstCall.args[0]).to.have.property('name', 'Diallo');
      expect(secondCall.args[0]).to.have.property('name', 'Paul');

      done();
    });
  });
});
