'use strict';

var paykounPath = '../../lib/';


var rewire = require('rewire'); // rewiring library

var chai = require('chai'); // assertion library
var expect = chai.expect;
var assert = chai.assert;
var should = chai.should;
var sinon = require('sinon');
var util = require('util');
var vasync = require('vasync');
var _ = require('lodash');
var sinonChai = require('sinon-chai');
var EventEmitter = require('events').EventEmitter;
var MockMgr = require('./mock/ikue');
chai.use(sinonChai);

var Paykoun = rewire(paykounPath + 'paykoun');
var JobRunner = rewire(paykounPath + 'job_runner');

function FakeBrowserify(inputStream){
  function Construct(){
    var self = this;

    EventEmitter.call(this);

    this.require = sinon.spy();

    this.bundle = sinon.spy(function(){
      setTimeout(function(){
        self.emit('end');
      }.bind(self), 5);

      return self;
    });

    this.pipe = sinon.spy();
  }

  util.inherits(Construct, EventEmitter);

  return new Construct();
}

function FakeThreadPool(){
  var fakePool = {
    size: 1,
    all: {
      error: null,
      failOnIndex: false,
      evalCallback: null,
      eval: sinon.spy(function(code, callback){
        fakePool.all.evalCallback = sinon.spy(callback);

        process.nextTick(function(){
          for (var i = 0; i < fakePool.size; i++) {
            if (fakePool.all.failOnIndex && fakePool.all.failOnIndex == i) {
              setTimeout(function(){
                fakePool.all.evalCallback(fakePool.all.error || new Error('FakePool eval error'));
              }, i*2);
            } else {
              setTimeout(function(){
                fakePool.all.evalCallback(null);
              }, i*2);
            }
          };
        });
      }),
    },
    any: {

    },
    destroy: sinon.spy()
  };

  fakePool.on = sinon.spy();

  return fakePool;
}

describe('JobRunner', function(){
  var queueMgr;

  beforeEach(function(){
    queueMgr = new MockMgr();
  }),

  describe('Basics', function(){
    it('Creating a JobRunner validate the properties', function(){
      assert.throws(JobRunner.create, /type/);
      
      assert.throws(_.bind(JobRunner.create, 
        this, 
        {type: 'thread'}),
        /concurrency/);

      assert.throws(_.bind(JobRunner.create, 
        this, 
        {type: 'thread', concurrency: 12}),
        /name/);

      assert.throws(_.bind(JobRunner.create, 
        this, 
        {type: 'thread', concurrency: 12, name: 'Name'}),
        /callback/);
    });

    it('Creating a JobRunner with invalid "type" fail', function(){
      var done = sinon.spy();

      assert.throws(_.bind(JobRunner.create, 
        this, 
        {type: 'unexisting', concurrency: 1, name: 'Name'}, done));

      assert.doesNotThrow(_.bind(JobRunner.create, 
        this, 
        {type: 'thread', concurrency: 1, name: 'Name'}, done));

      assert.doesNotThrow(_.bind(JobRunner.create, 
        this, 
        {type: 'vasync', concurrency: 1, name: 'Name'}, done));
    });
  })

  describe('ThreadRunner', function(){

    var FakeThreads
      , fakePool;
    beforeEach(function(){
      fakePool = FakeThreadPool();

      FakeThreads = {
        createPool: sinon.stub().returns(fakePool)
      };


      JobRunner.__set__('Threads', FakeThreads);
      JobRunner.__set__('fs', {
        readFileSync: sinon.stub().returns('var hello;')
      });

      JobRunner.__set__('browserify', FakeBrowserify);
    });

    it('Creating a JobRunner actually create threads and evaluate browserified code', function(done){
      var verify;

      var onDone = sinon.spy(function(err){
        verify();
        done(err);
      });

      var createFunc = _.bind(JobRunner.create, 
        this, {
          type: 'thread', 
          concurrency: 1, 
          name: 'Name'}, onDone);

      createFunc();

      function verify(){
        expect(onDone, "Done method should be called").to.have.been.calledOnce
        expect(FakeThreads.createPool, "FakeThreads.createPool").to.have.been.calledWith(1);

        expect(fakePool.all.eval, "browserified code was evaluated inside the thread").to.have.been.called;
      }
    });

    it('Creating a JobRunner actually evaluate the code for each thread', function(done){
      var verify;

      var onDone = sinon.spy(function(err){
        verify();
        done(err);
      });

      var createFunc = _.bind(JobRunner.create, 
        this, {
          type: 'thread', 
          concurrency: 4, 
          name: 'Name'}, onDone);

      fakePool.size = 4;

      createFunc();

      function verify(){
        expect(fakePool.all.evalCallback, "code should be evaluated in each thread").to.have.been.callCount(4);
      }
    });

    it('Creating a JobRunner fail if a thread fail to evaluate the code');
  });
});