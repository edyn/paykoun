'use strict';

var paykounPath = '../../lib/paykoun';


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
var MockMgr = require('./mock/ikue');
chai.use(sinonChai);

var Paykoun = rewire(paykounPath);


describe('PaykounWorker', function(){
  var queueMgr;

  beforeEach(function(){
    queueMgr = new MockMgr();
  })

  describe('createWorker:validateDefinition', function(){


    beforeEach(function(){
    });

    it('work function should be the only required field for a worker', function(){
      try{
        var worker = Paykoun.createWorker("Name", {});
      } catch(e){
      }

      assert.isUndefined(worker, "work function is required");

      worker = Paykoun.createWorker("Name", {
        work: function(job, done){
        }
      });

      assert.isObject(worker);
      assert.isFunction(worker.instantiate);

    });

    it('Should set default values for properties', function(){
      var WorkerDef = Paykoun.createWorker("Name", {
        work: function(job, done){
        }
      });

      assert.isObject(WorkerDef);
      assert.isFunction(WorkerDef.instantiate);

      var worker = WorkerDef.instantiate();

      assert.deepEqual(worker.getTriggers(), ["trigger_Name"], 'default trigger should be trigger_[WorkerName]');
      assert.equal(worker.isolationPolicy(), "vasync", "default isolationPolicy should be \'vasync\'");
      assert.equal(worker.concurrency(), 20, "default concurrency should be \'20\'");

      assert.equal(worker.isolationGroup(), "DefaultIsolationGroup", "default isolationGroup should be \'DefaultIsolationGroup\'");

      assert.equal(worker.name, "Name", "Name of the worker is correctly set");

    });

    it('Should set isolation policy to thread', function(){
      var WorkerDef = Paykoun.createWorker("Name", {
        work: function(job, done){
        },
        isolationPolicy: 'thread'
      });

      assert.isObject(WorkerDef);
      assert.isFunction(WorkerDef.instantiate);

      var worker = WorkerDef.instantiate();

      assert.deepEqual(worker.getTriggers(), ["trigger_Name"], 'default trigger should be trigger_[WorkerName]');
      assert.equal(worker.isolationPolicy(), "thread", "default isolationPolicy should be \'vasync\'");
      assert.equal(worker.concurrency(), 20, "default concurrency should be \'20\'");

      assert.equal(worker.isolationGroup(), "DefaultIsolationGroup", "default isolationGroup should be \'DefaultIsolationGroup\'");
      assert.equal(worker.name, "Name", "Name of the worker is correctly set");

      assert.deepEqual(worker.threadPool(), {name: "DefaultIsolationGroup", poolSize: 20});
    });

  });
});