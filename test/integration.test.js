'use strict';

const _ = require('lodash');
const { assert } = require('chai');
const co = require('co');
const dookie = require('../');
const fs = require('fs');
const mongodb = require('mongodb');
const stream = require('stream');
const yaml = require('js-yaml');

describe('dookie:push', function() {
  it('inserts documents', function(done) {
    co(function*() {
      const uri = 'mongodb://localhost:27017/test';
      yield dookie.push(uri, { 'sample': [{ x: 1 }] }, { dropDatabase: true });

      const db = yield mongodb.MongoClient.connect(uri);
      const docs = yield db.collection('sample').find({}).toArray();
      assert.equal(docs.length, 1);
      assert.equal(docs[0].x, 1);

      done();
    }).catch((error) => done(error));
  });

  it('$extend syntax', function(done) {
    co(function*() {
      const uri = 'mongodb://localhost:27017/test';

      const toInsert = {
        $test: { a: 1, b: 2 },
        sample: [
          { $extend: '$test', x: 1, b: 3 }
        ]
      };

      yield dookie.push(uri, toInsert, { dropDatabase: true });

      const db = yield mongodb.MongoClient.connect(uri);
      const docs = yield db.collection('sample').find({}).toArray();
      assert.equal(docs.length, 1);
      assert.equal(docs[0].x, 1);
      assert.equal(docs[0].a, 1);
      assert.equal(docs[0].b, 3);

      done();
    }).catch((error) => done(error));
  });

  it('recursive $extend syntax', function(done) {
    co(function*() {
      const uri = 'mongodb://localhost:27017/test';

      const toInsert = {
        $base: { c: 1 },
        $test: { a: 1, b: { $extend: '$base' } },
        sample: [
          { x: { $extend: '$test' } }
        ]
      };

      yield dookie.push(uri, toInsert, { dropDatabase: true });

      const db = yield mongodb.MongoClient.connect(uri);
      const docs = yield db.collection('sample').find({}).toArray();

      assert.equal(docs.length, 1);
      assert.deepEqual(docs[0].x, { a: 1, b: { c: 1 } });

      done();
    }).catch((error) => done(error));
  });

  it('$require syntax', function(done) {
    co(function*() {
      const uri = 'mongodb://localhost:27017/test';

      const path = './example/$require/parent.yml';
      const toInsert = yaml.safeLoad(fs.readFileSync(path));

      yield dookie.push(uri, toInsert, path, { dropDatabase: true });

      const db = yield mongodb.MongoClient.connect(uri);
      const people = yield db.collection('people').find({}).toArray();
      assert.equal(people.length, 1);
      assert.equal(people[0]._id, 'Axl Rose');

      const bands = yield db.collection('bands').find({}).toArray();
      assert.equal(bands.length, 1);
      assert.equal(bands[0]._id, `Guns N' Roses`);

      done();
    }).catch((error) => done(error));
  });

  it('$eval syntax', function(done) {
    co(function*() {
      const uri = 'mongodb://localhost:27017/test';

      const toInsert = {
        sample: [
          { a: 1, b: { $eval: 'this.a;' } }
        ]
      };

      yield dookie.push(uri, toInsert, { dropDatabase: true });

      const db = yield mongodb.MongoClient.connect(uri);
      const docs = yield db.collection('sample').find({}).toArray();

      assert.equal(docs.length, 1);
      assert.deepEqual(_.omit(docs[0], '_id'), { a: 1, b: 1 });

      done();
    }).catch((error) => done(error));
  });

  it('dropDatabase option', function(done) {
    co(function*() {
      const uri = 'mongodb://localhost:27017/test';

      const toInsert = {
        sample: [
          { a: 1 }
        ]
      };

      yield dookie.push(uri, _.cloneDeep(toInsert), { dropDatabase: true });
      yield dookie.push(uri, _.cloneDeep(toInsert));

      const db = yield mongodb.MongoClient.connect(uri);
      const docs = yield db.collection('sample').find({}).toArray();
      assert.equal(docs.length, 2);
      done();
    }).catch(done);
  });
});

describe('dookie:pull', function() {
  it('exports documents', function(done) {
    co(function*() {
      const uri = 'mongodb://localhost:27017/test2';

      const db = yield mongodb.MongoClient.connect(uri);
      yield db.dropDatabase();
      yield db.collection('sample').insert({ x: 1 });

      const results = yield dookie.pull(uri);

      assert.equal(Object.keys(results).length, 1);
      assert.equal(results.sample.length, 1);
      assert.equal(results.sample[0].x, 1);
      assert.ok(results.sample[0]._id.$oid);

      done();
    }).catch((error) => done(error));
  });

  describe('dookie:pull:collection', function() {
    it('exports documents from a single collection', function(done) {
      co(function*() {
        const uri = 'mongodb://localhost:27017/test3';

        const db = yield mongodb.MongoClient.connect(uri);
        yield db.dropDatabase();
        yield db.collection('sample').insert({ x: 1 });
        yield db.collection('sample2').insert({ y: 1 });

        const results = yield dookie.pull(uri, {collection:"sample2"});

        //assert.equal(Object.keys(results).length, 1);
        assert.notExists(results.sample);
        assert.equal(results.sample2.length, 1);
        assert.equal(results.sample2[0].y, 1);
        assert.ok(results.sample2[0]._id.$oid);

        done();
      }).catch((error) => done(error));
    });
    it('exports documents from a single collection with a query', function(done) {
      co(function*() {
        const uri = 'mongodb://localhost:27017/test3';

        const db = yield mongodb.MongoClient.connect(uri);
        yield db.dropDatabase();
        yield db.collection('sample').insert({ x: 1 });
        yield db.collection('sample').insert({ y: 5 });
        yield db.collection('sample2').insert({ x: 1 });
        yield db.collection('sample2').insert({ z: 1 });
        yield db.collection('sample2').insert({ y: 1 });
        yield db.collection('sample2').insert({ y: 2 });
        yield db.collection('sample2').insert({ y: 3 });
        yield db.collection('sample2').insert({ y: 4 });
        yield db.collection('sample2').insert({ y: 5 });

        const results = yield dookie.pull(uri, {collection: "sample2", query: { y: { $gte: 3 } } });

        //assert.equal(Object.keys(results).length, 1);
        assert.notExists(results.sample);
        assert.equal(results.sample2.length, 3);
        assert.equal(results.sample2[0].y, 3);
        assert.equal(results.sample2[1].y, 4);
        assert.equal(results.sample2[2].y, 5);
        assert.ok(results.sample2[0]._id.$oid);

        done();
      }).catch((error) => done(error));
    });
  });
});

describe('dookie:pullToStream', function() {
  it('writes JSON to stream', function(done) {
    co(function*() {
      const uri = 'mongodb://localhost:27017/test3';

      const db = yield mongodb.MongoClient.connect(uri);
      yield db.dropDatabase();
      yield db.collection('sample').insert({ x: 1 });
      yield db.collection('sample2').insert({ x: 2 });

      const ws = new stream.Writable();
      let str = '';
      ws._write = (chunk, enc, next) => {
        str += chunk.toString('utf8');
        next();
      };
      const results = yield dookie.pullToStream(uri, ws);

      assert.ok(str);
      const streamed = JSON.parse(str);
      assert.deepEqual(Object.keys(streamed), ['sample', 'sample2']);
      assert.equal(streamed['sample'].length, 1);
      assert.deepEqual(_.omit(streamed['sample'][0], '_id'), { x: 1 });

      done();
    }).catch((error) => done(error));
  });it('streams documents with a query', function(done) {
    co(function*() {
      const uri = 'mongodb://localhost:27017/test3';

      const db = yield mongodb.MongoClient.connect(uri);
      yield db.dropDatabase();
      yield db.collection('sample').insert({ x: 1 });
      yield db.collection('sample').insert({ y: 5 });
      yield db.collection('sample2').insert({ x: 1 });
      yield db.collection('sample2').insert({ z: 1 });
      yield db.collection('sample2').insert({ y: 1 });
      yield db.collection('sample2').insert({ y: 2 });
      yield db.collection('sample2').insert({ y: 3 });
      yield db.collection('sample2').insert({ y: 4 });
      yield db.collection('sample2').insert({ y: 5 });

      const ws = new stream.Writable();
      let str = '';
      ws._write = (chunk, enc, next) => {
        str += chunk.toString('utf8');
        next();
      };

      const results = yield dookie.pullToStream(uri, ws, { query: { y: { $gte: 3 } } });

      assert.ok(str);
      const streamed = JSON.parse(str);
      //assert.equal(Object.keys(results).length, 1);

      assert.deepEqual(Object.keys(streamed), ['sample', 'sample2']);
      assert.equal(streamed['sample'].length, 1);
      assert.deepEqual(_.omit(streamed['sample'][0], '_id'), { y: 5 });
      assert.equal(streamed['sample2'].length, 3);
      assert.deepEqual(_.omit(streamed['sample2'][0], '_id'), { y: 3 });
      assert.deepEqual(_.omit(streamed['sample2'][1], '_id'), { y: 4 });
      assert.deepEqual(_.omit(streamed['sample2'][2], '_id'), { y: 5 });

      done();
    }).catch((error) => done(error));
  });
  describe('dookie:pullToStream:collection', function() {
    it('streams documents from a single collection', function(done) {
      co(function*() {
        const uri = 'mongodb://localhost:27017/test3';

        const db = yield mongodb.MongoClient.connect(uri);
        yield db.dropDatabase();
        yield db.collection('sample').insert({ x: 1 });
        yield db.collection('sample2').insert({ y: 1 });
        const ws = new stream.Writable();
        let str = '';
        ws._write = (chunk, enc, next) => {
          str += chunk.toString('utf8');
          next();
        };

        const results = yield dookie.pullToStream(uri, ws, {collection:"sample2"});

        assert.ok(str);
        const streamed = JSON.parse(str);
        //assert.equal(Object.keys(results).length, 1);
        assert.deepEqual(Object.keys(streamed), ['sample2']);
        assert.equal(streamed['sample2'].length, 1);
        assert.deepEqual(_.omit(streamed['sample2'][0], '_id'), { y: 1 });

        done();
      }).catch((error) => done(error));
    });
    it('streams documents from a single collection with a query', function(done) {
      co(function*() {
        const uri = 'mongodb://localhost:27017/test3';

        const db = yield mongodb.MongoClient.connect(uri);
        yield db.dropDatabase();
        yield db.collection('sample').insert({ x: 1 });
        yield db.collection('sample').insert({ y: 5 });
        yield db.collection('sample2').insert({ x: 1 });
        yield db.collection('sample2').insert({ z: 1 });
        yield db.collection('sample2').insert({ y: 1 });
        yield db.collection('sample2').insert({ y: 2 });
        yield db.collection('sample2').insert({ y: 3 });
        yield db.collection('sample2').insert({ y: 4 });
        yield db.collection('sample2').insert({ y: 5 });

        const ws = new stream.Writable();
        let str = '';
        ws._write = (chunk, enc, next) => {
          str += chunk.toString('utf8');
          next();
        };

        const results = yield dookie.pullToStream(uri, ws, {collection:"sample2", query: { y: { $gte: 3 } } });

        assert.ok(str);
        const streamed = JSON.parse(str);
        //assert.equal(Object.keys(results).length, 1);

        assert.deepEqual(Object.keys(streamed), ['sample2']);
        assert.equal(streamed['sample2'].length, 3);
        assert.deepEqual(_.omit(streamed['sample2'][0], '_id'), { y: 3 });
        assert.deepEqual(_.omit(streamed['sample2'][1], '_id'), { y: 4 });
        assert.deepEqual(_.omit(streamed['sample2'][2], '_id'), { y: 5 });

        done();
      }).catch((error) => done(error));
    });
  });
});
