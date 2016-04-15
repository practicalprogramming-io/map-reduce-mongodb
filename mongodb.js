var mongoose = require('mongoose')
  , config = require('./config')
  , mapreduce = require('./mapreduce')
  , async = require('async')
  , mongoURL
  , mongoDB
  , records
  , harvests
  , geojson
  , joined


mongoURL = ['mongodb:/', config.mongodb.host, config.mongodb.dbname].join('/')

record = new mongoose.Schema({})
harvest = new mongoose.Schema({})
geojson = new mongoose.Schema({})
join = new mongoose.Schema({})

mongoDB = mongoose.connect(mongoURL)

mongoDB.connection.on('error', function (err) {
  console.log('Error connecting to MongoDB', err);
});

mongoDB.connection.on('open', function () {
  console.log('Connected to MongoDB');
});

function connectMongoCollection (collection, schema) {
  return mongoDB.model(collection, schema);
}

function getCollection (collection) {
  switch (collection) {
    case 'record':
      return connectMongoCollection('Record', record);
      break;
    case 'harvest':
      return connectMongoCollection('Harvest', harvest);
      break;
    case 'geojson':
      return connectMongoCollection('GeoJSON', geojson);
      break;
    case 'join':
      return connectMongoCollection('Join', join);
      break;
  }
}

function removeCollection (collection, callback) {
  var dbModel;
  dbModel = getCollection(collection);
  dbModel.remove({}, function (err) {
    if (err) callback(err);
    else callback(null, 'Removed collection:', collection);
  });
}

function createRecords (collection, data, callback) {
  var dbModel;

  dbModel = getCollection(collection);
  dbModel.collection.insert(data, function (err, res) {
    if (err) callback(err);
    else callback(null, res);
  });
}

function reduceRecords (collection, callback) {
  var dbModel
    , o
    , docIds
    , doc
    , i
    , idQuery
    ;

  dbModel = getCollection(collection);

  dbModel.find({}, function (err, res) {
    if (err) callback(err);
    docIds = [];
    for (i = 0; i < res.length; i++) {
      doc = res[i];
      docIds.push(doc._id);
    }
    idQuery = {_id: {$in: docIds}};
  });

  o = {};
  o.map = mapreduce.geojsonMap;
  o.reduce = mapreduce.geojsonReduce;
  o.query = idQuery;

  dbModel.mapReduce(o, function (err, res) {
    if (err) callback(err);
    else callback(null, res);
  })
}

function reduceMerge (collection, callback) {
  var dbModel
    , stream
    , gage
    , idQuery
    , docIds
    , doc
    , i

  dbModel = getCollection(collection);

  dbModel.find({}, function (err, res) {
    if (err) callback(err);
    docIds = [];
    for (i = 0; i < res.length; i++) {
      doc = res[i];
      docIds.push(doc._id);
    }
    idQuery = {_id: {$in: docIds}};
  });

  stream = {};
  stream.map = mapreduce.streamFlowMap;
  stream.reduce = mapreduce.mergeReduce;
  stream.query = idQuery;
  stream.out = {reduce: 'joined'};

  gage = {};
  gage.map = mapreduce.gageHeightMap;
  gage.reduce = mapreduce.mergeReduce;
  gage.query = idQuery;
  gage.out = {reduce: 'joined'};

  async.parallel({
    stream: function (callback) {
      dbModel.mapReduce(stream, function (err, res) {
        if (err) callback(null, new Error('failed'));
        else callback(null, 'passed');
      })
    },
    gage: function (callback) {
      dbModel.mapReduce(gage, function (err, res) {
        if (err) callback(null, new Error('failed'));
        else callback(null, 'passed');
      })
    }
  },
  function (err, res) {
    if (err) throw err;
    else callback(null, res);
  })
}

function singleGeoJsonDoc (collection, callback) {
  var dbModel
    , stream
    , geoJson

  dbModel = getCollection(collection);
  stream = dbModel.find().stream();
  geoJson = {"data": []};

  stream.on('data', function (data) {
    var geometry
      , properties
      , doc

    doc = data._doc;
    geometry = doc.value.geometry;
    properties = doc.value.properties;
    if (geometry && properties) {
      geoJson.data.push({
        "type": "Feature",
        "properties": properties,
        "geometry": geometry
      })
    }
  }).on('error', function (err) {
    callback(err);
  }).on('close', function () {
    callback(null, geoJson);
  })
}

function getAllDocs (collection, callback) {
  var dbModel = getCollection(collection);
  dbModel.find().lean().exec(function (err, data) {
    if (err) callback(err);
    else callback(null, data[0].data);
  })
}

function emptyCollection (collection, callback) {
  var dbModel = getCollection(collection);
  dbModel.remove({}, function (err) {
    if (err) callback(err);
    else callback(null)
  })
}

exports.removeCollection = removeCollection;
exports.createRecords = createRecords;
exports.reduceRecords = reduceRecords;
exports.reduceMerge = reduceMerge;
exports.singleGeoJsonDoc = singleGeoJsonDoc;
exports.getAllDocs = getAllDocs;
exports.emptyCollection = emptyCollection;
