'use strict'

const mongoose = require('mongoose')
const config = require('./config')
const mapreduce = require('./mapreduce')
const async = require('async')


const mongoURL = ['mongodb:/', config.mongodb.host, config.mongodb.dbname].join('/')
const record = new mongoose.Schema({})
const harvest = new mongoose.Schema({})
const geojson = new mongoose.Schema({})
const join = new mongoose.Schema({})

const mongoDB = mongoose.connect(mongoURL)

mongoDB.connection.on('error', (error) => {
  console.log('Error connecting to MongoDB', err)
})

mongoDB.connection.on('open', () => {
  console.log('Connected to MongoDB')
})

function connectMongoCollection (collection, schema) {
  return mongoDB.model(collection, schema)
}

function getCollection (collection) {
  switch (collection) {
    case 'record':
      return connectMongoCollection('Record', record)
      break
    case 'harvest':
      return connectMongoCollection('Harvest', harvest)
      break
    case 'geojson':
      return connectMongoCollection('GeoJSON', geojson)
      break
    case 'join':
      return connectMongoCollection('Join', join)
      break
  }
}

function removeCollection (collection, callback) {
  const dbModel = getCollection(collection)
  dbModel.remove({}, (error) => {
    if (error) callback(error)
    else callback(null, 'Removed collection:', collection)
  })
}

function createRecords (collection, data, callback) {
  const dbModel = getCollection(collection)
  dbModel.collection.insert(data, (error, response) => {
    if (error) callback(error)
    else callback(null, response)
  })
}

function reduceRecords (collection, callback) {
  var idQuery
  const dbModel = getCollection(collection)
  const o = {}

  dbModel.find({}, (error, response) => {
    if (error) callback(error)
    const docIds = []
    for (let i = 0; i < response.length; i++) {
      let doc = response[i]
      docIds.push(doc._id)
    }
    idQuery = {_id: {$in: docIds}}
  })

  o.map = mapreduce.geojsonMap
  o.reduce = mapreduce.geojsonReduce
  o.query = idQuery

  dbModel.mapReduce(o, (error, response) => {
    if (error) callback(error)
    else callback(null, response)
  })
}

function reduceMerge (collection, callback) {
  var idQuery
  const dbModel = getCollection(collection)
  const stream = {}
  const gage = {}

  dbModel.find({}, (error, response) => {
    if (error) callback(error)
    const docIds = []
    for (let i = 0; i < response.length; i++) {
      let doc = response[i]
      docIds.push(doc._id)
    }
    idQuery = {_id: {$in: docIds}}
  })

  stream.map = mapreduce.streamFlowMap
  stream.reduce = mapreduce.mergeReduce
  stream.query = idQuery
  stream.out = {reduce: 'joined'}

  gage.map = mapreduce.gageHeightMap
  gage.reduce = mapreduce.mergeReduce
  gage.query = idQuery
  gage.out = {reduce: 'joined'}

  async.parallel({
    stream: function (callback) {
      dbModel.mapReduce(stream, function (err, res) {
        if (err) callback(null, new Error('failed'))
        else callback(null, 'passed')
      })
    },
    gage: function (callback) {
      dbModel.mapReduce(gage, function (err, res) {
        if (err) callback(null, new Error('failed'))
        else callback(null, 'passed')
      })
    }
  },
  function (err, res) {
    if (err) throw err
    else callback(null, res)
  })
}

function singleGeoJsonDoc (collection, callback) {
  var dbModel
    , stream
    , geoJson

  dbModel = getCollection(collection)
  stream = dbModel.find().stream()
  geoJson = {"data": []}

  stream.on('data', function (data) {
    var geometry
      , properties
      , doc

    doc = data._doc
    geometry = doc.value.geometry
    properties = doc.value.properties
    if (geometry && properties) {
      geoJson.data.push({
        "type": "Feature",
        "properties": properties,
        "geometry": geometry
      })
    }
  }).on('error', function (err) {
    callback(err)
  }).on('close', function () {
    callback(null, geoJson)
  })
}

function getAllDocs (collection, callback) {
  var dbModel = getCollection(collection)
  dbModel.find().lean().exec(function (err, data) {
    if (err) callback(err)
    else callback(null, data[0].data)
  })
}

function emptyCollection (collection, callback) {
  var dbModel = getCollection(collection)
  dbModel.remove({}, function (err) {
    if (err) callback(err)
    else callback(null)
  })
}

exports.removeCollection = removeCollection
exports.createRecords = createRecords
exports.reduceRecords = reduceRecords
exports.reduceMerge = reduceMerge
exports.singleGeoJsonDoc = singleGeoJsonDoc
exports.getAllDocs = getAllDocs
exports.emptyCollection = emptyCollection
