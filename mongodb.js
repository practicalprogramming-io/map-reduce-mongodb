'use strict'

const mongoose = require('mongoose')
const config = require('./config')
const mapreduce = require('./mapreduce')
const async = require('async')

const record = new mongoose.Schema({})
const harvest = new mongoose.Schema({})
const geojson = new mongoose.Schema({})
const join = new mongoose.Schema({})

const mongoURL = ['mongodb:/', config.mongodb.host,
  config.mongodb.dbname].join('/')

const mongoDB = mongoose.connect(mongoURL)

mongoDB.connection.on('error', error => {
  console.log('Error connecting to MongoDB', error)
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

function createRecords (collection, data, callback) {
  const dbModel = getCollection(collection)
  dbModel.collection.insert(data, (error, response) => {
    if (error) callback(error)
    else callback(null, response)
  })
}

function reduceRecords (collection, callback) {
  const dbModel = getCollection(collection)
  const o = {}

  dbModel.find({}, (error, response) => {
    if (error) callback(error)
    const docIds = []
    for (let i = 0; i < response.length; i++) {
      let doc = response[i]
      docIds.push(doc._id)
    }
    o.query = {_id: {$in: docIds}}
  })

  o.map = mapreduce.geojsonMap
  o.reduce = mapreduce.geojsonReduce

  dbModel.mapReduce(o, (error, response) => {
    if (error) callback(error)
    else callback(null, response)
  })
}

function reduceMerge (collection, callback) {
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
    stream.query = {_id: {$in: docIds}}
    gage.query = {_id: {$in: docIds}}
  })

  stream.map = mapreduce.streamFlowMap
  stream.reduce = mapreduce.mergeReduce
  stream.out = {reduce: 'joined'}

  gage.map = mapreduce.gageHeightMap
  gage.reduce = mapreduce.mergeReduce
  gage.out = {reduce: 'joined'}

  async.parallel({
    stream: callback => {
      dbModel.mapReduce(stream, function (error, response) {
        if (error) callback(error)
        else callback(null, 'passed')
      })
    },
    gage: callback => {
      dbModel.mapReduce(gage, function (error, response) {
        if (error) callback(error)
        else callback(null, 'passed')
      })
    }
  },
  (error, response) => {
    if (error) throw error
    else callback(null, response)
  })
}

function singleGeoJSONDoc (collection, callback) {
  const dbModel = getCollection(collection)
  const stream = dbModel.find().stream()
  const geojson = {"data": []}

  stream.on('data', data => {
    const doc = data._doc
    const properties = doc.value.properties
    const geometry = doc.value.geometry
    if (geometry && properties) {
      geojson.data.push({
        "type": "Feature",
        "properties": properties,
        "geometry": geometry
      })
    }
  })
  .on('error', error => {
    callback(error)
  })
  .on('close', () => {
    callback(null, geojson)
  })
}


exports.createRecords = createRecords
exports.reduceRecords = reduceRecords
exports.reduceMerge = reduceMerge
exports.singleGeoJSONDoc = singleGeoJSONDoc
