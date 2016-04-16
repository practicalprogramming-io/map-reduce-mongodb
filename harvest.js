'use strict'

const async = require('async')
const config = require('./config')
const http = require('http')
const Parser = require('jsonparse')
const Stream = require('stream').Stream
const through = require('through')
const mongodb = require('./mongodb')


class StreamUSGS {

  constructor () {

  }

  static buildHarvestURL (state) {
    const baseURL = 'http://waterservices.usgs.gov/nwis/iv/?format=json'
    const stateCd = '&stateCd=' + state
    const parameters = '&parameterCd=00065,00060'
    return baseURL + stateCd + parameters
  }

  parseStream () {
    const parser = new Parser()
    const stream = through(chunk => {
      if('string' === typeof chunk)
        chunk = new Buffer(chunk)
      parser.write(chunk)
    },
    data => {
      if (data)
        stream.write(data)
      stream.queue(null)
    })
    parser.onValue = function (value) {
      if (!this.root) stream.root = value
      if (stream.root && typeof stream.root === 'object') {
        if (stream.root.hasOwnProperty('timeSeries')) {
          let data = stream.root.timeSeries
          for (let i = 0; i < data.length; i++) {
            if (data[i] != null)
              stream.queue(data[i])
          }
        }
      }
    }
    parser._onToken = parser.onToken
    parser.onToken = (token, value) => {
      parser._onToken(token, value)
    }
    parser.onError = error => {
      stream.emit('error', error)
    }
    return stream
  }


  streamMapper (callback) {
    const stream = new Stream()
    stream.writable = true
    stream.write = data => {
      return callback.call(null, data)
    }
    stream.end = data => {
      stream.writable = false
      stream.emit('end')
      stream.destroy()
    }
    stream.destroy = () => {
      stream.emit('close')
    }
    return stream
  }

  requestUSGS (url, callback) {
    const parse = parseStream()
    const mapper = streamMapper(data => {
      mongodb.createRecords('harvest', data, error => {
        if (error) callback(error)
      })
    })
    const request = http.get(url, response => {
      response.pipe(parse).pipe(mapper)
      response.on('error', error => {
        callback(error)
      })
      response.on('end', () => {
        callback(null, 'end')
      })
    })
    request.shouldKeepAlive = false
  }

}

function harvest (done) {

  const states = config.states

  function series (state) {
    if (state) {
      const url = buildHarvestURL(state)
      console.log('Currently scraping: ', url)
      requestUSGS(url, (error, data) => {
        if (error) callback(error)
        return series(states.shift())
      })
    }
    else {
      async.waterfall([
        function (callback) {
          mongodb.reduceRecords('harvest', (error, data) => {
            if (error) callback(error)
            else callback(null, data)
          })
        },
        function (data, callback) {
          mongodb.createRecords('geojson', data, (error) => {
            if (error) callback(error)
            callback(null)
          })
        },
        function (callback) {
          mongodb.reduceMerge('geojson', (error) => {
            if (error) callback(error)
            else callback(null)
          })
        },
        function (callback) {
          mongodb.singleGeoJsonDoc('join', (error, data) => {
            if (error) callback(error)
            callback(null, data)
          })
        },
        function (data, callback) {
          mongodb.createRecords('record', data, (error) => {
            if (error) callback(error)
            callback(null)
          })
        }
      ], function (error) {
        if (error) done(error)
        else done(null)
      })
    }
  }

  series(states.shift())

}


harvest(function (error, done) {
  if (error) console.log(error)
  process.exit(0)
})
