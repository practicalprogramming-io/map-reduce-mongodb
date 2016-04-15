var async = require('async')
  , config = require('./config')
  , http = require('http')
  , Parser = require('jsonparse')
  , Stream = require('stream').Stream
  , through = require('through')
  , mongodb = require('./mongodb')
;


function buildHarvestURL (state) {

  var baseURL = 'http://waterservices.usgs.gov/nwis/iv/?format=json'
    , stateCd = '&stateCd=' + state
    , parameters = '&parameterCd=00065,00060'
  ;

  return baseURL + stateCd + parameters;

}


function parseStream () {
  // https://github.com/dominictarr/JSONStream

  var parser
    , stream
    , i
    , data
  ;

  parser = new Parser()

  stream = through(function(chunk) {
    if('string' === typeof chunk)
      chunk = new Buffer(chunk)
    parser.write(chunk)
  },
  function (data) {
    if (data)
      stream.write(data)
    stream.queue(null)
  });

  parser.onValue = function (value) {
    if (!this.root)
      stream.root = value

    if (stream.root && typeof stream.root === 'object') {
      if (stream.root.hasOwnProperty('timeSeries')) {
        data = stream.root.timeSeries
        for (i = 0; i < data.length; i++) {
          if (data[i] != null)
            stream.queue(data[i])
        }
      }
    }

  }

  parser._onToken = parser.onToken;

  parser.onToken = function (token, value) {
    parser._onToken(token, value);
  }

  parser.onError = function (error) {
    stream.emit('error', error)
  }

  return stream

}


function streamMapper (callback) {

  var stream = new Stream()

  stream.writable = true

  stream.write = (data) => {
    return callback.call(null, data)
  }

  stream.end = (data) => {
    stream.writable = false
    stream.emit('end')
    stream.destroy()
  }

  stream.destroy = () => {
    stream.emit('close')
  }

  return stream

}


function requestUSGS (url, callback) {

  var request;

  request = http.get(url, function (response) {

    var parse, mapper;

    parse = parseStream()
    mapper = streamMapper(function (data) {
      mongodb.createRecords('harvest', data, function (error) {
        if (error) callback(error)
      })
    })

    response.pipe(parse).pipe(mapper);

    response.on('error', function (error) {
      callback(error);
    });

    response.on('end', function () {
      callback(null, 'end');
    });

  });

  request.shouldKeepAlive = false;

}

function harvest (done) {

  var states = config.states;

  function series (state) {
    if (state) {
      url = buildHarvestURL(state);
      console.log('Currently scraping: ', url);
      requestUSGS(url, function (error, data) {
        if (error) callback(error);
        return series(states.shift());
      });
    }
    else {
      async.waterfall([
        function (callback) {
          mongodb.reduceRecords('harvest', function (err, data) {
            if (err) callback(err);
            else callback(null, data);
          })
        },
        function (data, callback) {
          mongodb.createRecords('geojson', data, function (err) {
            if (err) callback(err);
            callback(null);
          })
        },
        function (callback) {
          mongodb.reduceMerge('geojson', function (err) {
            if (err) callback(err);
            else callback(null);
          })
        },
        function (callback) {
          mongodb.singleGeoJsonDoc('join', function (err, res) {
            if (err) callback(err);
            callback(null, res);
          })
        },
        function (data, callback) {
          mongodb.createRecords('record', data, function (err) {
            if (err) callback(err);
            callback(null);
          })
        }
      ], function (error) {
        if (error) done(error);
        else done(null);
      })
    }
  }

  series(states.shift());

}


harvest(function (error, done) {
  if (error) console.log(error)
  process.exit(0)
});
