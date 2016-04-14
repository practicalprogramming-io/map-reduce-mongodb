function geojsonMap () {
  var doc
    , attrs

  doc = {};
  doc.geometry = {};
  doc.properties = {};
  doc.type = 'Feature';

  doc.geometry.type = 'Point';
  doc.geometry.coordinates = [
    this.sourceInfo.geoLocation.geogLocation.longitude,
    this.sourceInfo.geoLocation.geogLocation.latitude];

  doc.properties.record = 'usgs-water';
  doc.properties.id = this.name;
  doc.properties.site = this.sourceInfo.siteName;
  doc.properties.srs = this.sourceInfo.geoLocation.geogLocation.srs;
  doc.properties.siteCode = this.sourceInfo.siteCode[0].value;

  attrs = {};
  if (this.values[0].value[0]) {
    attrs.variableCode = this.variable.variableCode[0].value || 'undefined';
    attrs.variableUnit = this.variable.unit.unitAbbreviation || 'undefined';
    attrs.noDataValue = this.variable.noDataValue.toString() || 'undefined';
    attrs.value = this.values[0].value[0].value || 'undefined';
    attrs.timeStamp = this.values[0].value[0].dateTime || 'undefined';
  }

  doc.properties.variables = [];
  doc.properties.variables.push(attrs);

  emit(this._id, doc);
}

function geojsonReduce (key, values) {
  return key;
}

function streamFlowMap () {
  if (this.value.properties.variables[0].variableCode === '00065') {
    emit(this.value.properties.siteCode, this)
  }
}

function gageHeightMap () {
  if (this.value.properties.variables[0].variableCode === '00060') {
    emit(this.value.properties.siteCode, this);
  }
}

function mergeReduce (key, values) {
  var result;

  result = {};
  result.geometry = {};
  result.properties = {};
  result.properties.streamFlow = {};
  result.properties.gageHeight = {};

  result.geometry.type = 'Point';

  values.forEach(function (value) {
    var stream
      , gage;

    value = value.value;
    if (value.geometry.coordinates) {
      result.geometry.coordinates = value.geometry.coordinates;
    }
    if (value.properties.record) {
      result.properties.record = value.properties.record;
    }
    if (value.properties.site) {
      result.properties.site = value.properties.site;
    }
    if (value.properties.srs) {
      result.properties.srs = value.properties.srs;
    }
    if (value.properties.siteCode) {
      result.properties.siteCode = value.properties.siteCode;
    }
    if (value.properties.variables[0].variableCode === '00065') {
      stream = value.properties.variables[0];
      result.properties.streamFlow = {
        variableCode: stream.variableCode,
        noDataValue: stream.noDataValue,
        value: stream.value,
        timeStamp: stream.timeStamp
      };
    }
    if (value.properties.variables[0].variableCode === '00060') {
      gage = value.properties.variables[0];
      result.properties.gageHeight = {
        variableCode: gage.variableCode,
        noDataValue: gage.noDataValue,
        value: gage.value,
        timeStamp: gage.timeStamp
      };
    }
  });

  return result;
}

exports.geojsonMap = geojsonMap;
exports.geojsonReduce = geojsonReduce;
exports.streamFlowMap = streamFlowMap;
exports.gageHeightMap = gageHeightMap;
exports.mergeReduce = mergeReduce;
