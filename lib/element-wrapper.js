'use strict';

var _ = require('lodash');
var Q = require('q');

var assert = require('assert'); // TODO: remove

var ElementWrapper = module.exports = function (gremlin, el) {
  this.gremlin = gremlin;
  this.el = el;
};

ElementWrapper.prototype.unwrap = function () {
  return this.el;
};

// each database seems to want to return a different data type for
// the id (Java object, long, string, etc.). in TinkerGraph and Titan
// all of the possible returned types serialize to a string, and the
// Graph object's getVertex and getEdge work correctly with these
// serialized strings. for this reason, we're standardizing on getId
// always returning a string (at least currently)
ElementWrapper.prototype.getId = function () {
  // TODO: rename this to just id() to match Tinkerpop3 naming.
  var id = this.el.idSync();
  return id;

// TODO: Can we get away with passing around an semi-opaque id Object?
// We may need helpers elsewhere to convert a java Object into a
// javascript (json) object, and vice versa. For now, wait and see.

//   if (_.isString(id)) {
//     return id;
//   } else if (id.longValue) {
//     console.log('id.longValue:', typeof id.longValue, id.longValue);
//     return id.longValue;
//   }
//
//   return id.toStringSync();
};

ElementWrapper.prototype.value = function (key, callback) {
  return Q.nbind(this.el.value, this.el)(key)
    .catch(function (err) {
      if (err.toString().match(/propertyDoesNotExist/))
        return undefined;
      else
        throw err;
    })
    .nodeify(callback);
};

ElementWrapper.prototype.valueSync = function (key) {
  return this.el.valueSync(key);
};

ElementWrapper.prototype.values = function (props, callback) {
  var self = this;
  var res = {};
  var propPromises = props.map(
    function (key) {
      return self.value(key)
        .then(function (value) { res[key] = value; });
    }
  );

  // Q.all() can be dangerous for operations that modify the database,
  // but should be fine here since this is read-only.
  return Q.all(propPromises)
    .then(function () { return new Q(res); })
    .nodeify(callback);
};

ElementWrapper.prototype.setProperties = function (props, callback) {
  var self = this;

  function setProps(keys) {
    if (keys.length === 0) {
      return new Q();
    }
    var key = keys.pop();
    return self.setProperty(key, props[key])
      .then(function () { return setProps(keys); });
  }

  return setProps(Object.keys(props)).nodeify(callback);
};

ElementWrapper.prototype.removeProperty = function (key, callback) {
  return Q.nbind(this.el.property, this.el)(key)
    .then(function (prop) { return Q.nbind(prop.remove, prop)(); })
    .nodeify(callback);
};

ElementWrapper.prototype.removeProperties = function (props, callback) {
  var self = this;

  function removeProps(keys) {
    if (keys.length === 0) {
      return new Q();
    }
    var key = keys.pop();
    return self.removeProperty(key)
      .then(function () { return removeProps(keys); });
  }

  return removeProps(props.slice()).nodeify(callback);
};

ElementWrapper.prototype.remove = function (callback) {
  return Q.nbind(this.el.remove, this.el)().nodeify(callback);
};

ElementWrapper.prototype.toJSON = function (callback) {
  return Q.nbind(this.gremlin.toJSON, this.gremlin)(this.el).nodeify(callback);
};

ElementWrapper.prototype.toJSONSync = function (callback) {
  return this.gremlin.toJSONSync(this.el);
};

ElementWrapper.prototype.toString = function (callback) {
  return Q.nbind(this.el.toString, this.el)().nodeify(callback);
};

ElementWrapper.prototype.toStringSync = function () {
  return this.el.toStringSync();
};
