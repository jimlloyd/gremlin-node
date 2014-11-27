'use strict';

var _ = require('lodash');
var Q = require('q');

function IteratorWrapper(it) {
  if (!(this instanceof IteratorWrapper)) {
    return new IteratorWrapper(it);
  }
  this.it = it;
}

IteratorWrapper.prototype.unwrap = function () {
  return this.it;
};

IteratorWrapper.prototype.nextSync = function () {
  return this.it.nextSync();
};

IteratorWrapper.prototype.next = function (callback) {
  return Q.nbind(this.it.next, this.it)()
    .nodeify(callback);
};

IteratorWrapper.prototype.hasNextSync = function () {
  return this.it.hasNextSync();
};

IteratorWrapper.prototype.hasNext = function (callback) {
  return Q.nbind(this.it.hasNext, this.it)()
    .nodeify(callback);
};

module.exports = IteratorWrapper;
