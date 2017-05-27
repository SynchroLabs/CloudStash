var request = require('supertest');
var assert = require('assert');
var jwt = require('jsonwebtoken');

var mantaBoxServer = require('./../lib/server');

var _testSecret = "test";
var server = mantaBoxServer(_testSecret);

var testToken = jwt.sign({ username: "test", userid: 1234 }, _testSecret);

describe('Array', function() {
  describe('#indexOf()', function() {
    it('should return -1 when the value is not present', function() {
      assert.equal(-1, [1,2,3].indexOf(4));
    });
  });
});

describe('GET /user', function() {
  it('respond with json', function(done) {
    request(server)
      .get('/echo/bar')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .expect('Content-Type', /json/)
      .expect('"You said: bar"')
      .expect(200, done);
  });
});