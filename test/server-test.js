var request = require('supertest');
var assert = require('assert');
var jwt = require('jsonwebtoken');

var mantaBoxServer = require('./../lib/server');


var _testSecret = "test";
var _testConfig = require('./../lib/config').getConfig(null, 
{ 
    "driver": 
    { 
        "provider": "file", 
        "basePath": "_mantabox_store" 
    } 
});
var server = mantaBoxServer(_testSecret, _testConfig);

var testToken = jwt.sign({ username: "test", userid: 1234 }, _testSecret);

describe('POST /users/get_current_account', function() {
  it('returns account id', function(done) {
    request(server)
      .post('/users/get_current_account')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .expect('Content-Type', /json/)
      .expect(function(res){
           assert.equal(res.body.userid, '1234'); 
      })
      .expect(200, done);
  });
});

describe('POST /files/download', function() {
  it('returns file', function(done) {
    request(server)
      .post('/files/download')
      .set('Accept', 'application/octet-stream')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "foo.txt" }')
      .expect('Content-Type', 'application/octet-stream')
      .expect(function(res){
           assert.equal(res.body.toString(), 'Foo is the word'); 
      })
      .expect(200, done);
  });
});