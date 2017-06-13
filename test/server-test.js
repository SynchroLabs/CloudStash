var request = require('supertest');
var assert = require('assert');
var jwt = require('jsonwebtoken');

var mantaBoxServer = require('./../lib/server');
var loggerModule = require('./../lib/logger');
loggerModule.createTestLogger();

var log = loggerModule.getLogger('Test');

var _testSecret = "test";

// var _testConfig = require('./../lib/config').getConfig('config_manta.json');
var _testConfig = require('./../lib/config').getConfig(null, 
{
    "driver":
    { 
        "provider": "file", 
        "basePath": "_mantabox_store" 
    } 
});

var server = mantaBoxServer(_testSecret, _testConfig);

var testAccount = 
{ 
  app_id:     "TEST01",
  account_id: "1234-BEEF"
};

var testToken = jwt.sign(testAccount, _testSecret);

// !!! Test on Manta
//
// !!! Test delete of folder and contents - not implemented yet (?)
//
// !!! Test copy/move of folders (and their contents) - not implemented yet
//
// !!! Test list_folder of non-existent folder
//
// !!! Test download of non-existent file (I think it times out)
//
// !!! Test upload/download of binary files to make sure we don't have any encoding weirdness
//

// Tests below assume starting with a 1234-BEEF/TEST01 that is empty (and if successful, will leave it empty)
//

describe('/users/get_current_account', function() {
  it('returns account id', function(done) {
    request(server)
      .post('/users/get_current_account')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert.equal(res.body.app_id, testAccount.app_id); 
          assert.equal(res.body.account_id, testAccount.account_id); 
      })
      .expect(200, done);
  });
});

describe('files/list_folder on empty root folder', function() {
  it('succeeds and returns 0 entries', function(done) {
    request(server)
      .post('/files/list_folder')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "" }')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert(res.body.entries);
          assert.equal(res.body.entries.length, 0); 
      })
      .expect(200, done);
  });
});

describe('files/upload of foo.txt to root', function() {
  it('succeeds', function(done) {
    request(server)
      .post('/files/upload')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "foo.txt" }')
      .send('Foo is the word')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert.equal(res.body[".tag"], 'file'); 
          assert.equal(res.body.name, 'foo.txt'); // !!! Check size, etc
      })
      .expect(200, done);
  });
  it('file shows up in list', function(done) {
    request(server)
      .post('/files/list_folder')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "" }')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert(res.body.entries);
          assert.equal(res.body.entries.length, 1); 
          assert.equal(res.body.entries[0][".tag"], 'file'); 
          assert.equal(res.body.entries[0].name, 'foo.txt'); 
      })
      .expect(200, done);
  });
});

describe('/files/download of foo.txt', function() {
  it('returns file contents', function(done) {
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

describe('/files/create_folder of test_folder', function() {
  it('succeeds in creating folder', function(done) {
    request(server)
      .post('/files/create_folder')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "test_folder" }')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert.equal(res.body[".tag"], 'folder'); 
          assert.equal(res.body.name, 'test_folder'); 
      })
      .expect(200, done);
  });
  it('new folder shows up in parent folder', function(done) {
    request(server)
      .post('/files/list_folder')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "" }')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert(res.body.entries);
          assert.equal(res.body.entries.length, 2); 
      })
      .expect(200, done);
  });
  it('new folder list_folder succeeds', function(done) {
    request(server)
      .post('/files/list_folder')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "test_folder" }')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert(res.body.entries);
          assert.equal(res.body.entries.length, 0); 
      })
      .expect(200, done);
  });
});

describe('/files/copy foo.txt to test_folder/bar.txt', function() {
  it('succeeds in copying file', function(done) {
    request(server)
      .post('/files/copy')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "from_path": "foo.txt", "to_path": "test_folder/bar.txt" }')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert.equal(res.body[".tag"], 'file'); 
          assert.equal(res.body.name, 'test_folder/bar.txt'); 
      })
      .expect(200, done);
  });
  it('file shows up in new location', function(done) {
    request(server)
      .post('/files/list_folder')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "test_folder" }')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert(res.body.entries);
          assert.equal(res.body.entries.length, 1); 
          assert.equal(res.body.entries[0][".tag"], 'file'); 
          assert.equal(res.body.entries[0].name, 'bar.txt'); 
      })
      .expect(200, done);
  });
  it('new file has correct contents', function(done) {
    request(server)
      .post('/files/download')
      .set('Accept', 'application/octet-stream')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "test_folder/bar.txt" }')
      .expect('Content-Type', 'application/octet-stream')
      .expect(function(res){
           assert.equal(res.body.toString(), 'Foo is the word'); 
      })
      .expect(200, done);
  });
});

describe('/files/delete of foo.txt', function() {
  it('succeeds in deleting file', function(done) {
    request(server)
      .post('/files/delete')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "foo.txt" }')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert.equal(res.body[".tag"], 'file'); 
          assert.equal(res.body.name, 'foo.txt'); 
      })
      .expect(200, done);
  });
  it('file no longers shows in folder', function(done) {
    request(server)
      .post('/files/list_folder')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "" }')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert(res.body.entries);
          assert.equal(res.body.entries.length, 1); 
          assert.equal(res.body.entries[0][".tag"], 'folder'); 
          assert.equal(res.body.entries[0].name, 'test_folder'); 
      })
      .expect(200, done);
  });
});

describe('/files/move of test_folder/bar.txt to baz.txt', function() {
  it('succeeds in moving file', function(done) {
    request(server)
      .post('/files/move')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "from_path": "test_folder/bar.txt", "to_path": "baz.txt" }')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert.equal(res.body[".tag"], 'file');
          assert.equal(res.body.name, 'baz.txt'); 
      })
      .expect(200, done);
  });
  it('file shows in new folder', function(done) {
    request(server)
      .post('/files/list_folder')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "" }')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert(res.body.entries);
          assert.equal(res.body.entries.length, 2); 
      })
      .expect(200, done);
  });
  it('file no longers shows in old folder', function(done) {
    request(server)
      .post('/files/list_folder')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "test_folder" }')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert(res.body.entries);
          assert.equal(res.body.entries.length, 0); 
      })
      .expect(200, done);
  });
  it('new file has correct contents', function(done) {
    request(server)
      .post('/files/download')
      .set('Accept', 'application/octet-stream')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "baz.txt" }')
      .expect('Content-Type', 'application/octet-stream')
      .expect(function(res){
           assert.equal(res.body.toString(), 'Foo is the word'); 
      })
      .expect(200, done);
  });
});

describe('/files/delete of test_folder', function() {
  it('succeeds in deleting folder', function(done) {
    request(server)
      .post('/files/delete')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "test_folder" }')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert.equal(res.body[".tag"], 'folder');
          assert.equal(res.body.name, 'test_folder'); 
      })
      .expect(200, done);
  });
  it('folder no longers shows in root', function(done) {
    request(server)
      .post('/files/list_folder')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "" }')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert(res.body.entries);
          assert.equal(res.body.entries.length, 1); 
          assert.equal(res.body.entries[0][".tag"], 'file'); 
          assert.equal(res.body.entries[0].name, 'baz.txt'); 
      })
      .expect(200, done);
  });
});

describe('/files/delete of baz.txt (last remaining file)', function() {
  it('succeeds in deleting file', function(done) {
    request(server)
      .post('/files/delete')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "baz.txt" }')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert.equal(res.body[".tag"], 'file'); 
          assert.equal(res.body.name, 'baz.txt'); 
      })
      .expect(200, done);
  });
  it('root folder is empty', function(done) {
    request(server)
      .post('/files/list_folder')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "" }')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert(res.body.entries);
          assert.equal(res.body.entries.length, 0); 
      })
      .expect(200, done);
  });
});
