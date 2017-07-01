var request = require('supertest');
var assert = require('assert');
var jwt = require('jsonwebtoken');

var mantaBoxServer = require('./../lib/server');
var loggerModule = require('./../lib/logger');
loggerModule.createTestLogger();

var log = loggerModule.getLogger('Test');

var _testSecret = "test";

// !!! Note: Manta tests pass, but there are some timing issues (file uploaded might not appear in directory list or be 
//           available for download immediately, etc).  So these tests have all passed in one run on Manta, but they don't
//           run clean every time.  Maybe we should just add a wait in between operations where needed (and only for Manta).
//
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

describe('/files/copy foo.txt to existing test_folder/bar.txt', function() {
  it('succeeds in copying file over existing', function(done) {
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

//
// !!! list_folder + list_folder/continue (cursor, limit, hasmore, etc) - recursive and non-recursive
// !!! list_folder/get_latest_cursor, list_folder/continue (empty), add file, list_folder/continue (new file shows up)
// !!! list_folder/get_latest_cursor, list_folder/longpoll - this on might be tricky
//

//
// !!! Only if file driver (not implemented in Manta yet)
//
describe('Multipart upload', function() {
  var uploadId;
  it('succeeds in starting upload session', function(done) {
    request(server)
      .post('/files/upload_session/start')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ }')
      .send('Foo is the word')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert(res.body.session_id); 
      })
      .expect(function(res) {
          uploadId = res.body.session_id;
      })
      .expect(200, done);
  });
  it('succeeds in appending first part using append', function(done) {
    request(server)
      .post('/files/upload_session/append')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "session_id": "' + uploadId + '", "offset": 15 }')
      .send('Bar is the next word')
      .expect(200, done); // !!! Verify not content returned (no c/t?)
  });
  it('succeeds in appending second part using append_v2', function(done) {
    request(server)
      .post('/files/upload_session/append_v2')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "cursor": { "session_id": "' + uploadId + '", "offset": 35 } }')
      .send('Baz is the third word')
      .expect(200, done);  // !!! Verify not content returned (no c/t?)
  });
  it('succeeds in finishing upload', function(done) {
    request(server)
      .post('/files/upload_session/finish')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "cursor": { "session_id": "' + uploadId + '", "offset": 56 }, "commit": { "path": "target.txt" } }')
      .send('Fraz is the final word')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert.equal(res.body[".tag"], 'file'); 
          assert.equal(res.body.name, 'target.txt'); 
          assert.equal(res.body.size, 78); 
      })
      .expect(200, done);
  });
  it('uploaded file has correct contents', function(done) {
    request(server)
      .post('/files/download')
      .set('Accept', 'application/octet-stream')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "target.txt" }')
      .expect('Content-Type', 'application/octet-stream')
      .expect(function(res){
           assert.equal(res.body.toString(), 'Foo is the wordBar is the next wordBaz is the third wordFraz is the final word'); 
      })
      .expect(200, done);
  });
  it('succeeds in deleting uploaded file', function(done) {
    request(server)
      .post('/files/delete')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "target.txt" }')
      .expect(200, done);
  });
});
