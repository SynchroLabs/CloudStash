var request = require('supertest');
var assert = require('assert');

var async = require('async');
var fs = require('fs');

var jwt = require('jsonwebtoken');

var cloudStashServer = require('./../lib/server');
var loggerModule = require('./../lib/logger');
loggerModule.createTestLogger();

var log = loggerModule.getLogger('Test');

var _testSecret = "test";

var driver; // = "manta";

var _testConfig;
var _testTimeout = 2000;

if (driver === "manta")
{
  // !!! Note: Manta tests pass, but there are some timing issues (file uploaded might not appear in 
  //     directory list or be available for download immediately, etc).  So these tests have all passed
  //     in one run on Manta, but they don't run clean every time.  Maybe we should just add a wait in
  //     between operations where needed (and only for Manta).
  //
  _testConfig = require('./../lib/config').getConfig('config_manta.json');

  // This keeps Mocha from timing out tests in the default 2000ms (some of the Manta driver calls we make
  // trigger round trips to Manta, which from a machine not running in the Joyent datacenter can take a fair amount
  // of time to complete).
  //
  _testTimeout = 5000;
}
else
{
  var _testConfig = require('./../lib/config').getConfig(null, 
  {
      "driver":
      { 
          "provider": "file", 
          "basePath": "test/_test_store" 
      },
      "LONGPOLL_INTERVAL_MS": 1000
  });
}

var server = cloudStashServer(_testSecret, _testConfig);

var testAccount = 
{ 
    app_id:     "TEST01",
    account_id: "1234-BEEF"
};

var testToken = jwt.sign(testAccount, _testSecret + "authToken");

// !!! Test delete of root folder (specified as either "/" or "") - see what Dropbox does.  Ditto move/copy.
//
// !!! Test list_folder of non-existent folder
//
// !!! Test download of non-existent file (I think it times out)
//

// Tests below assume starting with a 1234-BEEF/TEST01 that is empty (and if successful, will leave it empty)
//

function areBuffersEqual(a, b) 
{
    if (!Buffer.isBuffer(a)) return undefined;
    if (!Buffer.isBuffer(b)) return undefined;
    if (typeof a.equals === 'function') return a.equals(b);
    if (a.length !== b.length) return false;
    
    for (var i = 0; i < a.length; i++) {
        if (a[i] !== b[i]) return false;
    }
    
    return true;
};

describe('CloudStash', function() {

  if (driver === "manta")
  {
    this.timeout(_testTimeout);
  }

  describe('/users/get_current_account', function() {
    it('returns account id', function(done) {
      request(server)
        .post('/2/users/get_current_account')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body); 
            assert(res.body.account_id); 
            assert.equal(res.body.account_id.length, 40); 
            assert.equal(res.body.account_id.trim(), testAccount.account_id); 
        })
        .expect(200, done);
    });
  });

  describe('files/list_folder on empty root folder', function() {
    it('succeeds and returns 0 entries', function(done) {
      request(server)
        .post('/2/files/list_folder')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "" })
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
        .post('/2/files/upload')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ "path": "/foo.txt" }')
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
        .post('/2/files/list_folder')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "" })
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

  describe('/files/download', function() {
    var eTag;
    var lastModified;
    it('succeeds and returns file contents for existing object foo.txt', function(done) {
      request(server)
        .post('/2/files/download')
        .set('Accept', 'text/plain')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ "path": "foo.txt" }')
        .expect('Content-Type', 'text/plain')
        .expect('Dropbox-API-Result', /.+/)
        .expect(function(res){
             assert(res);
             assert(res.headers);
             assert(res.headers["etag"]);
             assert(res.headers["last-modified"]);
             assert(res.headers["accept-ranges"]);
             assert.equal(res.headers["accept-ranges"], "bytes");
             assert(res.headers["dropbox-api-result"]);
             var entry = JSON.parse(res.headers["dropbox-api-result"]);
             assert.equal(entry.name, "foo.txt");
             assert(res.body);
             assert.equal(res.text, 'Foo is the word'); 
             eTag = res.headers["etag"];
             lastModified = res.headers["last-modified"];
        })
        .expect(200, done);
    });
    it('fails for non-existant object', function(done) {
      request(server)
        .post('/2/files/download')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ "path": "flarg" }')
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert(res.body.error);
            assert.equal(res.body.error_summary, 'path/not_found'); 
            assert.equal(res.body.error[".tag"], 'path'); 
            assert.equal(res.body.error.path[".tag"], 'not_found'); 
        })
        .expect(409, done);
    });
    it('returns not-modified when using current eTag', function(done) {
      request(server)
        .post('/2/files/download')
        .set('Authorization', "Bearer " + testToken)
        .set('If-None-Match', eTag)
        .set('Dropbox-API-Arg', '{ "path": "foo.txt" }')
//      .expect('Content-Type', 'text/plain')
        .expect(304, done);
    });
    it('succeeds and returns file contents when using non-current eTag', function(done) {
      request(server)
        .post('/2/files/download')
        .set('Accept', 'text/plain')
        .set('Authorization', "Bearer " + testToken)
        .set('If-None-Match', 'foo')
        .set('Dropbox-API-Arg', '{ "path": "foo.txt" }')
        .expect('Content-Type', 'text/plain')
        .expect('Dropbox-API-Result', /.+/)
        .expect(function(res){
             assert(res);
             assert(res.headers);
             assert(res.headers["dropbox-api-result"]);
             var entry = JSON.parse(res.headers["dropbox-api-result"]);
             assert.equal(entry.name, "foo.txt");
             assert(res.body);
             assert.equal(res.text, 'Foo is the word'); 
        })
        .expect(200, done);
    });
    it('returns not-modified when using current last-modified', function(done) {
      request(server)
        .post('/2/files/download')
        .set('Authorization', "Bearer " + testToken)
        .set('If-Modified-Since', lastModified)
        .set('Dropbox-API-Arg', '{ "path": "foo.txt" }')
//      .expect('Content-Type', 'text/plain')
        .expect(304, done);
    });
    it('returns content when using older date', function(done) {
      var dayBeforeLastModified = new Date(lastModified);
      dayBeforeLastModified.setDate(dayBeforeLastModified.getDate()-1);
      request(server)
        .post('/2/files/download')
        .set('Authorization', "Bearer " + testToken)
        .set('If-Modified-Since', dayBeforeLastModified)
        .set('Dropbox-API-Arg', '{ "path": "foo.txt" }')
        .expect('Content-Type', 'text/plain')
        .expect(function(res){
             assert(res);
             assert(res.headers);
             assert(res.headers["dropbox-api-result"]);
             var entry = JSON.parse(res.headers["dropbox-api-result"]);
             assert.equal(entry.name, "foo.txt");
             assert(res.body);
             assert.equal(res.text, 'Foo is the word'); 
        })
        .expect(200, done);
    });
    it('returns not-modified when using newer date', function(done) {
      var dayAfterLastModified = new Date(lastModified);
      dayAfterLastModified.setDate(dayAfterLastModified.getDate()+1);
      request(server)
        .post('/2/files/download')
        .set('Authorization', "Bearer " + testToken)
        .set('If-Modified-Since', dayAfterLastModified)
        .set('Dropbox-API-Arg', '{ "path": "foo.txt" }')
//      .expect('Content-Type', 'text/plain')
        .expect(304, done);
    });
    it('succeeds and returns requested range', function(done) {
      request(server)
        .post('/2/files/download')
        .set('Accept', 'text/plain')
        .set('Authorization', "Bearer " + testToken)
        .set('If-Match', eTag)
        .set('Range', 'bytes=4-9')
        .set('Dropbox-API-Arg', '{ "path": "foo.txt" }')
//      .expect('Content-Type', 'text/plain')
        .expect('Content-Range', 'bytes 4-9/15')
        .expect(function(res){
           if (res.header['content-type'] === 'text/plain') {
             assert.equal(res.text, 'is the'); 
           }
           else {
             assert.equal(res.body, 'is the');
           }
        })
        .expect(206, done);
    });
    it('returns range not satisfiable with non-current If-Match eTag', function(done) {
      request(server)
        .post('/2/files/download')
        .set('Accept', 'text/plain')
        .set('Authorization', "Bearer " + testToken)
        .set('If-Match', "foo")
        .set('Range', 'bytes=4-9')
        .set('Dropbox-API-Arg', '{ "path": "foo.txt" }')
        .expect(416, done);
    });
    it('returns range not satisfiable with bad range', function(done) {
      request(server)
        .post('/2/files/download')
        .set('Accept', 'text/plain')
        .set('Authorization', "Bearer " + testToken)
        .set('If-Match', eTag)
        .set('Range', 'bytes=40-90')
        .set('Dropbox-API-Arg', '{ "path": "foo.txt" }')
        .expect(416, done);
    });
  });

  describe('upload and download of binary file', function(){
    var buf = fs.readFileSync('./test/files/CloudFolder.png');
    it('succeeds in uploading file', function(done) {
      request(server)
        .post('/2/files/upload')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ "path": "/CloudFolder.png" }')
        .send(buf)
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body[".tag"], 'file'); 
            assert.equal(res.body.name, 'CloudFolder.png'); // !!! Check size, etc
        })
        .expect(200, done);
    });
    it('returns file contents', function(done) {
      request(server)
        .post('/2/files/download')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ "path": "/CloudFolder.png" }')
        .expect('Content-Type', 'image/png')
        .expect('Dropbox-API-Result', /.+/)
        .expect(function(res){
             log.info("Res:", res);
             assert(res);
             assert(res.headers);
             assert(res.headers["dropbox-api-result"]);
             var entry = JSON.parse(res.headers["dropbox-api-result"]);
             assert.equal(entry.name, "CloudFolder.png");
             assert(res.body);
             assert(areBuffersEqual(res.body, buf)); 
        })
        .expect(200, done);
    });
    after("Clean up folder contents", function(done)
    {
      request(server)
        .post('/2/files/delete')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "/CloudFolder.png" })
        .expect('Content-Type', /json/)
        .expect(200, done);
    });
  });

  describe('files/upload of foo.txt when already exists', function() {
    it('fails as expected', function(done) {
      request(server)
        .post('/2/files/upload')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ "path": "/foo.txt" }')
        .send('Foo is not the word')
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert(res.body.error);
            assert.equal(res.body.error_summary, 'to/conflict'); 
            assert.equal(res.body.error[".tag"], 'to'); 
            assert.equal(res.body.error.to[".tag"], 'conflict'); 
        })
        .expect(409, done);
    });
    it('succeeds with rename', function(done) {
      request(server)
        .post('/2/files/upload')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ "path": "/foo.txt", "autorename": true }')
        .send('Foo1 is the word')
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body[".tag"], 'file'); 
            assert.equal(res.body.name, 'foo (1).txt');
        })
        .expect(200, done);
    });
    it('succeeds with overwrite', function(done) {
      request(server)
        .post('/2/files/upload')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ "path": "/foo (1).txt", "mode": "overwrite" }')
        .send('Foo1 is the word again')
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body[".tag"], 'file'); 
            assert.equal(res.body.name, 'foo (1).txt');
        })
        .expect(200, done);
    });
    it('returns new file contents after overwrite', function(done) {
      request(server)
        .post('/2/files/download')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ "path": "/foo (1).txt" }')
        .expect('Content-Type', 'text/plain')
        .expect('Dropbox-API-Result', /.+/)
        .expect(function(res){
             assert(res);
             assert(res.headers);
             assert(res.headers["dropbox-api-result"]);
             var entry = JSON.parse(res.headers["dropbox-api-result"]);
             assert.equal(entry.name, "foo (1).txt");
             assert(res.text);
             assert(res.text, "Foo1 is the word again"); 
        })
        .expect(200, done);
    });
    after("Clean up folder contents", function(done)
    {
      request(server)
        .post('/2/files/delete')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "/foo (1).txt" })
        .expect('Content-Type', /json/)
        .expect(200, done);
    });
  });

  describe('/files/create_folder of test_folder', function() {
    it('succeeds in creating folder', function(done) {
      request(server)
        .post('/2/files/create_folder')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "/test_folder" })
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
        .post('/2/files/list_folder')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "" })
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
        .post('/2/files/list_folder')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "/test_folder" })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert(res.body.entries);
            assert.equal(res.body.entries.length, 0); 
        })
        .expect(200, done);
    });
  });

  describe('/files/get_metadata', function() {
    it('succeeds for existing folder', function(done) {
      request(server)
        .post('/2/files/get_metadata')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "/test_folder" })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body[".tag"], 'folder'); 
            assert.equal(res.body.name, 'test_folder'); 
        })
        .expect(200, done);
    });
    it('succeeds for existing file', function(done) {
      request(server)
        .post('/2/files/get_metadata')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "/foo.txt" })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body[".tag"], 'file'); 
            assert.equal(res.body.name, 'foo.txt'); 
        })
        .expect(200, done);
    });
    it('fails for non-existant object', function(done) {
      request(server)
        .post('/2/files/get_metadata')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "/flarf" })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert(res.body.error);
            assert.equal(res.body.error_summary, 'path/not_found'); 
            assert.equal(res.body.error[".tag"], 'path'); 
            assert.equal(res.body.error.path[".tag"], 'not_found'); 
        })
        .expect(409, done);
    });
  });

  describe('/files/copy foo.txt to test_folder/bar.txt', function() {
    it('succeeds in copying file', function(done) {
      request(server)
        .post('/2/files/copy')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ from_path: "/foo.txt", to_path: "/test_folder/bar.txt" })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body[".tag"], 'file'); 
            assert.equal(res.body.name, 'bar.txt'); 
            assert.equal(res.body.path_display, '/test_folder/bar.txt'); 
        })
        .expect(200, done);
    });
    it('file shows up in new location', function(done) {
      request(server)
        .post('/2/files/list_folder')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "/test_folder" })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert(res.body.entries);
            assert.equal(res.body.entries.length, 1); 
            assert.equal(res.body.entries[0][".tag"], 'file'); 
            assert.equal(res.body.entries[0].name, 'bar.txt'); 
            assert.equal(res.body.entries[0].path_display, '/test_folder/bar.txt'); 
        })
        .expect(200, done);
    });
    it('new file has correct contents', function(done) {
      request(server)
        .post('/2/files/download')
        .set('Accept', 'application/octet-stream')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ "path": "/test_folder/bar.txt" }')
        .expect('Content-Type', 'text/plain')
        .expect(function(res){
             assert.equal(res.text, 'Foo is the word'); 
        })
        .expect(200, done);
    });
  });

  describe('/files/copy foo.txt to existing test_folder/bar.txt', function() {
    it('fails in copying file over existing', function(done) {
      request(server)
        .post('/2/files/copy')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ from_path: "/foo.txt", to_path: "/test_folder/bar.txt" })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert(res.body.error);
            assert.equal(res.body.error_summary, 'to/conflict'); 
            assert.equal(res.body.error[".tag"], 'to'); 
            assert.equal(res.body.error.to[".tag"], 'conflict'); 
        })
        .expect(409, done);
    });
    it('succeeds in copying file over existing with overwrite', function(done) {
      request(server)
        .post('/2/files/copy')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ from_path: "/foo.txt", to_path: "/test_folder/bar.txt", overwrite: true })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body[".tag"], 'file'); 
            assert.equal(res.body.name, 'bar.txt'); 
            assert.equal(res.body.path_display, '/test_folder/bar.txt'); 
        })
        .expect(200, done);
    });
    it('succeeds in copying file over existing with rename', function(done) {
      request(server)
        .post('/2/files/copy')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ from_path: "/foo.txt", to_path: "/test_folder/bar.txt", autorename: true })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body[".tag"], 'file'); 
            assert.equal(res.body.name, 'bar (1).txt'); 
            assert.equal(res.body.path_display, '/test_folder/bar (1).txt'); 
        })
        .expect(200, done);
    });
    after("Clean up folder contents", function(done)
    {
      request(server)
        .post('/2/files/delete')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "test_folder/bar (1).txt" })
        .expect('Content-Type', /json/)
        .expect(200, done);
    });
  });

  describe('/files/delete of foo.txt', function() {
    it('succeeds in deleting file', function(done) {
      request(server)
        .post('/2/files/delete')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "foo.txt" })
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
        .post('/2/files/list_folder')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "" })
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
        .post('/2/files/move')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ from_path: "test_folder/bar.txt", to_path: "baz.txt" })
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
        .post('/2/files/list_folder')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "" })
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
        .post('/2/files/list_folder')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "test_folder" })
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
        .post('/2/files/download')
        .set('Accept', 'application/octet-stream')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ "path": "baz.txt" }')
        .expect('Content-Type', 'text/plain')
        .expect(function(res){
             assert.equal(res.text, 'Foo is the word'); 
        })
        .expect(200, done);
    });
  });

  describe('/files/delete of test_folder', function() {
    it('succeeds in deleting folder', function(done) {
      request(server)
        .post('/2/files/delete')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "test_folder" })
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
        .post('/2/files/list_folder')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "" })
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
        .post('/2/files/delete')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "baz.txt" })
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
        .post('/2/files/list_folder')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "" })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert(res.body.entries);
            assert.equal(res.body.entries.length, 0); 
        })
        .expect(200, done);
    });
  });

  describe("list folder and friends", function() {
    before("Create folder contents", function(done)
    {
      var files =
      [
        { name: "/one.txt", contents: "This is file one.txt" },
        { name: "/two.txt", contents: "This is file two.txt" },
        { name: "/subfolder/three.txt", contents: "This is file three.txt" },
        { name: "/subfolder/four.txt", contents: "This is file four.txt" },
        { name: "/five.txt", contents: "This is file five.txt" }
      ]

      // We want to create the files in a specific order so they will come back sorted by mtime propertly.  However, on some
      // file systems (MacOS), the granularity of the file mtime is one second.  So if we just create these files in order,
      // all (or most) of them will have the same mtime, and thus be in an unpredictable sort order (files within the same mtime
      // will be sorted by name, but we can't guarantee that all of the files will be in the same mtime).
      //
      // To get around this, we introduce a delay befween each operation to make sure that every file/dir is created in its own
      // millisecond, this producing a predictable result order.
      //
      var intervalMs = 1050;
      this.timeout((_testTimeout + intervalMs) * files.length); 

      async.eachSeries(files, function(file, callback)
      {
          log.info("Processing file:", file.name);
          request(server)
            .post('/2/files/upload')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .set('Dropbox-API-Arg', '{ "path": "' + file.name + '" }')
            .send(file.contents)
            .expect(200, function(err)
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    setTimeout(callback, intervalMs);
                }
            });
      },
      function(err)
      {
          if (err)
          {
              log.error(err);
          }
          done(err);
      });
    });

    // Now we have:
    //
    //    /one.txt
    //    /two.txt
    //    /subfolder/three.txt
    //    /subfolder/four.txt
    //    /five.txt
    //
    // NOTE: We will be using the "limit" parameter below to set the page size for result sets.  This is not part
    //       of the DropBox API (they use a hard-coded default of 725 results per request/page).  We introduced
    //       the limit parameter specifically to make it easier to test the paging parts of these APIs.
    //

    it('non-recursive list_folder on root contains correct files', function(done) {
      request(server)
        .post('/2/files/list_folder')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "" })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert(res.body.entries);
            assert.equal(res.body.entries.length, 4);
            log.info("Results:", res.body.entries);
            assert.equal(res.body.entries[0].name, "one.txt"); 
            assert.equal(res.body.entries[1].name, "two.txt"); 
            assert.equal(res.body.entries[2].name, "subfolder"); 
            assert.equal(res.body.entries[3].name, "five.txt"); 
            assert.equal(res.body.has_more, false); 
            assert(res.body.cursor);
        })
        .expect(200, done);
    });

    it('recursive list_folder on root contains correct files', function(done) {
      request(server)
        .post('/2/files/list_folder')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "", recursive: true })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert(res.body.entries);
            assert.equal(res.body.entries.length, 6);
            log.info("Results:", res.body.entries);
            assert.equal(res.body.entries[0].name, "one.txt"); 
            assert.equal(res.body.entries[1].name, "two.txt"); 
            assert.equal(res.body.entries[2].name, "subfolder"); 
            assert.equal(res.body.entries[3].name, "three.txt"); 
            assert.equal(res.body.entries[4].name, "four.txt"); 
            assert.equal(res.body.entries[5].name, "five.txt"); 
            assert.equal(res.body.has_more, false); 
            assert(res.body.cursor);
        })
        .expect(200, done);
    });

    var cursor;

    it('recursive list_folder on root returns correct first page of results', function(done) {
      request(server)
        .post('/2/files/list_folder')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "", recursive: true, limit: 3 })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert(res.body.entries);
            assert.equal(res.body.entries.length, 3);
            log.info("Results:", res.body.entries);
            assert.equal(res.body.entries[0].name, "one.txt"); 
            assert.equal(res.body.entries[1].name, "two.txt"); 
            assert.equal(res.body.entries[2].name, "subfolder"); 
            assert.equal(res.body.has_more, true); 
            assert(res.body.cursor);
            cursor = res.body.cursor;
        })
        .expect(200, done);
    });

    it('list_folder/continue on recursive list_folder on root returns correct second page of results', function(done) {
      request(server)
        .post('/2/files/list_folder/continue')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ cursor: cursor })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert(res.body.entries);
            assert.equal(res.body.entries.length, 3);
            log.info("Results:", res.body.entries);
            assert.equal(res.body.entries[0].name, "three.txt"); 
            assert.equal(res.body.entries[1].name, "four.txt"); 
            assert.equal(res.body.entries[2].name, "five.txt"); 
            assert.equal(res.body.has_more, false); 
            assert(res.body.cursor);
            cursor = res.body.cursor;
        })
        .expect(200, done);
    });

    it('list_folder/continue on cursor from end of results returns no results', function(done) {
      request(server)
        .post('/2/files/list_folder/continue')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ cursor: cursor })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert(res.body.entries);
            assert.equal(res.body.entries.length, 0);
            assert.equal(res.body.has_more, false); 
            assert(res.body.cursor);
        })
        .expect(200, done);
    });

    it('list_folder/continue on cursor from end of results returns file added later', function(done) {
      async.series(
      [
        function(callback) 
        {
          request(server)
            .post('/2/files/upload')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .set('Dropbox-API-Arg', '{ "path": "six.txt" }')
            .send('This is file six.txt')
            .expect(200, callback);
        },
        function(callback)
        {
          request(server)
            .post('/2/files/list_folder/continue')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .send({ cursor: cursor })
            .expect('Content-Type', /json/)
            .expect(function(res){
                assert(res.body);
                assert(res.body.entries);
                assert.equal(res.body.entries.length, 1);
                assert.equal(res.body.entries[0].name, "six.txt"); 
                assert.equal(res.body.has_more, false); 
                assert(res.body.cursor);
            })
            .expect(200, callback);
        },
      ],
      function(err, results) 
      {
        if (err)
        {
          log.error(err);
        }
        done(err);
      });
    });

    it('list_folder/get_latest_cursor succeeds', function(done) {
      request(server)
        .post('/2/files/list_folder/get_latest_cursor')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "", recursive: true })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert(res.body.cursor);
            cursor = res.body.cursor;
        })
        .expect(200, done);
    });

    it('list_folder/continue on get_latest_cursor returns no results', function(done) {
      request(server)
        .post('/2/files/list_folder/continue')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ cursor: cursor })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert(res.body.entries);
            assert.equal(res.body.entries.length, 0);
            assert.equal(res.body.has_more, false); 
            assert(res.body.cursor);
        })
        .expect(200, done);
    });

    var cursorAfterAdd

    it('list_folder/continue returns file added after get_latest_cursor', function(done) {
      var intervalMs = 1050;
      this.timeout(_testTimeout + intervalMs); 
      async.series(
      [
        function(callback)
        {
          // We need to wait to make sure the new file has a later timestamp
          setTimeout(callback, intervalMs);
        },
        function(callback) 
        {
          request(server)
            .post('/2/files/upload')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .set('Dropbox-API-Arg', '{ "path": "seven.txt" }')
            .send('This is file seven.txt')
            .expect(200, callback);
        },
        function(callback)
        {
          request(server)
            .post('/2/files/list_folder/continue')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .send({ cursor: cursor })
            .expect('Content-Type', /json/)
            .expect(function(res){
                assert(res.body);
                assert(res.body.entries);
                assert.equal(res.body.entries.length, 1);
                assert.equal(res.body.entries[0].name, "seven.txt"); 
                assert.equal(res.body.has_more, false); 
                assert(res.body.cursor);
                assert.notEqual(res.body.cursor, cursor);
                cursorAfterAdd = res.body.cursor;
            })
            .expect(200, callback);
        },
      ],
      function(err, results) 
      {
        if (err)
        {
          log.error(err);
        }
        done(err);
      });
    });

    

    it('list_folder/longpoll returns false using cursor without changes', function(done) {
      // Note: No auth header (this API endpoint doesn't used auth - gets what it needs from the cursor)
      this.timeout(_testTimeout + _testConfig.get('LONGPOLL_INTERVAL_MS') + 1000); 

      request(server)
        .post('/2/files/list_folder/longpoll')
        .set('Accept', 'application/json')
        .send({ cursor: cursorAfterAdd, timeout: -1 })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            log.info("res:", res.body)
            assert.equal(res.body.changes, false);
        })
        .expect(200, done);
    });

    it('list_folder/longpoll returns true using current with changes', function(done) {
      // Note: No auth header (this API endpoint doesn't used auth - gets what it needs from the cursor)
      request(server)
        .post('/2/files/list_folder/longpoll')
        .set('Accept', 'application/json')
        .send({ cursor: cursor, timeout: -1 })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body.changes, true);
        })
        .expect(200, done);
    });

    after("Clean up folder contents", function(done)
    {
      var files = 
      [
          "seven.txt",
          "six.txt",
          "five.txt",
          "subfolder", // Will automatically delete contained four.txt and three.txt
          "two.txt",
          "one.txt"
      ]

      this.timeout(_testTimeout * files.length); 

      async.eachSeries(files, function(file, callback)
      {
          request(server)
            .post('/2/files/delete')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .send({ path: file })
            .expect(200, callback);
      },
      function(err)
      {
          if (err)
          {
              log.error(err);
          }
          done(err);
      });
    });
  });

  describe('Folder operations', function(done) {

    var files =
    [
      { file: "/one.txt", contents: "This is file one.txt" },
      { file: "/two.txt", contents: "This is file two.txt" },
      { file: "/subfolder/three.txt", contents: "This is file three.txt" },
      { file: "/subfolder/four.txt", contents: "This is file four.txt" },
      { folder: "/empty" },
      { file: "/five.txt", contents: "This is file five.txt" }
    ]

    before("Create folder contents (testfolder)", function(done)
    {
      this.timeout(_testTimeout * files.length); 

      async.eachSeries(files, function(entry, callback)
      {
        if (entry.file)
        {
          request(server)
            .post('/2/files/upload')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .set('Dropbox-API-Arg', '{ "path": "/testfolder' + entry.file + '" }')
            .send(entry.contents)
            .expect(200, callback);
        }
        else
        {
          request(server)
            .post('/2/files/create_folder')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .send({ 'path': '/testfolder' + entry.folder })
            .expect(200, callback);
        }
      },
      function(err)
      {
          log.error("Err:", err);
          done(err);
      });
    });

    it('finds item', function(done) {
      request(server)
        .post('/2/files/search')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "/testfolder", query: "three" })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert(res.body.matches);
            assert.equal(res.body.matches.length, 1); 
            assert.equal(res.body.matches[0].match_type[".tag"], "filename"); 
            assert.equal(res.body.matches[0].metadata["name"], "three.txt"); 
        })
        .expect(200, done);
    });

    it('finds item case mismatch', function(done) {
      request(server)
        .post('/2/files/search')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "/testfolder", query: "ThRee" })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert(res.body.matches);
            assert.equal(res.body.matches.length, 1); 
            assert.equal(res.body.matches[0].match_type[".tag"], "filename"); 
            assert.equal(res.body.matches[0].metadata["name"], "three.txt"); 
        })
        .expect(200, done);
    });

    it('succeeds in moving folder tree', function(done) {
      this.timeout(_testTimeout * files.length); 
      request(server)
        .post('/2/files/move')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ from_path: "/testfolder", to_path: "/testfolder1" })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body[".tag"], 'folder');
            assert.equal(res.body.name, 'testfolder1'); 
        })
        .expect(200, done);
    });

    it('moved folder tree contents are correct', function(done) {
      this.timeout(_testTimeout * files.length); 
      async.eachSeries(files, function(entry, callback)
      {
        if (entry.file)
        {
          request(server)
            .post('/2/files/download')
            .set('Accept', 'application/octet-stream')
            .set('Authorization', "Bearer " + testToken)
            .send({ "path": "/testfolder1/" + entry.file })
            .set('Dropbox-API-Arg', '{ "path": "/testfolder1' + entry.file + '" }')
            .expect('Content-Type', 'text/plain')
            .expect(function(res){
                 assert.equal(res.text, entry.contents); 
            })
            .expect(200, callback);
        }
        else
        {
          request(server)
            .post('/2/files/get_metadata')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .send({ 'path': '/testfolder1' + entry.folder })
            .expect(function(res){
                assert(res.body);
                assert.equal(res.body[".tag"], "folder");
                assert.equal(res.body["path_display"], "/testfolder1" + entry.folder);
            })
            .expect(200, callback);
        }
      },
      function(err)
      {
          log.error("Err:", err);
          done(err);
      });
    });

    it('move source folder no longer present after move', function(done) {
      request(server)
        .post('/2/files/get_metadata')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "/testfolder" })
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body.error_summary, 'path/not_found');
        })
        .expect(409, done);
    });

    it('succeeds in copying folder tree', function(done) {
      this.timeout(_testTimeout * files.length); 
      request(server)
        .post('/2/files/copy')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ from_path: "/testfolder1", to_path: "/testfolder2" })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body[".tag"], 'folder');
            assert.equal(res.body.name, 'testfolder2'); 
        })
        .expect(200, done);
    });

    it('copied folder tree contents are correct', function(done) {
      this.timeout(_testTimeout * files.length); 
      async.eachSeries(files, function(entry, callback)
      {
        if (entry.file)
        {
          request(server)
            .post('/2/files/download')
            .set('Accept', 'application/octet-stream')
            .set('Authorization', "Bearer " + testToken)
            .send({ "path": "/testfolder2/" + entry.file })
            .set('Dropbox-API-Arg', '{ "path": "/testfolder2' + entry.file + '" }')
            .expect('Content-Type', 'text/plain')
            .expect(function(res){
                 assert.equal(res.text, entry.contents); 
            })
            .expect(200, callback);
        }
        else
        {
          request(server)
            .post('/2/files/get_metadata')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .send({ 'path': '/testfolder2' + entry.folder })
            .expect(function(res){
                assert(res.body);
                assert.equal(res.body[".tag"], "folder");
                assert.equal(res.body["path_display"], "/testfolder2" + entry.folder);
            })
            .expect(200, callback);
        }
      },
      function(err)
      {
          log.error("Err:", err);
          done(err);
      });
    });

    it('source of copied folder tree contents unchanged', function(done) {
      this.timeout(_testTimeout * files.length); 
      async.eachSeries(files, function(entry, callback)
      {
        if (entry.file)
        {
          request(server)
            .post('/2/files/download')
            .set('Accept', 'application/octet-stream')
            .set('Authorization', "Bearer " + testToken)
            .send({ "path": "/testfolder1/" + entry.file })
            .set('Dropbox-API-Arg', '{ "path": "/testfolder1' + entry.file + '" }')
            .expect('Content-Type', 'text/plain')
            .expect(function(res){
                 assert.equal(res.text, entry.contents); 
            })
            .expect(200, callback);
        }
        else
        {
          request(server)
            .post('/2/files/get_metadata')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .send({ 'path': '/testfolder1' + entry.folder })
            .expect(function(res){
                assert(res.body);
                assert.equal(res.body[".tag"], "folder");
                assert.equal(res.body["path_display"], "/testfolder1" + entry.folder);
            })
            .expect(200, callback);
        }
      },
      function(err)
      {
          log.error("Err:", err);
          done(err);
      });
    });

    it('succeeds in deleting folder tree', function(done) {
      this.timeout(_testTimeout * files.length); 
      request(server)
        .post('/2/files/delete')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "/testfolder1" })
        .expect(200, done);
    });

    it('deleted folder tree not present', function(done) {
      request(server)
        .post('/2/files/get_metadata')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "/testfolder1" })
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body.error_summary, 'path/not_found');
        })
        .expect(409, done);
    });

    after("Cleanup", function(done){
      this.timeout(_testTimeout * files.length); 
      request(server)
        .post('/2/files/delete')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "/testfolder2" })
        .expect(200, done);
    });
  });

  describe('Folder batch operations', function(done) 
  {
    var files =
    [
      { file: "/one.txt", contents: "This is file one.txt" },
      { file: "/two.txt", contents: "This is file two.txt" },
      { file: "/subfolder/three.txt", contents: "This is file three.txt" },
      { file: "/subfolder/four.txt", contents: "This is file four.txt" },
      { folder: "/empty" },
      { file: "/five.txt", contents: "This is file five.txt" }
    ]

    before("Create folder contents (testfolder)", function(done)
    {
      this.timeout(_testTimeout * files.length); 
      async.eachSeries(files, function(entry, callback)
      {
        if (entry.file)
        {
          request(server)
            .post('/2/files/upload')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .set('Dropbox-API-Arg', '{ "path": "/testfolder' + entry.file + '" }')
            .send(entry.contents)
            .expect(200, callback);
        }
        else
        {
          request(server)
            .post('/2/files/create_folder')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .send({ 'path': '/testfolder' + entry.folder })
            .expect(200, callback);
        }
      },
      function(err)
      {
          log.error("Err:", err);
          done(err);
      });
    });

    var async_job_id;

    it('succeeds in batch move of folder tree', function(done) {
      request(server)
        .post('/2/files/move_batch')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ entries: [ { from_path: "/testfolder", to_path: "/testfolder1" } ] })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body[".tag"], 'async_job_id');
            async_job_id = res.body["async_job_id"];
            log.info("Got job id:", async_job_id);
        })
        .expect(200, done);
    });

    it('gets "complete" from batch move folder tree job', function(done) {
      var complete = false;
      this.timeout(_testTimeout * files.length); 
      async.whilst(
        function() 
        { 
          return !complete;
        },
        function(callback) 
        {
          request(server)
            .post('/2/files/move_batch/check')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .send({ "async_job_id": async_job_id })
            .expect('Content-Type', /json/)
            .expect(function(res){
                assert(res.body);
                if (res.body[".tag"] === "complete")
                {
                  log.info("Batch move complete");
                  complete = true;
                }
                else
                {
                  // If not complete, anything other than in_progress is an error
                  assert.equal(res.body[".tag"], 'in_progress');
                }
            })
            .expect(200, function(err)
            {
              if (err || complete)
              {
                callback(err);
              }
              else
              {
                // If we're not done, wait before retrying
                setTimeout(callback, 1000);
              }
            });
        },
        function (err, n) 
        {
            done(err);
        }      
      );
    });

    it('batch moved folder tree contents are correct', function(done) {
      this.timeout(_testTimeout * files.length); 
      async.eachSeries(files, function(entry, callback)
      {
        if (entry.file)
        {
          request(server)
            .post('/2/files/download')
            .set('Accept', 'application/octet-stream')
            .set('Authorization', "Bearer " + testToken)
            .send({ "path": "/testfolder1/" + entry.file })
            .set('Dropbox-API-Arg', '{ "path": "/testfolder1' + entry.file + '" }')
            .expect('Content-Type', 'text/plain')
            .expect(function(res){
                 assert.equal(res.text, entry.contents); 
            })
            .expect(200, callback);
        }
        else
        {
          request(server)
            .post('/2/files/get_metadata')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .send({ 'path': '/testfolder1' + entry.folder })
            .expect(function(res){
                assert(res.body);
                assert.equal(res.body[".tag"], "folder");
                assert.equal(res.body["path_display"], "/testfolder1" + entry.folder);
            })
            .expect(200, callback);
        }
      },
      function(err)
      {
          log.error("Err:", err);
          done(err);
      });
    });

    it('source folder no longer present after batch move', function(done) {
      request(server)
        .post('/2/files/get_metadata')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "/testfolder" })
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body.error_summary, 'path/not_found');
        })
        .expect(409, done);
    });

    it('batch job is no longer valid after complete', function(done) {
      request(server)
        .post('/2/files/move_batch/check')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ "async_job_id": async_job_id })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body.error_summary, 'invalid_async_job_id/...');
        })
        .expect(409, done);
    });

    it('succeeds in starting batch move that will fail', function(done) {
      request(server)
        .post('/2/files/move_batch')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ entries: [ { from_path: "/nonexistantfolder", to_path: "/testfolder1" } ] })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body[".tag"], 'async_job_id');
            async_job_id = res.body["async_job_id"];
            log.info("Got job id:", async_job_id);
        })
        .expect(200, done);
    });

    it('gets "failed" from batch move job that failed', function(done) {
      var complete = false;
      async.whilst(
        function() 
        { 
          return !complete;
        },
        function(callback) 
        {
          request(server)
            .post('/2/files/move_batch/check')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .send({ "async_job_id": async_job_id })
            .expect('Content-Type', /json/)
            .expect(function(res){
                assert(res.body);
                if (res.body[".tag"] === "failed")
                {
                  log.info("Batch move failed");
                  complete = true;
                }
                else
                {
                  // If not complete, anything other than in_progress is an error
                  assert.equal(res.body[".tag"], 'in_progress');
                }
            })
            .expect(200, function(err)
            {
              // We're going to wait after (even on complete) in order to give the job file
              // time to get delete (in Manta there can be a slight delay, which causes the
              // subsequent test to fail).
              //
              setTimeout(function(){ callback(err) }, 1000);
            });
        },
        function (err, n) 
        {
            done(err);
        }      
      );
    });

    it('batch job is no longer valid after failed', function(done) {
      request(server)
        .post('/2/files/move_batch/check')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ "async_job_id": async_job_id })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body.error_summary, 'invalid_async_job_id/...');
        })
        .expect(409, done);
    });

    it('succeeds in copying folder tree', function(done) {
      request(server)
        .post('/2/files/copy_batch')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ "entries": [ { from_path: "/testfolder1", to_path: "/testfolder2" } ] })
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body[".tag"], 'async_job_id');
            async_job_id = res.body["async_job_id"];
            log.info("Got job id:", async_job_id);
        })
        .expect(200, done);
    });

    it('gets "complete" from batch copy folder tree job', function(done) {
      this.timeout(_testTimeout * files.length); 
      var complete = false;
      async.whilst(
        function() 
        { 
          return !complete;
        },
        function(callback) 
        {
          request(server)
            .post('/2/files/copy_batch/check')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .send({ "async_job_id": async_job_id })
            .expect('Content-Type', /json/)
            .expect(function(res){
                assert(res.body);
                if (res.body[".tag"] === "complete")
                {
                  log.info("Batch move complete");
                  complete = true;
                }
                else
                {
                  // If not complete, anything other than in_progress is an error
                  assert.equal(res.body[".tag"], 'in_progress');
                }
            })
            .expect(200, function(err)
            {
              if (err || complete)
              {
                callback(err);
              }
              else
              {
                // If we're not done, wait before retrying
                setTimeout(callback, 1000);
              }
            });
        },
        function (err, n) 
        {
            done(err);
        }      
      );
    });

    it('batch copied folder tree contents are correct', function(done) {
      async.eachSeries(files, function(entry, callback)
      {
        if (entry.file)
        {
          request(server)
            .post('/2/files/download')
            .set('Accept', 'application/octet-stream')
            .set('Authorization', "Bearer " + testToken)
            .send({ "path": "/testfolder2/" + entry.file })
            .set('Dropbox-API-Arg', '{ "path": "/testfolder2' + entry.file + '" }')
            .expect('Content-Type', 'text/plain')
            .expect(function(res){
                 assert.equal(res.text, entry.contents); 
            })
            .expect(200, callback);
        }
        else
        {
          request(server)
            .post('/2/files/get_metadata')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .send({ 'path': '/testfolder2' + entry.folder })
            .expect(function(res){
                assert(res.body);
                assert.equal(res.body[".tag"], "folder");
                assert.equal(res.body["path_display"], "/testfolder2" + entry.folder);
            })
            .expect(200, callback);
        }
      },
      function(err)
      {
          log.error("Err:", err);
          done(err);
      });
    });

    it('source of batch copied folder tree contents unchanged', function(done) {
      async.eachSeries(files, function(entry, callback)
      {
        if (entry.file)
        {
          request(server)
            .post('/2/files/download')
            .set('Accept', 'application/octet-stream')
            .set('Authorization', "Bearer " + testToken)
            .send({ "path": "/testfolder1/" + entry.file })
            .set('Dropbox-API-Arg', '{ "path": "/testfolder1' + entry.file + '" }')
            .expect('Content-Type', 'text/plain')
            .expect(function(res){
                 assert.equal(res.text, entry.contents); 
            })
            .expect(200, callback);
        }
        else
        {
          request(server)
            .post('/2/files/get_metadata')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .send({ 'path': '/testfolder1' + entry.folder })
            .expect(function(res){
                assert(res.body);
                assert.equal(res.body[".tag"], "folder");
                assert.equal(res.body["path_display"], "/testfolder1" + entry.folder);
            })
            .expect(200, callback);
        }
      },
      function(err)
      {
          log.error("Err:", err);
          done(err);
      });
    });

    it('succeeds in batch deleting folder tree', function(done) {
      request(server)
        .post('/2/files/delete_batch')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ "entries": [ { path: "/testfolder1" } ] })
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body[".tag"], 'async_job_id');
            async_job_id = res.body["async_job_id"];
            log.info("Got job id:", async_job_id);
        })
        .expect(200, done);
    });

    it('gets "complete" from batch delete folder tree job', function(done) {
      this.timeout(_testTimeout * files.length); 
      var complete = false;
      async.whilst(
        function() 
        { 
          return !complete;
        },
        function(callback) 
        {
          request(server)
            .post('/2/files/delete_batch/check')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .send({ "async_job_id": async_job_id })
            .expect('Content-Type', /json/)
            .expect(function(res){
                assert(res.body);
                if (res.body[".tag"] === "complete")
                {
                  log.info("Batch delete complete");
                  complete = true;
                }
                else
                {
                  // If not complete, anything other than in_progress is an error
                  assert.equal(res.body[".tag"], 'in_progress');
                }
            })
            .expect(200, function(err)
            {
              if (err || complete)
              {
                callback(err);
              }
              else
              {
                // If we're not done, wait before retrying
                setTimeout(callback, 1000);
              }
            });
        },
        function (err, n) 
        {
            done(err);
        }
      );
    });

    it('batch deleted folder tree not present', function(done) {
      request(server)
        .post('/2/files/get_metadata')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "/testfolder1" })
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body.error_summary, 'path/not_found');
        })
        .expect(409, done);
    });

    after("Cleanup", function(done){
      request(server)
        .post('/2/files/delete')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "/testfolder2" })
        .expect(200, done);
    });
  });

  describe('Multipart upload', function() {
    var uploadId;
    it('succeeds in starting upload session', function(done) {
      request(server)
        .post('/2/files/upload_session/start')
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
        .post('/2/files/upload_session/append')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ "session_id": "' + uploadId + '", "offset": 15 }')
        .send('Bar is the next word')
        .expect(200, done); // !!! Verify not content returned (no c/t?)
    });
    it('succeeds in appending second part using append_v2', function(done) {
      request(server)
        .post('/2/files/upload_session/append_v2')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ "cursor": { "session_id": "' + uploadId + '", "offset": 35 } }')
        .send('Baz is the third word')
        .expect(200, done);  // !!! Verify not content returned (no c/t?)
    });
    it('succeeds in finishing upload', function(done) {
      this.timeout(_testTimeout * 3); 
      request(server)
        .post('/2/files/upload_session/finish')
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
        .post('/2/files/download')
        .set('Accept', 'application/octet-stream')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ "path": "target.txt" }')
        .expect('Content-Type', 'text/plain')
        .expect(function(res){
             assert.equal(res.text, 'Foo is the wordBar is the next wordBaz is the third wordFraz is the final word'); 
        })
        .expect(200, done);
    });
    it('succeeds in deleting uploaded file', function(done) {
      request(server)
        .post('/2/files/delete')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "target.txt" })
        .expect(200, done);
    });
  });

  describe('Batch multipart upload', function() {
    var uploadId1;
    var uploadId2;
    var async_job_id;

    it('succeeds in starting upload session (file 1)', function(done) {
      request(server)
        .post('/2/files/upload_session/start')
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
            uploadId1 = res.body.session_id;
        })
        .expect(200, done);
    });
    it('succeeds in appending first part using append (file 1)', function(done) {
      request(server)
        .post('/2/files/upload_session/append_v2')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ "cursor": { "session_id": "' + uploadId1 + '", "offset": 15 }, "close": true }')
        .send('Bar is the next word')
        .expect(200, done);
    });
    it('succeeds in starting upload session (file 2)', function(done) {
      request(server)
        .post('/2/files/upload_session/start')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ }')
        .send('Car is the word')
        .expect('Content-Type', /json/)
        .expect(function(res){
            assert(res.body);
            assert(res.body.session_id); 
        })
        .expect(function(res) {
            uploadId2 = res.body.session_id;
        })
        .expect(200, done);
    });
    it('succeeds in appending first part using append (file 2)', function(done) {
      request(server)
        .post('/2/files/upload_session/append_v2')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ "cursor": { "session_id": "' + uploadId2 + '", "offset": 15 }, "close": true }')
        .send('Zoo is the next word')
        .expect(200, done);
    });
    it('succeeds in upload session finish batch', function(done) {
      var entries = [
        { cursor: { session_id: uploadId1, offset: 35 }, commit: { path: "test/file1.txt" } },
        { cursor: { session_id: uploadId2, offset: 35 }, commit: { path: "test/file2.txt" } },
      ];
      request(server)
        .post('/2/files/upload_session/finish_batch')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ "entries": entries })
        .expect(function(res){
            assert(res.body);
            assert.equal(res.body[".tag"], 'async_job_id');
            async_job_id = res.body["async_job_id"];
            log.info("Got job id:", async_job_id);
        })
        .expect(200, done);
    });

    it('gets "complete" from upload session finish batch job', function(done) {
      this.timeout(_testTimeout * 2); 
      var complete = false;
      async.whilst(
        function() 
        { 
          return !complete;
        },
        function(callback) 
        {
          request(server)
            .post('/2/files/upload_session/finish_batch/check')
            .set('Accept', 'application/json')
            .set('Authorization', "Bearer " + testToken)
            .send({ "async_job_id": async_job_id })
            .expect('Content-Type', /json/)
            .expect(function(res){
                assert(res.body);
                if (res.body[".tag"] === "complete")
                {
                  log.info("Batch delete complete");
                  complete = true;
                }
                else
                {
                  // If not complete, anything other than in_progress is an error
                  assert.equal(res.body[".tag"], 'in_progress');
                }
            })
            .expect(200, function(err)
            {
              if (err || complete)
              {
                callback(err);
              }
              else
              {
                // If we're not done, wait before retrying
                setTimeout(callback, 1000);
              }
            });
        },
        function (err, n) 
        {
            done(err);
        }
      );
    });
    it('uploaded file (file 1) has correct contents', function(done) {
      request(server)
        .post('/2/files/download')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ "path": "test/file1.txt" }')
        .expect('Content-Type', 'text/plain')
        .expect(function(res){
             assert.equal(res.text, 'Foo is the wordBar is the next word'); 
        })
        .expect(200, done);
    });
    it('uploaded file (file 2) has correct contents', function(done) {
      request(server)
        .post('/2/files/download')
        .set('Authorization', "Bearer " + testToken)
        .set('Dropbox-API-Arg', '{ "path": "test/file2.txt" }')
        .expect('Content-Type', 'text/plain')
        .expect(function(res){
             assert.equal(res.text, 'Car is the wordZoo is the next word'); 
        })
        .expect(200, done);
    });
    after('delete uploaded files', function(done) {
      request(server)
        .post('/2/files/delete')
        .set('Accept', 'application/json')
        .set('Authorization', "Bearer " + testToken)
        .send({ path: "test" })
        .expect(200, done);
    });
  });
});
