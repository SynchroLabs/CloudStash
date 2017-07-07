var request = require('supertest');
var assert = require('assert');

var async = require('async');

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

describe('/files/get_metadata', function() {
  it('succeeds for existing folder', function(done) {
    request(server)
      .post('/files/get_metadata')
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
  it('succeeds for existing file', function(done) {
    request(server)
      .post('/files/get_metadata')
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
  it('fails for non-existant object', function(done) {
    request(server)
      .post('/files/get_metadata')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "flarf" }')
      .expect('Content-Type', /json/)
      .expect(function(res){
          assert(res.body);
          assert(res.body.error);
          assert.equal(res.body.error[".tag"], 'path'); 
          assert.equal(res.body.error.path[".tag"], 'not_found'); 
      })
      .expect(409, done);
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

describe("list folder and friends", function() {
  before("Create folder contents", function(done)
  {
    // We want to create the files in a specific order so they will come back sorted by mtime propertly.  However, on some
    // file systems (MacOS), the granularity of the file mtime is one second.  So if we just create these files in order,
    // all (or most) of them will have the same mtime, and thus be in an unpredictable sort order (files within the same mtime
    // will be sorted by name, but we can't guarantee that all of the files will be in the same mtime).
    //
    // To get around this, we introduce a delay befween each operation to make sure that every file/dir is created in its own
    // millisecond, this producing a predictable result order.
    //
    var intervalMs = 1050;
    this.timeout(2000 + (intervalMs*5)); // This keeps Mocha from timing out the function in the default 2000ms.
    async.series(
    [
      function(callback) 
      {
        request(server)
          .post('/files/upload')
          .set('Accept', 'application/json')
          .set('Authorization', "Bearer " + testToken)
          .set('Dropbox-API-Arg', '{ "path": "one.txt" }')
          .send('This is file one.txt')
          .expect(200, callback);
      },
      function(callback)
      {
        setTimeout(callback, intervalMs);
      },
      function(callback) 
      {
        request(server)
          .post('/files/upload')
          .set('Accept', 'application/json')
          .set('Authorization', "Bearer " + testToken)
          .set('Dropbox-API-Arg', '{ "path": "two.txt" }')
          .send('This is file two.txt')
          .expect(200, callback);
      },
      function(callback)
      {
        setTimeout(callback, intervalMs);
      },
      function(callback)
      {
        request(server)
          .post('/files/create_folder')
          .set('Accept', 'application/json')
          .set('Authorization', "Bearer " + testToken)
          .set('Dropbox-API-Arg', '{ "path": "subfolder" }')
          .expect(200, callback);
      },
      function(callback)
      {
        setTimeout(callback, intervalMs);
      },
      function(callback) 
      {
        log.info("three");
        request(server)
          .post('/files/upload')
          .set('Accept', 'application/json')
          .set('Authorization', "Bearer " + testToken)
          .set('Dropbox-API-Arg', '{ "path": "subfolder/three.txt" }')
          .send('This is file three.txt')
          .expect(200, callback);
      },
      function(callback)
      {
        setTimeout(callback, intervalMs);
      },
      function(callback) 
      {
        log.info("four");
        request(server)
          .post('/files/upload')
          .set('Accept', 'application/json')
          .set('Authorization', "Bearer " + testToken)
          .set('Dropbox-API-Arg', '{ "path": "subfolder/four.txt" }')
          .send('This is file four.txt')
          .expect(200, callback);
      },
      function(callback)
      {
        setTimeout(callback, intervalMs);
      },
      function(callback) 
      {
        log.info("five");
        request(server)
          .post('/files/upload')
          .set('Accept', 'application/json')
          .set('Authorization', "Bearer " + testToken)
          .set('Dropbox-API-Arg', '{ "path": "five.txt" }')
          .send('This is file five.txt')
          .expect(200, callback);
      },
    ],
    function(err, results) 
    {
      if (err)
      {
        log.error(err);
      }
      done();
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
      .post('/files/list_folder')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "" }')
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
      .post('/files/list_folder')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "", "recursive": true }')
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
      .post('/files/list_folder')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "", "recursive": true, "limit": 3 }')
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
      .post('/files/list_folder/continue')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "cursor": "' + cursor + '" }')
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
      .post('/files/list_folder/continue')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "cursor": "' + cursor + '" }')
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
          .post('/files/upload')
          .set('Accept', 'application/json')
          .set('Authorization', "Bearer " + testToken)
          .set('Dropbox-API-Arg', '{ "path": "six.txt" }')
          .send('This is file six.txt')
          .expect(200, callback);
      },
      function(callback)
      {
        request(server)
          .post('/files/list_folder/continue')
          .set('Accept', 'application/json')
          .set('Authorization', "Bearer " + testToken)
          .set('Dropbox-API-Arg', '{ "cursor": "' + cursor + '" }')
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
      done();
    });
  });

  it('list_folder/get_latest_cursor succeeds', function(done) {
    request(server)
      .post('/files/list_folder/get_latest_cursor')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "path": "", "recursive": true }')
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
      .post('/files/list_folder/continue')
      .set('Accept', 'application/json')
      .set('Authorization', "Bearer " + testToken)
      .set('Dropbox-API-Arg', '{ "cursor": "' + cursor + '" }')
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

  it('list_folder/continue returns file added after get_latest_cursor', function(done) {
    async.series(
    [
      function(callback) 
      {
        request(server)
          .post('/files/upload')
          .set('Accept', 'application/json')
          .set('Authorization', "Bearer " + testToken)
          .set('Dropbox-API-Arg', '{ "path": "seven.txt" }')
          .send('This is file six.txt')
          .expect(200, callback);
      },
      function(callback)
      {
        request(server)
          .post('/files/list_folder/continue')
          .set('Accept', 'application/json')
          .set('Authorization', "Bearer " + testToken)
          .set('Dropbox-API-Arg', '{ "cursor": "' + cursor + '" }')
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
      done();
    });
  });

  //
  // !!! Long poll - list_folder/get_latest_cursor, list_folder/longpoll - this one might be tricky to test
  //

  after("Clean up folder contents", function(done)
  {
    async.series(
    [
      function(callback) 
      {
        request(server)
          .post('/files/delete')
          .set('Accept', 'application/json')
          .set('Authorization', "Bearer " + testToken)
          .set('Dropbox-API-Arg', '{ "path": "seven.txt" }')
          .expect(200, callback);
      },
      function(callback) 
      {
        request(server)
          .post('/files/delete')
          .set('Accept', 'application/json')
          .set('Authorization', "Bearer " + testToken)
          .set('Dropbox-API-Arg', '{ "path": "six.txt" }')
          .expect(200, callback);
      },
      function(callback) 
      {
        request(server)
          .post('/files/delete')
          .set('Accept', 'application/json')
          .set('Authorization', "Bearer " + testToken)
          .set('Dropbox-API-Arg', '{ "path": "five.txt" }')
          .expect(200, callback);
      },
      function(callback) 
      {
        request(server)
          .post('/files/delete')
          .set('Accept', 'application/json')
          .set('Authorization', "Bearer " + testToken)
          .set('Dropbox-API-Arg', '{ "path": "subfolder/four.txt" }')
          .expect(200, callback);
      },
      function(callback) 
      {
        request(server)
          .post('/files/delete')
          .set('Accept', 'application/json')
          .set('Authorization', "Bearer " + testToken)
          .set('Dropbox-API-Arg', '{ "path": "subfolder/three.txt" }')
          .expect(200, callback);
      },
      function(callback) 
      {
        request(server)
          .post('/files/delete')
          .set('Accept', 'application/json')
          .set('Authorization', "Bearer " + testToken)
          .set('Dropbox-API-Arg', '{ "path": "subfolder" }')
          .expect(200, callback);
      },
      function(callback) 
      {
        request(server)
          .post('/files/delete')
          .set('Accept', 'application/json')
          .set('Authorization', "Bearer " + testToken)
          .set('Dropbox-API-Arg', '{ "path": "two.txt" }')
          .expect(200, callback);
      },
      function(callback) 
      {
        request(server)
          .post('/files/delete')
          .set('Accept', 'application/json')
          .set('Authorization', "Bearer " + testToken)
          .set('Dropbox-API-Arg', '{ "path": "one.txt" }')
          .expect(200, callback);
      }
    ],
    function(err, results) 
    {
      if (err)
      {
        log.error(err);
      }
      done();
    });
  });
});

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
