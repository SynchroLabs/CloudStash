var stream = require('stream');
var csUtil = require('./util');

// This module provides helper methods to interact with files and folders at the root of the user storage, as 
// opposed to in app storage for the user.
//
module.exports = function(config, driver)
{
    var log = require('./logger').getLogger("root");

    function stripAppId(user)
    {
        return { account_id: user.account_id };
    }

    var bulk;
    var bridge;

    var root = 
    {
        setBulk: function(theBulk)
        {
            bulk = theBulk;
        },
        setBridge: function(theBridge)
        {
            bridge = theBridge;
        },
        putObject: function(user, path, content, callback)
        {
            var contentString = content;
            if (typeof content === 'object')
            {
                contentString = JSON.stringify(content, null, 4);
            }

            bridge.putObject(stripAppId(user), path, function(err, writeStream)
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    var readStream = new stream.Readable();
                    readStream.push(contentString);
                    readStream.push(null);

                    csUtil.pipeRequest(readStream, writeStream, function(err, details)
                    {
                        callback(err);
                    });
                }
            });
        },
        putObjectStream: function(user, path, callback)
        {
            bridge.putObject(stripAppId(user), path, callback);
        },
        getObject: function(user, path, callback)
        {
            bridge.getObject(stripAppId(user), path, function(err, stream)
            {
                if (err)
                {
                    callback(err);
                }
                else if (stream)
                {
                    var chunks = [];

                    stream.on("data", function (chunk)
                    {
                        chunks.push(chunk);
                    });

                    stream.on("end", function () 
                    {
                        callback(null, JSON.parse(Buffer.concat(chunks)));
                    });

                    stream.once('error', function(err)
                    {
                        callback(err);
                    });
                }
                else
                {
                    // Object didn't exist
                    callback(null);
                }
            });
        },
        getObjectStream: function(user, path, callback)
        {
            bridge.getObject(stripAppId(user), path, callback);
        },
        deleteObject: function(user, path, callback)
        {
            bridge.deleteObject(stripAppId(user), path, function(err)
            {
                callback(err);
            });
        },
        createDirectory: function(user, path, callback)
        {
            bridge.createDirectory(stripAppId(user), path, function(err)
            {
                callback(err);
            });
        },
        deleteDirectory: function(user, path, callback)
        {
            var req = { user: stripAppId(user), log: log };
            var srcEntry = { ".tag": "folder", path_display: path };
            bulk.doOperation(req, "delete", [{ srcEntry: srcEntry }], function(err)
            {
                log.info("Account directory deleted:", path);
                callback(err);
            });
        },
        getDirectoryEntries: function(user, path, callback)
        {
            var entries = [];

            function onEntry(entry)
            {
                entries.push(entry);
            }

            bridge.traverseDirectory(stripAppId(user), path, false, onEntry, function(err, stopped)
            {
                callback(err, entries);
            });
        }
    }

    return root;
}