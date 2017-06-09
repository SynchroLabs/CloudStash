// Manta API
//
// https://apidocs.joyent.com/manta/
// https://apidocs.joyent.com/manta/nodesdk.html
// https://github.com/joyent/node-manta
//
// Example config:
//
// Using a keystore file (ssh key file):
//
// {
//   "mount": "/foo/bar",
//   "provider": "manta",
//   "basePath": "~~/stor/",
//   "url": "https://us-east.manta.joyent.com",
//   "user": "user@domain.com"
//   "keyId": "8c:09:65:e3:8c:09:65:e3:8c:09:65:e3:8c:09:65:e3",
//   "keyStore": "/Users/you/.ssh/joyent_id_rsa"
// }
//
// Specifying the key explicitly in config (using contents of ssh key file):
//
// {
//   "mount": "/foo/bar",
//   "provider": "manta",
//   "basePath": "~~/stor/",
//   "url": "https://us-east.manta.joyent.com",
//   "user": "user@domain.com"
//   "keyId": "8c:09:65:e3:8c:09:65:e3:8c:09:65:e3:8c:09:65:e3",
//   "key": "-----BEGIN RSA PRIVATE KEY-----\nLOTS-OF-KEY-DATA-HERE==\n-----END RSA PRIVATE KEY-----"
// }
//
var log = require('./../lib/logger').getLogger("manta-driver");

var fs = require('fs');
var path = require('path');

var manta = require('manta');

function getEntryDetails(mantaEntry)
{
    // !!! Convert to Dropbox form
    //
    return mantaEntry;
}

module.exports = function(params)
{
    var basePath = params.basePath;

    log.debug("Using Manta store, basePath:", basePath);

    // key is "key" if provided, else from "keyStore" file.
    //
    if (params.key64)
    {
        params.key = new Buffer(params.key64, 'base64').toString();
    }
    
    var key = params.key || fs.readFileSync(params.keyStore, 'utf8'); 

    var client = manta.createClient({
        sign: manta.privateKeySigner({
            key: key,
            keyId: params.keyId,
            user: params.user
        }),
        user: params.user,
        url: params.url,
        log: log
    });

    function toSafeLocalPath(fileName)
    {
        // path.posix.normalize will move any ../ to the front, and the regex will remove them.
        //
        var safeFilename = path.posix.normalize(fileName).replace(/^(\.\.[\/\\])+/, '');
        var filePath = path.posix.join(basePath, safeFilename); 

        return filePath;
    }

    log.debug('Manta client setup: %s', client.toString());

    var driver = 
    {
        provider: "manta",
        createDirectory: function(dirPath, callback)
        {
            var fullPath = toSafeLocalPath(dirPath); 

            client.mkdirp(fullPath, function(err)
            {
                // !!! Better entry details?  (query existing dir?)
                //
                if (err) 
                {
                    callback(err);
                }
                else 
                {
                    var entry = { type: "directory", file: dirPath };
                    callback(null, getEntryDetails(entry));
                }
            });
        },
        listDirectory: function(dirpath, callback)
        {
            var fullPath = toSafeLocalPath(dirpath);

            var options = {};

            client.ls(fullPath, options, function(err, res)
            {
                var entries = [];

                res.on('object', function (obj) 
                {
                    log.info("file", obj);
                    entries.push(getEntryDetails(obj));
                });

                res.on('directory', function (dir) 
                {
                    log.info("dir", dir);
                    entries.push(getEntryDetails(dir));
                });

                res.once('error', function (err) 
                {
                    log.error(err);
                    callback(err);
                });

                res.once('end', function () 
                {
                    callback(null, entries);
                });
            });
        },
        getObject: function(filename, callback)
        {
            var filePath = toSafeLocalPath(filename);

            client.get(filePath, function(err, stream) 
            {
                if (err)
                {
                    if (err.code == 'ResourceNotFound')
                    {
                        // Return null - file doesn't exist
                        callback(null, null);
                    }
                    else
                    {
                        log.error(err);
                        callback(err);
                    }
                }

                callback(null, stream);
            });
        },
        putObject: function(filename, callback)
        {
            var filePath = toSafeLocalPath(filename);

            // !!! May need to create parent dirs if they don't exist
            // !!! Do we have to do anything special to overwrite existing file?

            var options = {};

            callback(null, client.createWriteStream(filePath, options));
        },
        deleteObject: function(filename, callback)
        {
            // This will remove a file or a directory, so let's hope it's used correctly
            //
            var filePath = toSafeLocalPath(filename);

            client.info(filePath, function(err, info) 
            {
                if (err) 
                {
                    callback(err);
                }
                else 
                {
                    log.info("Got entry info on delete:", info);

                    var entry = { name: filename };
                    entry.type = (info.extension == "directory") ? "directory" : "object";

                    client.unlink(filePath, function(err)
                    {
                        if (err) 
                        {
                            callback(err);
                        }
                        else 
                        {
                            callback(null, getEntryDetails(entry));
                        }
                    });
                }
            });
        }
    }

    return driver;
}
