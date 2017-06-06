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
        url: params.url
    });

    log.debug('Manta client setup: %s', client.toString());

    var driver = 
    {
        provider: "manta",
        getObject: function(filename, callback)
        {
            // path.posix.normalize will move any ../ to the front, and the regex will remove them.
            //
            var safeFilenamePath = path.posix.normalize(filename).replace(/^(\.\.[\/\\])+/, '');
            var filePath = path.posix.join(basePath, safeFilenamePath); 

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
        listDirectory: function(dirpath, callback)
        {
            // path.posix.normalize will move any ../ to the front, and the regex will remove them.
            //
            var safeFilenamePath = path.posix.normalize(dirpath).replace(/^(\.\.[\/\\])+/, '');
            var fullPath = path.posix.join(basePath, safeFilenamePath); 

            log.info("safeFilenamePath", fullPath);

            var options = {};

            client.ls(fullPath, options, function(err, res)
            {
                var list = [];

                res.on('object', function (obj) {
                    log.info("file", obj);
                    list.push(obj);
                });

                res.on('directory', function (dir) {
                    log.info("dir", dir);
                    list.push(dir);
                });

                res.once('error', function (err) {
                    log.error(err);
                    callback(err);
                });

                res.once('end', function () {
                    callback(null, list);
                });
            });
        },        
        putObject: function(filename, callback)
        {
            // path.posix.normalize will move any ../ to the front, and the regex will remove them.
            //
            var safeFilenamePath = path.posix.normalize(filename).replace(/^(\.\.[\/\\])+/, '');
            var filePath = path.posix.join(basePath, safeFilenamePath); 
            
            // !!! May need to create parent dirs if they don't exist
            // !!! Do we have to do anything special to overwrite existing file?

            var options = {};

            callback(null, client.createWriteStream(filePath, options));
        },
    }

    return driver;
}

