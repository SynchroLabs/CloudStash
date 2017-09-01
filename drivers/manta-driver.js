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
// ----
//
// REVIEW:
//
// !!! We do mkdrip on object write (put/copy/move).  We could be more clever and try the put before doing the mkdrip, 
//     catching any DirectoryDoesNotExistError, and in only that case do the mkdirp and retry the put.
//
// ----
//
// Multipart uploads:
//
//    https://github.com/joyent/node-manta/blob/master/lib/client.js
//    https://github.com/joyent/manta-muskie/blob/master/lib/uploads/common.js
//
// ----
//
// !!! When doing Manta get using If-Modified-Since or If-None-Match and the return is a 304, the content is empty and the
//     content-type is application/octet-stream.  Likewise, when doing Manta get with a Range request, the the content-type\
//     is application/octet-stream.  One way of looking at this is that the content-type describes the content type of the
//     response, not the underlying object.  In that view, in the former case, the content-type is irrelevant, as there is no
//     content.  And in the latter case, since it is a byte stream, application/octet-stream kind of makes sense (I guess), though
//     there are a bunch of examples online where the underlying object content type is used (which is how the file driver does it).
//
var log = require('./../lib/logger').getLogger("manta-driver");

var fs = require('fs');
var path = require('path');
var async = require('async');

var lodash = require('lodash');
var mimeTypes = require('mime-types');

var manta = require('manta');

module.exports = function(params, config)
{
    var basePath = params.basePath;

    log.debug("Using Manta store, basePath:", basePath);

    var maxConcurrency = config.get('MAX_CONCURRENCY');

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

    function toSafePath(filePath)
    {
        // path.posix.normalize will move any ../ to the front, and the regex will remove them.
        //
        var safePath = path.posix.normalize(filePath).replace(/^(\.\.[\/\\])+/, '');

        if (path.sep != '/') // !!! WTF?
        {
            // Replace forward slash with local platform seperator
            //
            safePath = filePath.replace(/[\/]/g, path.sep);
        }

        return safePath;
    }

    function toSafeLocalPath(account_id, app_id, filePath)
    {
        // !!! This forms a path of basePath/account_id/app_id/xxxxx - For scale, we assume the account_id
        //     is a GUID (randomly distributed digits).  In order to keep directories from getting too large, 
        //     we can break down the path further using the first three pairs of characters from the GUID, for
        //     a path like: basePath/AB/CD/EF/GHIJKLxxx/app_id/xxxxx.  In that model, with 100m users acounts,
        //     the first two levels of directories will be "full" (256 entries), and the third level will contain
        //     an average of 6 accounts.
        //
        if (app_id)
        {
            return path.posix.join(basePath, account_id, app_id, toSafePath(filePath)); 
        }
        else
        {
            return path.posix.join(basePath, account_id, toSafePath(filePath));
        }
    }

    function getEntryDetails(user, mantaEntry)
    {
        // Manta object
        /*
        { 
            name: 'baz.txt',
            etag: '6eaac329-47be-c6d3-de6b-80897012f60d',
            size: 15,
            type: 'object',
            mtime: '2017-06-26T07:42:11.339Z',
            durability: 2,
            parent: '/synchro/stor/mantabox/1234-BEEF/TEST01' 
        }
        */

        // Manta directory
        /*
        { 
            name: 'test_folder',
            type: 'directory',
            mtime: '2017-06-26T07:42:04.057Z',
            parent: '/synchro/stor/mantabox/1234-BEEF/TEST01' 
        }
        */

        var fullpath = path.posix.join(mantaEntry.parent, mantaEntry.name);

        var userPath = path.posix.join(basePath, user.account_id);
        if (user.app_id)
        {
            userPath = path.posix.join(userPath, user.app_id);
        } 

        var displayPath = "/" + path.relative(userPath, fullpath);

        // Convert to Dropbox form
        //
        var item = { };
        item[".tag"] = (mantaEntry.type == "object") ? "file" : "folder";
        item["name"] = mantaEntry.name;

        item["path_lower"] = displayPath.toLowerCase();
        item["path_display"] = displayPath;
        item["id"] = displayPath; // !!! Required by Dropbox - String(min_length=1)

        item["server_modified"] = mantaEntry.mtime.replace(/\.\d{3}/, ''); // !!! Remove ms for Dropbox
        item["client_modified"] = item["server_modified"]; // !!! Required by Dropbox

        item["rev"] = "000000001"; // !!! Required by Dropbox - String(min_length=9, pattern="[0-9a-f]+")
        if (mantaEntry.size)
        {
            // Not present on directory entry
            item["size"] = mantaEntry.size;
        }
        // item["content_hash"]

        return item;
    }

    log.debug('Manta client setup: %s', client.toString());

    var driver = 
    {
        provider: "manta",
        createDirectory: function(user, dirPath, callback)
        {
            var fullPath = toSafeLocalPath(user.account_id, user.app_id, dirPath); 

            client.mkdirp(fullPath, function(err)
            {
                callback(err);
            });
        },
        traverseDirectory: function(user, dirPath, recursive, onEntry, callback)
        {
            var stopped = false;

            var q = async.queue(function(task, done) 
            {
                var fullPath = toSafeLocalPath(user.account_id, user.app_id, task.dirpath);

                // !!! It appears from the source code that client.ls will make multiple underlying REST API
                //     calls and page through the entries without us having to do anything special.
                //
                //         https://github.com/joyent/node-manta/blob/master/lib/client.js
                //
                var options = {};
                client.ls(fullPath, options, function(err, res)
                {
                    if (err)
                    {
                        if ((err.name == 'NotFoundError') && (dirPath == ''))
                        {
                            // If the error is 'not found' and the dir in question is the root dir, we're just
                            // going to ignore that treat it like an empty dir lising (it just means we haven't
                            // created this user/app path yet because it hasn't been used yet).
                            //
                            done();
                        }
                        else
                        {
                            done(err);
                        }
                    }
                    else
                    {
                        res.on('entry', function(item)
                        {
                            if (!stopped)
                            {
                                log.info("Traverse got item:", item);
                                var entry = getEntryDetails(user, item);
                                log.debug("Entry", entry);

                                if (onEntry(entry))
                                {
                                    // !!! It would be great if we could tell Manta to stop (since we're done)
                                    //
                                    stopped = true;
                                    done();
                                }
                                else if (recursive && (entry[".tag"] == "folder"))
                                {
                                    q.push({ dirpath: path.posix.join(task.dirpath, entry.name) });
                                }
                            }
                        });

                        res.once('error', function (err) 
                        {
                            log.error(err);
                            done(err);
                        });

                        res.once('end', function () 
                        {
                            if (!stopped)
                            {
                                done();
                            }
                        });
                    }
                });
            }, maxConcurrency);

            q.error = lodash.once(function(err, task)
            {
                q.kill();
                callback(err);
            });

            q.drain = function() 
            {
                callback(null, stopped);
            };

            q.push({ dirpath: dirPath });
        },
        getObject: function(user, filename, requestHeaders, callback)
        {
            // requestHeaders is optional
            //
            if (typeof callback === 'undefined')
            {
                callback = requestHeaders;
                requestHeaders = null;
            }

            var filePath = toSafeLocalPath(user.account_id, user.app_id, filename);

            if (requestHeaders)
            {
                log.info("getObject got headers:", requestHeaders);
            }

            var options = { headers: lodash.pick(requestHeaders, ['if-modified-since', 'if-none-match', 'if-match', 'range']) };

            log.info("Calling client.get with options:", options);

            client.get(filePath, options, function(err, stream, res)
            {
                if (err)
                {
                    if (err.code == 'ResourceNotFound')
                    {
                        callback(null, null, 404, 'Not Found');
                    }
                    else if (err.code === 'RequestedRangeNotSatisfiableError')
                    {
                        callback(null, null, 416, "Range Not Satisfiable");
                    }
                    else if (options.headers.range && (err.code === 'PreconditionFailed') && (err.message.indexOf('if-match') !== -1))
                    {
                        // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-Match
                        //
                        // When the If-Match precondition fails when doing a Range request, the correct response is...
                        //
                        callback(null, null, 416, "Range Not Satisfiable");
                    }
                    else
                    {
                        log.error(err);
                        callback(err);
                    }
                }
                else
                {
                    callback(null, stream, res.statusCode, res.statusMessage, res.headers);
                }
            });
        },
        putObject: function(user, filename, readStream, callback)
        {
            var filePath = toSafeLocalPath(user.account_id, user.app_id, filename);

            client.mkdirp(path.dirname(filePath), function(err)
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    // Set the Content-Type from the filename
                    log.info("Setting mime type to:", mimeTypes.lookup(filePath));

                    var options = { type: mimeTypes.lookup(filePath) || 'application/octet-stream' };
                    var writeStream = client.createWriteStream(filePath, options);

                    var errorSent = false;

                    function onError(err)
                    {
                        if (!errorSent)
                        {
                            errorSent = true;
                            readStream.unpipe();
                            writeStream.end();
                            callback(err);
                        }
                    }

                    readStream.once('error', onError);
                    writeStream.once('error', onError);

                    writeStream.once('close', function(details) 
                    {
                        if (!errorSent)
                        {
                            callback(null, details);
                        }
                    });

                    readStream.pipe(writeStream);
                }
            });
        },
        copyObject: function(user, filename, newFilename, callback)
        {
            var filePath = toSafeLocalPath(user.account_id, user.app_id, filename); 
            var newFilePath = toSafeLocalPath(user.account_id, user.app_id, newFilename); 
            
            client.mkdirp(path.dirname(newFilePath), function(err)
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    client.ln(filePath, newFilePath, function(err) 
                    {
                        callback(err);
                    });
                }
            });
        },
        moveObject: function(user, filename, newFilename, callback)
        {
            var filePath = toSafeLocalPath(user.account_id, user.app_id, filename); 
            var newFilePath = toSafeLocalPath(user.account_id, user.app_id, newFilename); 

            client.mkdirp(path.dirname(newFilePath), function(err)
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    client.ln(filePath, newFilePath, function(err) 
                    {
                        if (err)
                        {
                            callback(err);
                        }
                        else
                        {
                            client.unlink(filePath, function(err)
                            {
                                callback(err);
                            });
                        }
                    });
                }
            });
        },
        deleteObject: function(user, filename, callback)
        {
            var filePath = toSafeLocalPath(user.account_id, user.app_id, filename);

            client.unlink(filePath, function(err)
            {
                callback(err);
            });
        },
        getObjectMetaData: function(user, filename, callback)
        {
            var filePath = toSafeLocalPath(user.account_id, user.app_id, filename);

            var parentPath = path.dirname(filePath);
            var filename = path.basename(filePath);

            var options = {};

            // client.info returns info in the form:
            //
            // Directory
            // { 
            //     name: '000001',
            //     extension: 'directory',
            //     type: 'application/x-json-stream; type=directory',
            //     headers: 
            //     { 
            //         'last-modified': 'Tue, 27 Jun 2017 01:04:48 GMT',
            //         'content-type': 'application/x-json-stream; type=directory',
            //         'result-set-size': '2',
            //         'date': 'Tue, 25 Jul 2017 18:36:12 GMT',
            //         'server': 'Manta',
            //         'x-request-id': 'a0e73400-dc22-41d8-9b27-39a511d0fa37',
            //         'x-response-time': '19',
            //         'x-server-name': '4ff3f83e-23d3-49a0-986b-a6b0a881670b',
            //         'connection': 'keep-alive',
            //         'x-request-received': 1501007773169,
            //         'x-request-processing-time': 529 
            //     } 
            // }
            //
            // File
            // {
            //     name: 'DicknsonDiploma.pdf',
            //     extension: 'pdf',
            //     type: 'application/pdf',
            //     etag: 'cb7da3fd-d081-44e7-979e-a19f631ce17f',
            //     md5: '3wBMj3NUx7fuggrYNmMhWQ==',
            //     size: 321748,
            //     headers: 
            //     { 
            //         'etag': 'cb7da3fd-d081-44e7-979e-a19f631ce17f',
            //         'last-modified': 'Tue, 25 Jul 2017 02:43:24 GMT',
            //         'durability-level': '2',
            //         'content-length': '321748',
            //         'content-md5': '3wBMj3NUx7fuggrYNmMhWQ==',
            //         'content-type': 'application/pdf',
            //         'date': 'Tue, 25 Jul 2017 18:39:16 GMT',
            //         'server': 'Manta',
            //         'x-request-id': 'f01f0bff-e311-4944-a483-0b672a706de9',
            //         'x-response-time': '30',
            //         'x-server-name': '60771e58-2ad0-4c50-8b23-86b72f9307f8',
            //         'connection': 'keep-alive',
            //         'x-request-received': 1501007956549,
            //         'x-request-processing-time': 464 
            //     } 
            // }
            //
            client.info(filePath, options, function(err, info)
            {
                if (err)
                {
                    if (err.code == 'NotFoundError') // Why not "ResourceNotFound" like in download?
                    {
                        // Return null - file doesn't exist
                        callback(null, null);
                    }
                    else
                    {
                        log.error("Error getting file metadata:", err);
                        callback(err);
                    }
                }
                else
                {
                    // Convert info to entry
                    //
                    var entry = { name: filename, parent: parentPath };

                    if (info.extension === "directory")
                    {
                        entry["type"] = "directory";

                    }
                    else
                    {
                        entry["type"] = "object";
                        entry["etag"] = info.etag;
                        entry["size"] = info.size;
                    }

                    entry["mtime"] = new Date(info.headers["last-modified"]).toISOString();

                    callback(null, getEntryDetails(user, entry));
                }
            });

            // At one point we used "client.ls" (as commented out below) to get entry details, since it provided
            // them in the exact same form as in a directory listing.  Using "client.info" returned the results in
            // a different form, and among other things, did not include ms granularity in the last modified time.
            // As it turns out, Dropbox doesn't even allow ms granularity on times, so we switched back to using
            // "client.info" (above).  But here is the old "client.ls" code and notes in case we need it in future.
            //
            // In a perfect world we should be able to specify a limit of 1 and "client.ls" should return one item.  The 
            // marker/limit logic in the Manta module "client.ls" method is kind of screwey in that it uses the passed-in
            // limit as the per-request limit for its own internal paging, thus when you get exactly "limit" items,
            // it considers that a full page and tries to fetch another page (it's actually worse than that, because
            // there is a bug that causes this to happen even if you get one less than the amount requested).  If that
            // method understood the difference between a passed-in limit and its internal paging limit, then we could
            // just set "limit" below to 1 and that would be optimal (it would internally get exactly one item and return it).
            //
            // !!! TODO: Open a bug against Node Manta for the above.
            //
            /*
            var options = { marker: filename, limit: 3 };

            client.ls(parentPath, options, function(err, res)
            {
                var entry;

                if (err)
                {
                    callback(err);
                }
                else
                {
                    res.once('entry', function(item)
                    {
                        // The first entry is the one we want
                        entry = getEntryDetails(user, item);
                        log.info("Entry", entry);
                    });

                    res.once('error', function (err) 
                    {
                        log.error(err);
                        callback(err);
                    });

                    res.once('end', function () 
                    {
                        callback(null, entry);
                    });
                }
            });
            */
        }
    }

    return driver;
}
