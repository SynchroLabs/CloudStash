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
var log = require('./../lib/logger').getLogger("manta-driver");

var fs = require('fs');
var path = require('path');
var async = require('async');

var lodash = require('lodash');

var manta = require('manta');

module.exports = function(params)
{
    var basePath = params.basePath;

    log.debug("Using Manta store, basePath:", basePath);

    var maxConcurrency = 4; // !!! Get this from config (with reasonable default)

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

    function toSafeLocalPath(user, fileName)
    {
        // path.posix.normalize will move any ../ to the front, and the regex will remove them.
        //
        var safeFilename = path.posix.normalize(fileName).replace(/^(\.\.[\/\\])+/, '');

        // !!! This forms a path of basePath/account_id/app_id/xxxxx - For scale, we assume the account_id
        //     is a GUID (randomly distributed digits).  In order to keep directories from getting too large, 
        //     we can break down the path further using the first three pairs of characters from the GUID, for
        //     a path like: basePath/AB/CD/EF/GHIJKLxxx/app_id/xxxxx.  In that model, with 100m users acounts,
        //     the first two levels of directories will be "full" (256 entries), and the third level will contain
        //     an average of 6 accounts.
        //
        var filePath = path.posix.join(basePath, user.account_id, user.app_id, safeFilename); 

        return filePath;
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
        var displayPath = "/" + path.relative(path.posix.join(basePath, user.account_id, user.app_id), fullpath);

        // Convert to Dropbox form
        //
        var item = { };
        item[".tag"] = (mantaEntry.type == "object") ? "file" : "folder";
        item["name"] = mantaEntry.name;

        item["path_lower"] = displayPath.toLowerCase();
        item["path_display"] = displayPath;
        // item["id"]
        // item["client_modified"]
        item["server_modified"] = mantaEntry.mtime;
        //item["rev"]
        if (mantaEntry.size)
        {
            // Not present on directory entry
            item["size"] = mantaEntry.size;
        }
        // item["content_hash"]

        return item;
    }

    function getEntrySortKey(entry)
    {
        return entry["server_modified"] + entry["path_display"];
    }

    function getCursorItem(entry)
    {
        var cursorItem = null;

        if (entry)
        {
            cursorItem = 
            {
                "server_modified": entry["server_modified"],
                "path_display": entry["path_display"]
            }
        }

        return cursorItem;
    }

    log.debug('Manta client setup: %s', client.toString());

    var driver = 
    {
        provider: "manta",
        isCursorItemNewer: function(item1, item2)
        {
            return (!item1 || (getEntrySortKey(item1) < getEntrySortKey(item2)));
        },
        createDirectory: function(user, dirPath, callback)
        {
            var fullPath = toSafeLocalPath(user, dirPath); 

            client.mkdirp(fullPath, function(err)
            {
                if (err)
                {
                    callback(err);
                }
                else 
                {
                    // !!! Better entry details?  (query existing dir? - may have to wait)
                    //
                    var entry = { type: "directory", name: dirPath };
                    callback(null, getEntryDetails(user, entry));
                }
            });
        },
        listDirectory: function(user, dirPath, recursive, limit, cursor, callback)
        {
            var entries = [];

            var q = async.queue(function(task, done) 
            {
                var fullPath = toSafeLocalPath(user, task.dirpath);

                // !!! It appears from the source code that client.ls will make multiple underlying REST API
                //     calls and page through the entries without us havint to do anything special.
                //
                //         https://github.com/joyent/node-manta/blob/master/lib/client.js
                //
                var options = {};

                client.ls(fullPath, options, function(err, res)
                {
                    if (err)
                    {
                        if ((err.code == 'NOTFOUND') && (dirPath == ''))
                        {
                            // If the error is 'not found' and the dir in question is the root dir, we're just
                            // going to ignore that and return an empty dir lising (just means we haven't created
                            // this user/app path yet because it hasn't been used yet).
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
                            var entry = getEntryDetails(user, item);
                            log.debug("Entry", entry);

                            // If there is a cursor, only process entries greater than the cursor
                            //
                            if (!cursor || (getEntrySortKey(cursor) < getEntrySortKey(entry)))
                            {
                                // This will insert into "entries" such that "entries" will be/stay in sorted order
                                //
                                entries.splice(lodash.sortedIndexBy(entries, entry, function(o){ return getEntrySortKey(o); }), 0, entry);

                                // This will keep the list from growing beyond more than one over the limit (we purposely
                                // leave the "extra" entry so that at the end we will be able to see that we went past
                                // the limit).
                                //
                                if (entries.length > limit + 1)
                                {
                                    entries.splice(limit + 1);
                                }
                            }

                            if (recursive && (entry[".tag"] == "folder"))
                            {
                                q.push({ dirpath: path.posix.join(task.dirpath, entry.name) });
                            }
                        });

                        res.once('error', function (err) 
                        {
                            log.error(err);
                            done(err);
                        });

                        res.once('end', function () 
                        {
                            done();
                        });
                    }
                });
            }, maxConcurrency);

            q.error = function(err, task)
            {
                q.kill();
                callback(err);
            };

            q.drain = function() 
            {
                var hasMore = false;
                var cursorItem = cursor && cursor.lastItem;

                if (entries.length > limit)
                {
                    entries.splice(limit);
                    hasMore = true;
                }

                if (entries.length > 0)
                {
                    cursorItem = getCursorItem(entries[entries.length-1]);
                }

                callback(null, entries, hasMore, cursorItem);
            };

            q.push({ dirpath: dirPath });
        },
        getLatestCursorItem: function(user, dirPath, recursive, callback)
        {
            var latestEntry;

            var q = async.queue(function(task, done) 
            {
                var fullPath = toSafeLocalPath(user, task.dirpath);

                // !!! See comment in listDirecory (above) re paging results.
                //
                var options = {};

                client.ls(fullPath, options, function(err, res)
                {
                    if (err)
                    {
                        if ((err.code == 'NOTFOUND') && (dirPath == ''))
                        {
                            // If the error is 'not found' and the dir in question is the root dir, we're just
                            // going to ignore that and return an empty dir lising (just means we haven't created
                            // this user/app path yet because it hasn't been used yet).
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
                            var entry = getEntryDetails(user, item);
                            log.debug("Entry", entry);

                            if (!latestEntry)
                            {
                                latestEntry = entry;
                            }
                            else
                            {
                                var entrySortKey = getEntrySortKey(entry);
                                var latestEntrySortKey = getEntrySortKey(latestEntry);

                                if (entrySortKey > latestEntrySortKey)
                                {
                                    latestEntry = entry;
                                }
                            }

                            if (recursive && (entry[".tag"] == "folder"))
                            {
                                q.push({ dirpath: path.posix.join(task.dirpath, entry.name) });
                            }
                        });

                        res.once('error', function (err) 
                        {
                            log.error(err);
                            done(err);
                        });

                        res.once('end', function () 
                        {
                            done();
                        });
                    }
                });
            }, maxConcurrency);

            q.error = function(err, task)
            {
                q.kill();
                callback(err);
            };

            q.drain = function() 
            {
                callback(null, getCursorItem(latestEntry));
            };

            q.push({ dirpath: dirPath });
        },
        findItems: function(user, dirPath, isMatch, start, limit, callback)
        {
            var entries = [];

            var q = async.queue(function(task, done) 
            {
                var fullPath = toSafeLocalPath(user, task.dirpath);

                // !!! See comment in listDirecory (above) re paging results.
                //
                var options = {};

                client.ls(fullPath, options, function(err, res)
                {
                    if (err)
                    {
                        if ((err.code == 'NOTFOUND') && (dirPath == ''))
                        {
                            // If the error is 'not found' and the dir in question is the root dir, we're just
                            // going to ignore that and return an empty dir lising (just means we haven't created
                            // this user/app path yet because it hasn't been used yet).
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
                            var entry = getEntryDetails(user, item);
                            log.debug("Find items evaluating entry", entry);

                            if (isMatch(entry))
                            {
                                // This will insert into "entries" such that "entries" will be/stay in sorted order
                                //
                                entries.splice(lodash.sortedIndexBy(entries, entry, function(o){ return getEntrySortKey(o); }), 0, entry);

                                // This will keep the list from growing beyond more than one over the limit (we purposely
                                // leave the "extra" entry so that at the end we will be able to see that we went past
                                // the limit).
                                //
                                if (entries.length > (start + limit + 1))
                                {
                                    entries.splice(start + limit + 1);
                                }
                            }

                            if (entry[".tag"] == "folder")
                            {
                                q.push({ dirpath: path.posix.join(task.dirpath, entry.name) });
                            }
                        });

                        res.once('error', function (err) 
                        {
                            log.error(err);
                            done(err);
                        });

                        res.once('end', function () 
                        {
                            done();
                        });
                    }
                });
            }, maxConcurrency);

            q.error = function(err, task)
            {
                q.kill();
                callback(err);
            };

            q.drain = function() 
            {
                var hasMore = false;

                if (entries.length > (start + limit))
                {
                    entries.splice(start + limit);
                    hasMore = true;
                }

                if (start)
                {
                    entries.splice(0, start);
                }

                callback(null, entries, hasMore, start + entries.length);
            };

            q.push({ dirpath: dirPath });
        },        
        getObject: function(user, filename, callback)
        {
            var filePath = toSafeLocalPath(user, filename);

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
        putObject: function(user, filename, callback)
        {
            var filePath = toSafeLocalPath(user, filename);

            client.mkdirp(path.dirname(filePath), function(err)
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    // !!! Do we have to do anything special to overwrite existing file?
                    //
                    var options = {};
                    callback(null, client.createWriteStream(filePath, options));
                }
            });
        },
        copyObject: function(user, filename, newFilename, callback)
        {
            var filePath = toSafeLocalPath(user, filename); 
            var newFilePath = toSafeLocalPath(user, newFilename); 
            
            client.mkdirp(path.dirname(newFilePath), function(err)
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    // !!! Note: Only copies single file (as opposed to folder), doesn't deal with name conflict / rename
                    //
                    client.ln(filePath, newFilePath, function(err) 
                    {
                        if (err)
                        {
                            callback(err);
                        }
                        else
                        {
                            // !!! Better entry details?  
                            //
                            //        Query source obj before copy?
                            //        Get info on new obj after copy (may have to wait for it to show up)?
                            //
                            var entry = { type: "object", name: newFilename };
                            callback(null, getEntryDetails(user, entry));
                        }
                    });
                }
            });
        },
        moveObject: function(user, filename, newFilename, callback)
        {
            var filePath = toSafeLocalPath(user, filename); 
            var newFilePath = toSafeLocalPath(user, newFilename); 

            client.mkdirp(path.dirname(newFilePath), function(err)
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    // !!! Note: Only moves single file (as opposed to folder), doesn't deal with name conflict / rename
                    //
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
                                if (err)
                                {
                                    callback(err);
                                }
                                else
                                {
                                    // !!! Better entry details?  
                                    //
                                    //        Query source obj before move?
                                    //        Get info on obj after move (may have to wait for it to show up)?
                                    //
                                    var entry = { type: "object", name: newFilename };
                                    callback(null, getEntryDetails(user, entry));
                                }
                            });
                        }
                    });
                }
            });
        },
        deleteObject: function(user, filename, callback)
        {
            // !!! This will remove a single file or an empty directory only (need to implement support for deleting
            //     non-empty directory).
            //
            var filePath = toSafeLocalPath(user, filename);

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
                            callback(null, getEntryDetails(user, entry));
                        }
                    });
                }
            });
        },
        getObjectMetaData: function(user, filename, callback)
        {
            var filePath = toSafeLocalPath(user, filename);

            var parentPath = path.dirname(filePath);
            var filename = path.basename(filePath);

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
            var options = { marker: filename, limit: 3 };

            var entry;

            // We used to do a "client.info" on the object to get metadata, but the metadata returned doesn't
            // match the metadata we get from "client.ls" (in particular, the mtime is not at ms granularity).
            //
            client.ls(parentPath, options, function(err, res)
            {
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
        },
        startMultipartUpload: function(user, callback)
        {
            // Multipart upload RFD - https://github.com/joyent/rfd/blob/master/rfd/0065/README.md
            //
            var tmpPath = path.posix.join(basePath, "temp0000"); 

            var options = {
                account: params.user
            }

            client.createUpload(tmpPath, options, function(err, uploadId)
            {
                if (err)
                {
                    log.error("Error on creatUpload", err);
                    callback(err);
                }
                else
                {
                    log.info("createUpload id:", uploadId);
                    callback(null, uploadId);
                }
            });
        }
    }

    return driver;
}
