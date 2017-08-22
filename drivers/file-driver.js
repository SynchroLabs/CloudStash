// File store
//
// https://nodejs.org/api/fs.html
// https://github.com/jprichardson/node-fs-extra
//
var fs = require('fs-extra');
var path = require('path');
var async = require('async');

var lodash = require('lodash');
var md5File = require('md5-file');
var mimeTypes = require('mime-types');

var log = require('./../lib/logger').getLogger("file-driver");

module.exports = function(params, config)
{
    var basePath = params.basePath;

    log.info("Using file store, basePath:", basePath);

    var maxConcurrency = config.get('MAX_CONCURRENCY');

    function getEntryDetails(user, fullpath)
    {
        var userPath = path.posix.join(basePath, user.account_id);
        if (user.app_id)
        {
            userPath = path.posix.join(userPath, user.app_id);
        } 

        var fStat = fs.statSync(fullpath); // !!! What if not found?
        var displayPath = "/" + path.relative(userPath, fullpath);

        var item = { };
        item[".tag"] = fStat.isFile() ? "file" : "folder";
        item["name"] = path.basename(displayPath);
        item["path_lower"] = displayPath.toLowerCase();
        item["path_display"] = displayPath;
        item["id"] = displayPath; // !!! Required by Dropbox - String(min_length=1)

        // At least in MacOS, the mtime of a directory is equal to the mtime of the most recent file it contains.
        // For our purposes, we need the last time the directory itself was modified, which on a file-based
        // implementation (with no properties, integral rename, etc), is the creation time.
        //
        var mtime = fStat.isFile() ? fStat.mtime.toISOString() : fStat.birthtime.toISOString();
        item["server_modified"] = mtime.replace(/\.\d{3}/, ''); // !!! Remove ms for Dropbox
        item["client_modified"] = item["server_modified"]; // !!! Required by Dropbox

        item["rev"] = "000000001"; // !!! Required by Dropbox - String(min_length=9, pattern="[0-9a-f]+")

        item["size"] = fStat.size;
        // item["content_hash"]

        return item;
    }

    function toSafePath(filePath)
    {
        // path.posix.normalize will move any ../ to the front, and the regex will remove them.
        //
        var safePath = path.posix.normalize(filePath).replace(/^(\.\.[\/\\])+/, '');

        if (path.sep != '/')
        {
            // Replace forward slash with local platform seperator
            //
            safePath = filePath.replace(/[\/]/g, path.sep);
        }

        return safePath;
    }

    function toSafeLocalPath(account_id, app_id, filePath)
    {
        if (app_id)
        {
            return path.posix.join(basePath, account_id, app_id, toSafePath(filePath)); 
        }
        else
        {
            return path.posix.join(basePath, account_id, toSafePath(filePath));
        }
    }

    var driver = 
    {
        provider: "file",
        createDirectory: function(user, dirPath, callback)
        {
            var fullPath = toSafeLocalPath(user.account_id, user.app_id, dirPath); 

            fs.mkdirs(fullPath, function(err)
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

                fs.readdir(fullPath, function(err, files) 
                {
                    // If the error is 'not found' and the dir in question is the root dir, we're just
                    // going to ignore that and return an empty dir lising (just means we haven't created
                    // this user/app path yet because it hasn't been used yet).
                    //
                    if (err && ((err.code !== 'ENOENT') || (task.dirpath !== '')))
                    {
                        done(err);
                    }
                    else
                    {
                        if (files)
                        {
                            for (var i = 0; i < files.length; i++)
                            {
                                var file = files[i];
                                var entry = getEntryDetails(user, path.posix.join(fullPath, file));
                                log.debug("Entry", entry);

                                if (onEntry(entry))
                                {
                                    stopped = true;
                                    break;
                                }
                                else if (recursive && (entry[".tag"] == "folder"))
                                {
                                    q.push({ dirpath: path.posix.join(task.dirpath, entry.name) });
                                }
                            }
                        }

                        done();
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

            try
            {
                var stats = fs.statSync(filePath);
                if (stats.isDirectory())
                {
                    // !!! This should only be called for objects (not directories).  Maybe look at what DropBox/Manta do.
                    //
                    callback(new Error("getObject called on directory"));
                }
                else
                {
                    // Generate some headers from the content
                    //
                    var respSent = false;
                    var respHeaders = {};

                    respHeaders['content-type'] = mimeTypes.lookup(filePath) || 'application/octet-stream';
                    respHeaders['content-md5'] = md5File.sync(filePath);
                    respHeaders['etag'] = respHeaders['content-md5'];
                    respHeaders['content-length'] = stats.size;
                    respHeaders['last-modified'] = stats.mtime.toUTCString();
                    respHeaders['accept-ranges'] = 'bytes';

                    if (requestHeaders)
                    {
                        // If-None-Match
                        //
                        if (requestHeaders['if-none-match'])
                        {
                            if (requestHeaders['if-none-match'].indexOf(respHeaders['etag']) != -1)
                            {
                                callback(null, null, 304, 'Not Modified', respHeaders);
                                respSent = true;
                            }
                        }
                        // If-Modified-Since (ignored when If-None-Match also present)
                        //
                        else if (requestHeaders["if-modified-since"])
                        {
                            var ifModDate = new Date(requestHeaders["if-modified-since"]);
                            log.info("Comparing if-modified-since %s to mtime: %s", ifModDate, stats.mtime);

                            if (new Date(requestHeaders["if-modified-since"]) >= stats.mtime)
                            {
                                callback(null, null, 304, 'Not Modified', respHeaders);
                                respSent = true;
                            }
                        }
                    }

                    if (!respSent)
                    {
                        var respCode = 200;
                        var respMessage = "OK";
                        var readStreamOpts = {};

                        // Range 
                        //
                        if (requestHeaders && requestHeaders["range"])
                        {
                            if (requestHeaders['if-match'] && (requestHeaders['if-match'].indexOf(respHeaders['etag']) === -1))
                            {
                                log.error("getObject Range unsatisfiable, If-Match of %s did't match %s", requestHeaders['if-match'], respHeaders['etag']);
                                callback(null, null, 416, 'Range Not Satisfiable', respHeaders);
                                respSent = true;
                            }
                            else if (requestHeaders["range"].indexOf("bytes=") === 0) // Forms we support: "bytes=200-" "bytes=200-1000"
                            {
                                var ranges = requestHeaders["range"].substring("bytes=".length).split("-");
                                readStreamOpts.start = parseInt(ranges[0]);
                                if (ranges.length > 1)
                                {
                                    readStreamOpts.end = parseInt(ranges[1]);
                                }
                                else
                                {
                                    readStreamOpts.end = stats.size;
                                }

                                if (isNaN(readStreamOpts.start) || 
                                    isNaN(readStreamOpts.end) || 
                                    (readStreamOpts.start >= readStreamOpts.end) || 
                                    (readStreamOpts.end > stats.size))
                                {
                                    log.error("getObject Range unsatisfiable, start %s, end %s", readStreamOpts.start, readStreamOpts.end);
                                    callback(null, null, 416, 'Range Not Satisfiable', respHeaders);
                                    respSent = true;
                                }
                                else
                                {
                                    respCode = 206;
                                    respMessage = "Partial Content";
                                    respHeaders['content-range'] = "bytes " + readStreamOpts.start + "-" + readStreamOpts.end + "/" + stats.size;
                                }
                            }
                            else
                            {
                                log.error("getObject Range request contained no 'bytes=' prefix, unsatisfiable");
                                callback(null, null, 416, 'Range Not Satisfiable', respHeaders);
                                respSent = true;
                            }
                        }

                        if (!respSent)
                        {
                            callback(null, fs.createReadStream(filePath, readStreamOpts), respCode, respMessage, respHeaders);
                        }
                    }
                }
            }
            catch (err)
            {
                if (err.code === 'ENOENT')
                {
                    callback(null, null, 404, 'Not Found');
                }
                else
                {
                    callback(err);
                }
            }
        },
        putObject: function(user, filename, callback)
        {
            var filePath = toSafeLocalPath(user.account_id, user.app_id, filename);

            fs.ensureDir(path.dirname(filePath), function(err)
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    // !!! May need to use mode r+ (instead of default w) to overwrite existing file
                    //
                    callback(null, fs.createWriteStream(filePath));
                }
            });
        },
        copyObject: function(user, filename, newFilename, callback)
        {
            var filePath = toSafeLocalPath(user.account_id, user.app_id, filename);
            var newFilePath = toSafeLocalPath(user.account_id, user.app_id, newFilename);
            
            fs.copy(filePath, newFilePath, function(err) // Creates directories as needed
            {
                callback(err);
            });
        },
        moveObject: function(user, filename, newFilename, callback)
        {
            var filePath = toSafeLocalPath(user.account_id, user.app_id, filename);
            var newFilePath = toSafeLocalPath(user.account_id, user.app_id, newFilename);

            fs.move(filePath, newFilePath, function(err) // Creates directories as needed
            {
                callback(err);
            });
        },
        deleteObject: function(user, filename, callback)
        {
            var filePath = toSafeLocalPath(user.account_id, user.app_id, filename);

            fs.remove(filePath, function(err)
            {
                callback(err)
            });
        },
        getObjectMetaData: function(user, filename, callback)
        {
            var filePath = toSafeLocalPath(user.account_id, user.app_id, filename);

            try
            {
                callback(null, getEntryDetails(user, filePath));
            }
            catch (err)
            {
                if (err.code == 'ENOENT')
                {
                    callback(null, null);
                }
                else
                {
                    callback(err);
                }
            }
        },
    }

    return driver;
}
