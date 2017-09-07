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
    log.info("Using file store, basePath:", params.basePath);

    var maxConcurrency = config.get('MAX_CONCURRENCY');

    function getEntryDetails(fullpath)
    {
        var fStat = fs.statSync(fullpath); // !!! What if not found?

        var displayPath = "/" + path.relative(params.basePath, fullpath);

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

    function toLocalPath (thePath)
    {
        // Do we need to change separator for DOS/Windows?  Do we care?
        //
        var localPath = path.posix.join(params.basePath, thePath);
        if (path.sep != '/')
        {
            // Replace forward slash with local platform seperator
            //
            localPath = localPath.replace(/[\/]/g, path.sep);
        }

        return localPath;
    }

    var driver = 
    {
        provider: "file",
        createDirectory: function(dirPath, callback)
        {
            fs.mkdirs(toLocalPath(dirPath), function(err)
            {
                callback(err);
            });
        },
        traverseDirectory: function(dirPath, recursive, onEntry, callback)
        {
            var stopped = false;

            var q = async.queue(function(task, done) 
            {
                var fullPath = toLocalPath(task.dirpath);

                fs.readdir(fullPath, function(err, files) 
                {
                    if (err && (err.code !== 'ENOENT'))
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
                                var entry = getEntryDetails(path.posix.join(fullPath, file));
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
        getObjectMetaData: function(filePath, callback)
        {
            try
            {
                callback(null, getEntryDetails(toLocalPath(filePath)));
            }
            catch (err)
            {
                log.error("Got err in getObjectMetaData:", err);
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
        getObject: function(filePath, requestHeaders, callback)
        {
            // requestHeaders is optional
            //
            if (typeof callback === 'undefined')
            {
                callback = requestHeaders;
                requestHeaders = null;
            }

            try
            {
                var localPath = toLocalPath(filePath);

                log.info("Getting object at path:", localPath);

                var stats = fs.statSync(localPath);
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
                    respHeaders['content-md5'] = md5File.sync(localPath);
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
                            callback(null, fs.createReadStream(localPath, readStreamOpts), respCode, respMessage, respHeaders);
                        }
                    }
                }
            }
            catch (err)
            {
                if (err.code === 'ENOENT')
                {
                    log.error("Not fount in getObject", err);
                    callback(null, null, 404, 'Not Found');
                }
                else
                {
                    callback(err);
                }
            }
        },
        putObject: function(filePath, readStream, callback)
        {
            fs.ensureDir(path.dirname(toLocalPath(filePath)), function(err)
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    // !!! May need to use mode r+ (instead of default w) to overwrite existing file
                    //
                    var writeStream = fs.createWriteStream(toLocalPath(filePath));

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
        copyObject: function(filePath, newFilePath, callback)
        {
            fs.copy(toLocalPath(filePath), toLocalPath(newFilePath), function(err) // Creates directories as needed
            {
                callback(err);
            });
        },
        moveObject: function(filePath, newFilePath, callback)
        {
            fs.move(toLocalPath(filePath), toLocalPath(newFilePath), function(err) // Creates directories as needed
            {
                callback(err);
            });
        },
        deleteObject: function(filePath, callback)
        {
            fs.remove(toLocalPath(filePath), function(err)
            {
                callback(err)
            });
        },
    }

    return driver;
}
