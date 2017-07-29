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
                            files.forEach(function(file)
                            {
                                var entry = getEntryDetails(user, path.posix.join(fullPath, file));
                                log.debug("Entry", entry);

                                if (onEntry(entry))
                                {
                                    stopped = true;
                                    done();
                                }
                                else if (recursive && (entry[".tag"] == "folder"))
                                {
                                    q.push({ dirpath: path.posix.join(task.dirpath, entry.name) });
                                }
                            });
                        }

                        if (!stopped)
                        {
                            done();
                        }
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
                // !!! Process headers...
                //
                // Conditional requests - if not satisfied, return headers plus 304 'Not Modified'
                //
                //     If-Modified-Since: <date>
                //     If-None-Match: <etag> (one or more sep by comma)
                //
                // Range request (return 206 'Partial Content')
                //
                //     Range: <ranges>
                //
                log.info("getObject got headers:", requestHeaders);
            }

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
                    var respHeaders = {};

                    respHeaders['content-type'] = mimeTypes.lookup(filePath) || 'application/octet-stream';
                    respHeaders['content-md5'] = md5File.sync(filePath);
                    respHeaders['etag'] = respHeaders['content-md5'];
                    respHeaders['content-length'] = stats.size;
                    respHeaders['last-modified'] = stats.mtime.toISOString();
                    respHeaders['accept-ranges'] = 'bytes';

                    // !!! headers['content-range'] - on range request, when supported

                    callback(null, fs.createReadStream(filePath), 200, 'OK', respHeaders);
                }
            }
            catch (err)
            {
                if (err.code === 'ENOENT')
                {
                    // We return null content to indicate "Not found"
                    err = null;
                }
                callback(err, null);
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
