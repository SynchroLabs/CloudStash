// File store
//
// https://nodejs.org/api/fs.html
// https://github.com/jprichardson/node-fs-extra
//
var fs = require('fs-extra');
var path = require('path');
var async = require('async');

var lodash = require('lodash');

var uuidv4 = require('uuid/v4');

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
        isCursorItemNewer: function(item1, item2)
        {
            return (!item1 || (getEntrySortKey(item1) < getEntrySortKey(item2)));
        },
        createDirectory: function(user, dirPath, callback)
        {
            var fullPath = toSafeLocalPath(user.account_id, user.app_id, dirPath); 

            fs.mkdirs(fullPath, function(err)
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    var entry = getEntryDetails(user, fullPath);
                    callback(err, entry);
                }
            });
        },
        listDirectory: function(user, dirPath, recursive, limit, cursor, callback) // !!! cursor is cursor item/entry
        {
            var entries = [];

            limit = limit || 999999;

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
        traverseDirectory: function(user, dirPath, onEntry, callback)
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
                                else if (entry[".tag"] == "folder")
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
        getLatestCursorItem: function(user, dirPath, recursive, callback)
        {
            var latestEntry;

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
                callback(null, getCursorItem(latestEntry));
            };

            q.push({ dirpath: dirPath });
        },
        findItems: function(user, dirPath, isMatch, start, limit, callback)
        {
            var entries = [];

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
                    var entry = getEntryDetails(user, filePath);
                    callback(null, entry, fs.createReadStream(filePath));
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
                if (err)
                {
                    callback(err);
                }
                else
                {
                    callback(err, getEntryDetails(user, newFilePath));
                }
            });
        },
        moveObject: function(user, filename, newFilename, callback)
        {
            var filePath = toSafeLocalPath(user.account_id, user.app_id, filename);
            var newFilePath = toSafeLocalPath(user.account_id, user.app_id, newFilename);

            fs.move(filePath, newFilePath, function(err) // Creates directories as needed
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    callback(err, getEntryDetails(user, newFilePath));
                }
            });
        },
        deleteObject: function(user, filename, callback)
        {
            // This will remove a file or a directory, so let's hope it's used correctly
            //
            var filePath = toSafeLocalPath(user.account_id, user.app_id, filename);

            var entry = getEntryDetails(user, filePath);

            fs.remove(filePath, function(err)
            {
                callback(err, entry)
            });
        },
        getObjectMetaData: function(user, filename, callback)
        {
            try
            {
                log.info("getObjectMetaData for path:", filename);
                var filePath = toSafeLocalPath(user.account_id, user.app_id, filename);
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
        startMultipartUpload: function(user, callback)
        {
            // File name convention:
            //
            //    <user>/uploads/<uuid>/<offset>.bin 
            //
            var uploadId = uuidv4();

            var uploadPath = toSafeLocalPath(user.account_id, null, path.join("uploads", uploadId, "0.bin"));

            fs.ensureDir(path.dirname(uploadPath), function(err)
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    callback(null, uploadId, fs.createWriteStream(uploadPath));
                }
            });
        },
        multipartUpload: function(user, uploadId, offset, callback)
        {
            var uploadPath = toSafeLocalPath(user.account_id, null, path.join("uploads", uploadId, offset.toString() + ".bin"));
            callback(null, fs.createWriteStream(uploadPath));
        },
        finishMultipartUpload: function(user, uploadId, filename, callback)
        {
            var uploadDirPath = toSafeLocalPath(user.account_id, null, path.join("uploads", uploadId));
            var filePath = toSafeLocalPath(user.account_id, user.app_id, filename); 

            log.info("Processing finishMultipartUpload for upload '%s', writing to dest: %s", uploadId, filename);

            // Process uploaded file segments
            //
            fs.readdir(uploadDirPath, function(err, files) 
            {
                if (err)
                {
                    callback(err);
                }
                else if (!files)
                {
                    callback(new Error("No files uploaded with the id:", uploadId));
                }
                else
                {
                    var entries = [];

                    // Get the detailed info for uploaded file segments
                    //
                    files.forEach(function(file)
                    {
                        var entry = getEntryDetails(user, path.posix.join(uploadDirPath, file));
                        entry.offset = parseInt(path.parse(entry.name).name);
                        entries.push(entry);
                    });

                    // Sort entries by offset
                    //
                    entries.sort(function(a, b)
                    {
                        return a.offset - b.offset;
                    });

                    log.info("Entries", entries);

                    // !!! Verify that we start at 0 and there are no holes
                    //

                    // Stream the files in order to destination file
                    //
                    fs.ensureDir(path.dirname(filePath), function(err)
                    {
                        if (err)
                        {
                            callback(new Error("Unable to create dir for uploaded file"));
                            return;
                        }

                        var writeStream = fs.createWriteStream(filePath);
                        async.eachSeries(entries, function (entry, callback) 
                        {
                            var currentFile = path.join(uploadDirPath, entry.name);
                            var stream = fs.createReadStream(currentFile).on('end', function () 
                            {
                                callback();
                            });
                            stream.pipe(writeStream, { end: false });
                        }, 
                        function(err)
                        {
                            writeStream.end();

                            fs.remove(uploadDirPath, function(err)
                            {
                                if (err)
                                {
                                    log.error("Failed to delete upload path for uploadID:", uploadId);
                                }

                                // Return details about newly created file
                                //
                                callback(null, getEntryDetails(user, filePath));
                            });
                        });
                    });
                }
            });
        }
    }

    return driver;
}
