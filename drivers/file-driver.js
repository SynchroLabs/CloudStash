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

module.exports = function(params)
{
    var basePath = params.basePath;

    log.info("Using file store, basePath:", basePath);

    function getEntryDetails(user, fullpath, filename)
    {
        var fStat = fs.statSync(fullpath);
        var displayPath = "/" + path.relative(path.posix.join(basePath, user.account_id, user.app_id), fullpath);

        var item = { };
        item[".tag"] = fStat.isFile() ? "file" : "folder";
        item["name"] = filename;
        item["path_lower"] = displayPath.toLowerCase();
        item["path_display"] = displayPath;
        // item["id"]
        // item["client_modified"]
        item["server_modified"] = fStat.mtime.toISOString();
        //item["rev"]
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

    function toSafeLocalUserPath(user, filePath)
    {
        return toSafePath(path.posix.join(basePath, user.account_id, filePath)); 
    }

    function toSafeLocalUserAppPath(user, filePath)
    {
        return toSafePath(path.posix.join(basePath, user.account_id, user.app_id, filePath)); 
    }

    function processFiles(user, entries, fullPath, files, recursive, limit, cursor, callback)
    {
        log.info("Entries for dir %s: %s", fullPath, files);

        async.eachLimit(files, 10, function(file, fileComplete)
        {
            var entry = getEntryDetails(user, path.posix.join(fullPath, file), file);

            // If there is a cursor, only process entries greater than the cursor
            //
            if (!cursor || (getEntrySortKey(cursor) < getEntrySortKey(entry)))
            {
                // This will insert into "entries" such that "entries" will be/stay in sorted order
                //
                entries.splice(lodash.sortedIndexBy(entries, entry, function(o){ return getEntrySortKey(o); }), 0, entry);

                // This will keep the list from growing beyond more than one over the limit (we purposely leave the "extra"
                // entry so that at the end the top level function will be able to see that we went past the limit, and will
                // have the "next" entry after the limit, in case that's useful).
                //
                if (entries.length > limit + 1)
                {
                    entries.splice(limit + 1);
                }
            }

            if (recursive && (entry[".tag"] == "folder"))
            {
                processDirectory(user, entries, path.posix.join(fullPath, entry.name), recursive, limit, cursor, fileComplete);
            }
            else
            {
                fileComplete();
            }
        }, 
        function(err)
        {
            callback(err);
        });
    }

    function processDirectory(user, entries, dirpath, recursive, limit, cursor, callback)
    {
        fs.readdir(dirpath, function(err, files) 
        {
            if (err)
            {
                callback(err);
            }
            else if (files)
            {
                processFiles(user, entries, dirpath, files, recursive, limit, cursor, callback);
            }
            else
            {
                callback();
            }
        });
    }

    function findLatestInFiles(user, latestEntry, fullPath, files, recursive, callback)
    {
        async.eachLimit(files, 10, function(file, fileComplete)
        {
            var entry = getEntryDetails(user, path.posix.join(fullPath, file), file);

            var entrySortKey = getEntrySortKey(entry);
            var latestEntrySortKey = getEntrySortKey(latestEntry);

            if (!latestEntrySortKey || (entrySortKey > latestEntrySortKey))
            {
                latestEntry["server_modified"] = entry["server_modified"];
                latestEntry["path_display"] = entry["path_display"];
            }

            if (recursive && (entry[".tag"] == "folder"))
            {
                findLatestInDirectory(user, latestEntry, path.posix.join(fullPath, entry.name), recursive, fileComplete);
            }
            else
            {
                fileComplete();
            }
        }, 
        function(err)
        {
            callback(err);
        });
    }

    function findLatestInDirectory(user, latestEntry, dirpath, recursive, callback)
    {
        fs.readdir(dirpath, function(err, files) 
        {
            if (err)
            {
                callback(err);
            }
            else if (files)
            {
                findLatestInFiles(user, latestEntry, dirpath, files, recursive, callback);
            }
            else
            {
                callback();
            }
        });
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
            var fullPath = toSafeLocalUserAppPath(user, dirPath); 

            fs.mkdirs(fullPath, function(err)
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    var entry = getEntryDetails(user, fullPath, dirPath);
                    callback(err, entry);
                }
            });
        },
        listDirectory: function(user, dirPath, recursive, limit, cursor, callback)
        {
            var fullPath = toSafeLocalUserAppPath(user, dirPath); 

            fs.readdir(fullPath, function(err, files) 
            {
                // If the error is 'not found' and the dir in question is the root dir, we're just
                // going to ignore that and return an empty dir lising (just means we haven't created
                // this user/app path yet because it hasn't been used yet).
                //
                if (err && ((err.code !== 'ENOENT') || (dirPath !== '')))
                {
                    callback(err);
                }

                if (files)
                {
                    var entries = [];

                    processFiles(user, entries, fullPath, files, recursive, limit, cursor, function(err)
                    {
                        if (err)
                        {
                            callback(err);
                        }
                        else
                        {
                            var hasMore = false;
                            if (entries.length > limit)
                            {
                                entries.splice(limit);
                                hasMore = true;
                            }

                            var cursorItem = getCursorItem(entries[entries.length-1]);

                            callback(null, entries, hasMore, cursorItem);
                        }
                    })
                }
                else
                {
                    callback(null, [], false, null);
                }
            });
        },
        getLatestCursorItem: function(user, dirPath, recursive, callback)
        {
            var fullPath = toSafeLocalUserAppPath(user, dirPath); 

            fs.readdir(fullPath, function(err, files) 
            {
                // If the error is 'not found' and the dir in question is the root dir, we're just
                // going to ignore that and return an empty dir lising (just means we haven't created
                // this user/app path yet because it hasn't been used yet).
                //
                if (err && ((err.code !== 'ENOENT') || (dirPath !== '')))
                {
                    callback(err);
                }

                if (files)
                {
                    var latestEntry = {};

                    findLatestInFiles(user, latestEntry, fullPath, files, recursive, function(err)
                    {
                        if (err)
                        {
                            callback(err);
                        }
                        else
                        {
                            callback(null, latestEntry["server_modified"] ? latestEntry : null);
                        }
                    })
                }
                else
                {
                    callback(null, null);
                }
            });

        },
        getObject: function(user, filename, callback)
        {
            var filePath = toSafeLocalUserAppPath(user, filename);

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
                    callback(null, fs.createReadStream(filePath));
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
            var filePath = toSafeLocalUserAppPath(user, filename); 

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
            var filePath = toSafeLocalUserAppPath(user, filename); 
            var newFilePath = toSafeLocalUserAppPath(user, newFilename); 
            
            fs.copy(filePath, newFilePath, function(err) // Creates directories as needed
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    callback(err, getEntryDetails(user, newFilePath, newFilename));
                }
            });
        },
        moveObject: function(user, filename, newFilename, callback)
        {
            var filePath = toSafeLocalUserAppPath(user, filename); 
            var newFilePath = toSafeLocalUserAppPath(user, newFilename); 

            fs.move(filePath, newFilePath, function(err) // Creates directories as needed
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    callback(err, getEntryDetails(user, newFilePath, newFilename));
                }
            });
        },
        deleteObject: function(user, filename, callback)
        {
            // This will remove a file or a directory, so let's hope it's used correctly
            //
            var filePath = toSafeLocalUserAppPath(user, filename);

            var entry = getEntryDetails(user, filePath, filename);

            fs.remove(filePath, function(err)
            {
                callback(err, entry)
            });
        },
        startMultipartUpload: function(user, callback)
        {
            // File name convention:
            //
            //    <user>/uploads/<uuid>/<offset>.bin 
            //
            var uploadId = uuidv4();

            var uploadPath = toSafeLocalUserPath(user, path.join("uploads", uploadId, "0.bin"));

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
            var uploadPath = toSafeLocalUserPath(user, path.join("uploads", uploadId, offset.toString() + ".bin"));
            callback(null, fs.createWriteStream(uploadPath));
        },
        finishMultipartUpload: function(user, uploadId, filename, callback)
        {
            var uploadDirPath = toSafeLocalUserPath(user, path.join("uploads", uploadId));
            var filePath = toSafeLocalUserAppPath(user, filename); 

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
                        var entry = getEntryDetails(user, path.posix.join(uploadDirPath, file), file);
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
                                callback(null, getEntryDetails(user, filePath, filename));
                            });
                        });
                    });
                }
            });
        }
    }

    return driver;
}
