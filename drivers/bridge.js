var async = require('async');
var path = require('path');
var stream = require('stream');

var lodash = require('lodash');
var uuidv4 = require('uuid/v4');

// This module bridges to optional driver methods.  If the driver does not implement the method, a default
// implementation is provided by this module.
//
module.exports = function(config, driver)
{
    var _maxConcurrency = config.get('MAX_CONCURRENCY');

    var log = require('./../lib/logger').getLogger("bridge");

    var bridge =
    {
        getEntrySortKey: function(entry)
        {
            if (driver.getEntrySortKey)
            {
                return driver.getEntrySortKey(entry);
            }
            else
            {
                return entry["server_modified"] + entry["path_display"];
            }
        },
        getCursorItem: function(entry)
        {
            if (driver.getCursorItem)
            {
                return driver.getCursorItem(entry);
            }
            else
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
        },
        isCursorItemNewer: function(item1, item2)
        {
            return (!item1 || (bridge.getEntrySortKey(item1) < bridge.getEntrySortKey(item2)));
        },
        createDirectory: function (dirPath, callback)
        {
            driver.createDirectory(dirPath, callback);
        },
        deleteDirectory: function (dirPath, callback)
        {
            if (driver.deleteDirectory)
            {
                driver.deleteDirectory(dirPath, callback);
            }
            else // Fall back to object method (driver only overrides if dir operation is different)
            {
                driver.deleteObject(dirPath, callback);
            }
        },
        getDirectoryMetaData: function(dirPath, callback)
        {
            if (driver.getDirectoryMetaData)
            {
                driver.getDirectoryMetaData(dirPath, callback);
            }
            else // Fall back to object method (driver only overrides if dir operation is different)
            {
                driver.getObjectMetaData(dirPath, callback);
            }
        },
        getObjectMetaData: function (filePath, callback)
        {
            driver.getObjectMetaData(filePath, callback);
        },
        getMetaData: function(fileOrDirPath, type, callback)
        {
            if (type === "file")
            {
                driver.getObjectMetaData(fileOrDirPath, callback);
            }
            else if (type === "folder")
            {
                bridge.getDirectoryMetaData(fileOrDirPath, callback);
            }
            else if (type === null)
            {
                // This is specifically the case where the caller does not know if the object is
                // a file or directory and they're getting metadata to find out...
                //
                if (driver.getDirectoryMetaData)
                {
                    // Try getDirectoryMetaData, and if that fails (not found), try getObjectMetaData
                    //
                    driver.getDirectoryMetaData(fileOrDirPath, function(err, entry)
                    {
                        if (err || entry)
                        {
                            callback(err, entry);
                        }
                        else
                        {
                            driver.getObjectMetaData(fileOrDirPath, callback);
                        }
                    });
                }
                else
                {
                    driver.getObjectMetaData(fileOrDirPath, callback);
                }
            }
        },
        getObject: function(filePath, requestHeaders, callback)
        {
            driver.getObject(filePath, requestHeaders, callback);
        },
        putObject: function(filePath, readStream, callback)
        {
            log.info("Put object:", filePath);
            driver.putObject(filePath, readStream, callback);
        },
        copyObject: function(filePath, newFilePath, callback)
        {
            if (driver.copyObject)
            {
                driver.copyObject(filePath, newFilePath, callback);
            }
            else
            {
                // !!! driver.getObject + driver.putObject (All current drivers have copyObject)
                callback(new Error("bridge synthetic copyObject not yet implemented"));
            }
        },
        moveObject: function(filePath, newFilePath, callback)
        {
            if (driver.moveObject)
            {
                driver.moveObject(filePath, newFilePath, callback);
            }
            else
            {
                bridge.copyObject(filePath, newFilePath, function(err)
                {
                    if (err)
                    {
                        callback(err);
                        return;
                    }

                    bridge.deleteObject(filePath, callback);
                })
            }
        },
        deleteObject: function(filePath, callback)
        {
            driver.deleteObject(filePath, callback);
        },
        traverseDirectory: function(dirPath, recursive, onEntry, callback)
        {
            driver.traverseDirectory(dirPath, recursive, onEntry, callback);
        },
        listFolderUsingCursor: function(dirPath, recursive, limit, cursor, callback)
        {
            if (driver.listFolderUsingCursor)
            {
                driver.listFolderUsingCursor(dirPath, recursive, limit, cursor, callback);
            }
            else // Fall back to traverse (brute force)
            {
                var entries = [];

                limit = limit || 999999;

                function onEntry(entry)
                {
                    // If there is a cursor, only process entries greater than the cursor
                    //
                    if (!cursor || (bridge.getEntrySortKey(cursor) < bridge.getEntrySortKey(entry)))
                    {
                        // This will insert into "entries" such that "entries" will be/stay in sorted order
                        //
                        entries.splice(lodash.sortedIndexBy(entries, entry, function(o){ return bridge.getEntrySortKey(o); }), 0, entry);

                        // This will keep the list from growing beyond more than one over the limit (we purposely
                        // leave the "extra" entry so that at the end we will be able to see that we went past
                        // the limit).
                        //
                        if (entries.length > limit + 1)
                        {
                            entries.splice(limit + 1);
                        }
                    }
                }

                bridge.traverseDirectory(dirPath, recursive, onEntry, function(err, stopped)
                {
                    if (err)
                    {
                        // !!! req logger?
                        log.error("Traversal error on listFolderUsingCursor:", err);
                        callback(err);
                    }
                    else
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
                            cursorItem = bridge.getCursorItem(entries[entries.length-1]);
                        }

                        callback(null, entries, hasMore, cursorItem);
                    }
                });
            }
        },
        getLatestCursorItem: function(dirPath, recursive, callback)
        {
            if (driver.getLatestCursorItem)
            {
                driver.getLatestCursorItem(dirPath, recursive, callback);
            }
            else // Fall back to traverse (brute force)
            {
                var latestEntry = null;

                function onEntry(entry)
                {
                    if (!latestEntry)
                    {
                        latestEntry = entry;
                    }
                    else
                    {
                        var entrySortKey = bridge.getEntrySortKey(entry);
                        var latestEntrySortKey = bridge.getEntrySortKey(latestEntry);

                        if (entrySortKey > latestEntrySortKey)
                        {
                            latestEntry = entry;
                        }
                    }
                }

                bridge.traverseDirectory(dirPath, recursive, onEntry, function(err, stopped)
                {
                    if (err)
                    {
                        // !!! req logger?
                        log.error("Traversal error on getLatestCursorItem:", err);
                        callback(err);
                    }
                    else
                    {
                        callback(null, bridge.getCursorItem(latestEntry));
                    }
                });
            }
        },
        isAnyCursorItemNewer: function(dirPath, recursive, cursorItem, callback)
        {
            if (driver.isAnyCursorItemNewer)
            {
                driver.isAnyCursorItemNewer(dirPath, recursive, cursorItem, callback);
            }
            else if (driver.getLatestCursorItem)
            {
                driver.getLatestCursorItem(dirPath, recursive, function(err, latestCursorItem)
                {
                    callback(err, bridge.isCursorItemNewer(cursorItem, latestCursorItem));
                });
            }
            else // Fall back to traverse (brute force)
            {
                function onEntry(entry)
                {
                    return bridge.isCursorItemNewer(cursorItem, bridge.getCursorItem(entry));
                }

                bridge.traverseDirectory(dirPath, recursive, onEntry, function(err, stopped)
                {
                    if (err)
                    {
                        callback(err);
                    }
                    else
                    {
                        callback(null, stopped);
                    }
                });
            }
        },
        getObjectMetaDataWithRetry: function(filePath, isFolder, callback)
        {
            // There are a number of cases where we do a file/folder create operation (create/move/copy/etc) where we
            // know that the resulting file/folder should exist afterward, but it may not be immediately visible to the 
            // driver (with eventually-consistent cloud storage providers, for example).  In these cases, we want to
            // retry getting the metadata until the entry shows up.
            //
            var notFoundError = 'Object not found';

            async.retry(
            {
                times: 5,
                interval: function(retryCount) 
                {
                    // Start at 100ms retry interval, with exponential backoff (100, 200, 400, 800, etc).
                    return 100 * Math.pow(2, retryCount);
                },
                errorFilter: function(err) 
                {
                    // Only retry on not found
                    return err.message === notFoundError;
                }
            },
            function(callback)
            {
                bridge.getMetaData(filePath, isFolder ? "folder" : "file", function(err, entry)
                {
                    if (!err && !entry)
                    {
                        err = new Error(notFoundError);
                    }
                    callback(err, entry);
                });
            },
            function(err, entry) 
            {
                if (err && (err.message === notFoundError))
                {
                    err = null;
                }
                callback(err, entry);
            });
        },        
        startMultipartUpload: function(uploadDir, readStream, callback)
        {
            if (driver.startMultipartUpload)
            {
                driver.startMultipartUpload(readStream, callback);
            }
            else
            {
                var uploadId = uuidv4();
                var uploadPath = path.join(uploadDir, uploadId, "0.bin");

                driver.putObject(uploadPath, readStream, function(err, details)
                {
                    callback(err, uploadId);
                });
            }
        },
        multipartUpload: function(uploadDir, readStream, uploadId, offset, callback)
        {
            if (driver.multipartUpload)
            {
                driver.multipartUpload(readStream, uploadId, offset, callback);
            }
            else
            {
                var uploadPath = path.join(uploadDir, uploadId, offset.toString() + ".bin");
                driver.putObject(uploadPath, readStream, callback);
            }
        },
        finishMultipartUpload: function(uploadDir, uploadId, filePath, callback)
        {
            if (driver.finishMultipartUpload)
            {
                driver.finishMultipartUpload(uploadId, filePath, callback);
            }
            else
            {
                var uploadDirPath = path.join(uploadDir, uploadId);

                var entries = [];

                function onEntry(entry)
                {
                    entries.push(entry);
                }

                driver.traverseDirectory(uploadDirPath, false, onEntry, function(err, stopped)
                {
                    if (err)
                    {
                        callback(err);
                    }
                    else if (entries.length === 0)
                    {
                        callback(new Error("No files uploaded with the id:", uploadId));
                    }
                    else
                    {
                        // Sort entries by offset
                        //
                        entries.sort(function(a, b)
                        {
                            return a.offset - b.offset;
                        });

                        log.info("Entries", entries);

                        // !!! Verify that we start at 0 and there are no holes
                        //

                        var entriesToBeDeleted = [];

                        var multiReadStream = new stream.Transform();
                        multiReadStream._transform = function (chunk, encoding, done) 
                        {
                            done(null, chunk);
                        }

                        function nextStream()
                        {
                            var entry = entries.shift();
                            if (entry)
                            {
                                log.info("Processing upload entry:", entry);
                                var currentFile = path.join(uploadDirPath, entry.name);
                                driver.getObject(currentFile, function(err, readStream)
                                {
                                    readStream.on('end', function ()
                                    {
                                        log.info("Done piping stream for entry:", entry);
                                        nextStream();
                                    });

                                    log.info("Pipe stream for entry:", entry);
                                    readStream.pipe(multiReadStream, { end: false });
                                });

                                entriesToBeDeleted.push(entry);
                            }
                            else
                            {
                                log.info("Done appending uploaded files");
                                multiReadStream.end();
                            }
                        }

                        nextStream();

                        driver.putObject(filePath, multiReadStream, function(err, details)
                        {
                            if (err)
                            {
                                log.error("Failed to upload multipart object:", err);
                                callback(err);
                                return;
                            }

                            log.info("Done uploading multipart object");

                            async.eachLimit(entriesToBeDeleted, _maxConcurrency, function(entry, callback)
                            {
                                driver.deleteObject(entry.path_display, callback);
                            },
                            function (err) 
                            {
                                if (err)
                                {
                                    log.error("Error deleting upload part:", err);

                                    // We don't bail just because we couldn't clean up.  And no use attempting to delete
                                    // the upload dir since it's not empty, so let's just get out of here...
                                    //
                                    bridge.getObjectMetaDataWithRetry(filePath, false, function(err, srcEntry)
                                    {
                                        callback(null, srcEntry);
                                    });
                                }
                                else
                                {
                                    log.info("Uploaded parts deleted")

                                    bridge.deleteDirectory(uploadDirPath, function(err)
                                    {
                                        // We hope this worked, but we're not going to error out just
                                        // because the upload dir delete failed.
                                        //
                                        if (err)
                                        {
                                            log.error("Failed to delete upload dir:", uploadDirPath, err);
                                        }
                                        else
                                        {
                                            log.info("Upload directory deleted:", uploadDirPath);
                                        }

                                        bridge.getObjectMetaDataWithRetry(filePath, false, function(err, srcEntry)
                                        {
                                            callback(null, srcEntry);
                                        });
                                    })
                                }
                            });
                        });
                    }
                });
            }
        }
    }

    return bridge;
}
