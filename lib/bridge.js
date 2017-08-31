var async = require('async');
var path = require('path');

var lodash = require('lodash');
var uuidv4 = require('uuid/v4');

// This module bridges to optional driver methods.  If the driver does not implement the method, a default
// implementation is provided by this module.
//
module.exports = function(config, driver)
{
    var log = require('./logger').getLogger("bridge");

    var root;

    var bridge =
    {
        setRoot: function(theRoot)
        {
            root = theRoot;
        },
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
        deleteDirectory: function(user, dirPath, callback)
        {
            if (driver.deleteDirectory)
            {
                driver.deleteDirectory(user, dirPath, callback);
            }
            else // Fall back to object method (driver only overrides if dir operation is different)
            {
                driver.deleteObject(user, dirPath, callback);
            }
        },
        getDirectoryMetaData: function(user, dirPath, callback)
        {
            if (driver.getDirectoryMetaData)
            {
                driver.getDirectoryMetaData(user, dirPath, callback);
            }
            else // Fall back to object method (driver only overrides if dir operation is different)
            {
                driver.getObjectMetaData(user, dirPath, callback);
            }
        },
        getMetaData: function(user, fileOrDirPath, type, callback)
        {
            if (type === "file")
            {
                driver.getObjectMetaData(user, fileOrDirPath, callback);
            }
            else if (type === "folder")
            {
                bridge.getDirectoryMetaData(user, fileOrDirPath, callback);
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
                    driver.getDirectoryMetaData(user, fileOrDirPath, function(err, entry)
                    {
                        if (err || entry)
                        {
                            callback(err, entry);
                        }
                        else
                        {
                            driver.getObjectMetaData(user, fileOrDirPath, callback);
                        }
                    });
                }
                else
                {
                    driver.getObjectMetaData(user, fileOrDirPath, callback);
                }
            }
        },
        listFolderUsingCursor: function(user, dirPath, recursive, limit, cursor, callback)
        {
            if (driver.listFolderUsingCursor)
            {
                driver.listFolderUsingCursor(user, dirPath, recursive, limit, cursor, callback);
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

                driver.traverseDirectory(user, dirPath, recursive, onEntry, function(err, stopped)
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
        getLatestCursorItem: function(user, path, recursive, callback)
        {
            if (driver.getLatestCursorItem)
            {
                driver.getLatestCursorItem(user, path, recursive, callback);
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

                driver.traverseDirectory(user, path, recursive, onEntry, function(err, stopped)
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
        isAnyCursorItemNewer: function(user, path, recursive, cursorItem, callback)
        {
            if (driver.isAnyCursorItemNewer)
            {
                driver.isAnyCursorItemNewer(user, path, recursive, cursorItem, callback);
            }
            else if (driver.getLatestCursorItem)
            {
                driver.getLatestCursorItem(user, path, recursive, function(err, latestCursorItem)
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

                driver.traverseDirectory(user, path, recursive, onEntry, function(err, stopped)
                {
                    if (err)
                    {
                        // !!! req logger?
                        log.error("Traversal error on isAnyCursorItemNewer:", err);
                        callback(err);
                    }
                    else
                    {
                        callback(null, stopped);
                    }
                });
            }
        },
        getObjectMetaDataWithRetry: function(user, filename, isFolder, callback)
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
                bridge.getMetaData(user, filename, isFolder ? "folder" : "file", function(err, entry)
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
        startMultipartUpload: function(user, callback)
        {
            if (driver.startMultipartUpload)
            {
                driver.startMultipartUpload(user, callback);
            }
            else
            {
                var uploadId = uuidv4();
                var uploadPath = path.join("uploads", uploadId, "0.bin");

                root.putObjectStream(user, uploadPath, function(err, stream)
                {
                    callback(err, uploadId, stream);
                });
            }
        },
        multipartUpload: function(user, uploadId, offset, callback)
        {
            if (driver.multipartUpload)
            {
                driver.multipartUpload(user, uploadId, offset, callback);
            }
            else
            {
                var uploadPath = path.join("uploads", uploadId, offset.toString() + ".bin");
                root.putObjectStream(user, uploadPath, callback);
            }
        },
        finishMultipartUpload: function(user, uploadId, filename, callback)
        {
            if (driver.finishMultipartUpload)
            {
                driver.finishMultipartUpload(user, uploadId, filename, callback);
            }
            else
            {
                var uploadDirPath = path.join("uploads", uploadId);

                root.getDirectoryEntries(user, uploadDirPath, function(err, entries)
                {
                    if (err)
                    {
                        callback(err);
                    }
                    else if (!entries)
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

                        driver.putObject(user, filename, function(err, writeStream)
                        {
                            async.eachSeries(entries, function (entry, callback) 
                            {
                                log.info("Processing upload entry:", entry);
                                var currentFile = path.join(uploadDirPath, entry.name);
                                root.getObjectStream(user, currentFile, function(err, readStream)
                                {
                                    readStream.on('end', function ()
                                    {
                                        log.info("Done piping stream for entry:", entry);
                                        callback();
                                    });

                                    log.info("Pipe stream for entry:", entry);
                                    readStream.pipe(writeStream, { end: false });
                                });
                            }, 
                            function(err)
                            {
                                log.info("Done appending uploaded files");
                                writeStream.end();

                                root.deleteDirectory(user, uploadDirPath, function(err)
                                {
                                    // We hope this worked, but we're not going to error out just
                                    // because the upload dir delete failed.
                                    //
                                    if (err)
                                    {
                                        log.error("Failed to delete upload dir:", uploadDirPath, err);
                                    }

                                    bridge.getObjectMetaDataWithRetry(user, filename, false, function(err, srcEntry)
                                    {
                                        callback(null, srcEntry);
                                    });
                                });
                            });
                        });
                    }
                });
            }
        }
    }

    return bridge;
}
