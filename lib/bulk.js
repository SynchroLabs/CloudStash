var async = require('async');
var path = require('path');

var lodash = require('lodash');

var dbErr = require('./error');

// This module provides for validating and performing bulk operations (operations on a folder, or on a list of
// files/folders).
//
module.exports = function(config, driver)
{
    var log = require('./logger').getLogger("bulk");

    var _maxConcurrency = config.get('MAX_CONCURRENCY');

    var bridge;

    var bulk = 
    {
        // In Dropbox
        //
        //   * When move/copy/delete, item returned is a single item representing the destination object (file or folder)
        //   * conflict detection (for autorename) is only at top level item (no folder merging w/ rename of contents)
        //   * folder items don't have mtime
        //
        // Dropbox errors
        //
        //   too_many_files
        //   duplicated_or_nested_paths
        //   cant_move_folder_into_itself
        //   other
        //
        setBridge: function(theBridge)
        {
            bridge = theBridge;
        },
        validateOperation: function(user, srcpath, dstpath, operation, mode, maxItems, callback)
        {
            // operation = move, copy, delete
            // mode (for move/copy) = add (no overwrite if conflict), overwrite, rename
            // maxItems = If we exceed this, stop and return too_many_files
            //
            var itemCount = 0;

            async.waterfall([
                function(callback) // Get source entry metadata 
                {
                    if (operation === "upload")
                    {
                        // No source entry to get on upload
                        callback(null, null);
                    }
                    else
                    {
                        log.info("Getting source entry at path:", srcpath);
                        bridge.getMetaData(user, srcpath, null, function(err, srcEntry)
                        {
                            log.info("Got callback");
                            if (err)
                            {
                                callback(err);
                            }
                            else if (!srcEntry)
                            {
                                callback(new dbErr.DropboxError("from_lookup", "not_found", "Source file not found")); 
                            }
                            else
                            {
                                log.info("Got srcEntry", srcEntry);
                                callback(null, srcEntry);
                            }
                        });
                    }
                },
                function(srcEntry, callback) // Ensure entry count is less than maxItems if maxItems specified
                {
                    log.info("Validating item count, if applicable");
                    if (maxItems)
                    {
                        if ((operation === "upload") || (srcEntry[".tag"] === "file"))
                        {
                            itemCount = 1;
                            callback(null, srcEntry);
                        }
                        else
                        {
                            function onEntry(entry)
                            {
                                return (++itemCount > maxItems);
                            }

                            bridge.traverseDirectory(user, srcpath, true, onEntry, function(err, stopped)
                            {
                                if (stopped)
                                {
                                    err = new dbErr.DropboxError("too_many_files");
                                }
                                callback(err, srcEntry);
                            });
                        }
                    }
                    else
                    {
                        callback(null, srcEntry);
                    }
                },
                function(srcEntry, callback) // Get destination entry metadata (if move/copy)
                {
                    log.info("Getting destination entry if applicable");
                    if ((operation === "move") || (operation === "copy") || (operation === "upload"))
                    {
                        bridge.getMetaData(user, dstpath, null, function(err, dstEntry)
                        {
                            if (err)
                            {
                                callback(err);
                            }
                            else
                            {
                                callback(null, srcEntry, dstEntry);
                            }
                        });
                    }
                    else
                    {
                        callback(null, srcEntry, null);
                    }
                },
                function(srcEntry, dstEntry, callback) // Resolve destination conflicts (if applicable)
                {
                    log.info("Resolving destination conflicts (if applicable)");
                    try 
                    {
                        if (operation === "delete")
                        {
                            callback(null, srcEntry, null);
                        }
                        else // move/copy/upload
                        {
                            if (dstEntry)
                            {
                                if (mode === "add")
                                {
                                    // FAIL - dstEntry exists
                                    //
                                    callback(new dbErr.DropboxError("to", "conflict", "Destination already exists"));
                                }
                                else if (mode === "rename")
                                {
                                    // Determine a new valid (non-existant) dst path we can use
                                    //
                                    var validIndex = null;

                                    var start = 1;
                                    var tries = 10;
                                    var indexes = [];

                                    var parsedPath = path.parse(dstpath);
                                    var dstName = parsedPath.name;

                                    // If dstpath already contains an index suffix, start after that...
                                    //
                                    var matches = /^(.*)\s\((\d*)\)$/.exec(dstName);
                                    if (matches)
                                    {
                                        dstName = matches[1];
                                        start = parseInt(matches[2]) + 1;
                                    }

                                    for (var i = 0; i < tries; i++)
                                    {
                                        indexes[i] = i + start;
                                    }

                                    // For each candate, do getObjectMetaData, if doesn't exist, good, else increment and repeat
                                    //
                                    async.someSeries(indexes, function(index, callback)
                                    {
                                        var testPath = path.join(parsedPath.dir, dstName + " (" + index + ")") + parsedPath.ext;
                                        log.info("Getting metadata for", testPath);
                                        bridge.getObjectMetaData(user, testPath, function(err, entry)
                                        {
                                            if (err)
                                            {
                                                callback(err);
                                            }
                                            else
                                            {
                                                if (!entry)
                                                {
                                                    validIndex = index;
                                                }
                                                callback(null, !entry);
                                            }
                                        });
                                    },
                                    function(err, result)
                                    {
                                        if (err)
                                        {
                                            callback(err);
                                        }
                                        else if (result)
                                        {
                                            // A valid index was found (one of the series functions returned true)
                                            //
                                            log.info("Found usable index:", validIndex);
                                            var newDstPath = path.join(parsedPath.dir, dstName + " (" + validIndex + ")") + parsedPath.ext;
                                            callback(null, srcEntry, newDstPath);
                                        }
                                        else
                                        {
                                            // No valid index was found
                                            callback(new Error("No available paths for autorename"));
                                        }
                                    });

                                    // In case of folder, do we need to create it?
                                }
                                else // mode === "overwrite"
                                {
                                    if ((operation === "upload") && (dstEntry[".tag"] == "file"))
                                    {
                                        // Upload and dest is a file, so we're good to overwrite
                                        //
                                        callback(null, srcEntry, dstEntry.path_display);
                                    }
                                    else if (dstEntry[".tag"] === srcEntry[".tag"])
                                    {
                                        // They're the same type, so we're good to overwrite
                                        //
                                        callback(null, srcEntry, dstEntry.path_display);
                                    }
                                    else
                                    {
                                        // FAIL - dstEntry exists, but is not same type as srcEntry
                                        //
                                        callback(new dbErr.DropboxError("to", "conflict", "Destination already exists and is not the same type as source"));
                                    }
                                }
                            }
                            else // dest entry doesn't exist
                            {
                                // If folder, make sure dstpath exists, if file, make sure parent of dstpath exists
                                //
                                var createDstPath = dstpath;
                                if ((operation === "upload") || (srcEntry[".tag"] === "file"))
                                {
                                    createDstPath = path.dirname(dstpath);
                                }

                                bridge.createDirectory(user, createDstPath, function(err)
                                {
                                    callback(null, srcEntry, dstpath);
                                });
                            }
                        }
                    } 
                    catch (err)
                    { 
                        log.error("Error resolving destination conflicts", err);
                        callback(err); 
                    }
                }
            ], 
            function (err, srcEntry, dstPath)
            {
                if (err)
                {
                    log.error("err validating:", err);
                    callback(err);
                }
                else
                {
                    // The idea is that by this point we have a valid operation with a valid dstPath, if applicable 
                    // (name conflicts resolved, and if folder, will have been created), such that we're ready to just complete
                    // the move/copy/delete operation without having to check for further conflicts.  Note that dstPath may 
                    // not match dstpath (if rename due to conflict occurred).
                    //
                    log.info("validated:", itemCount, srcEntry, dstPath);
                    callback(null, itemCount, srcEntry, dstPath);
                }
            });
        },
        doOperation: function(req, operation, workItems, callback)
        {
            // operation = move, copy, delete
            //
            // workItems is an array of: { [parent], srcEntry, dstPath, [remainingEntries] } 
            //
            //     "parent" refers to the workItem for the parent directory of the given workItem (if any)
            //
            //     "remainingEntries" is the count of child entries (will be decrememnted on move/delete as children deleted) 
            //
            var q = async.queue(function(workItem, done) 
            {
                log.info("workItem:", workItem);

                if (workItem.srcEntry[".tag"] === "file")
                {
                    // File operation
                    //
                    if (operation === "copy")
                    {
                        log.info("Copy file src to dst:", workItem);
                        bridge.copyObject(req.user, workItem.srcEntry.path_display, workItem.dstPath, function(err)
                        {
                            if (err)
                            {
                                req.log.error("Error on copy:", err);
                            }

                            done(err);
                        });
                    }
                    else if (operation === "move")
                    {
                        log.info("Move file src to dst:", workItem);
                        bridge.moveObject(req.user, workItem.srcEntry.path_display, workItem.dstPath, function(err)
                        {
                            if (err)
                            {
                                req.log.error("Error on move:", err);
                            }

                            if (workItem.parent && (--workItem.parent.remainingEntries === 0))
                            {
                                // Scheule parent workItem for deletion
                                //
                                q.push(workItem.parent);
                            }

                            done(err);
                        });
                    }
                    else if (operation === "delete")
                    {
                        log.info("Delete file src to dst:", workItem);
                        bridge.deleteObject(req.user, workItem.srcEntry.path_display, function(err)
                        {
                            if (err)
                            {
                                req.log.error("Error on delete:", err);
                            }

                            if (workItem.parent && (--workItem.parent.remainingEntries === 0))
                            {
                                // Scheule parent workItem for deletion
                                //
                                q.push(workItem.parent);
                            }

                            done(err);
                        });
                    }
                }
                else
                {
                    // Folder operation
                    //
                    if (workItem.remainingEntries === 0) // Only move/delete operations are tracking remaining entries
                    {
                        log.info("Deleting folder itself:", workItem);
                        bridge.deleteDirectory(req.user, workItem.srcEntry.path_display, function(err)
                        {
                            if (err)
                            {
                                // !!! If it fails because it's not empty it could be a lag in deleted objects showing
                                //     as deleted, so we want to reschedule it and try again after a short delay.
                                //
                                req.log.error("Error on delete of directory:", err);
                            }

                            if (workItem.parent && (--workItem.parent.remainingEntries === 0))
                            {
                                // Scheule parent workItem for deletion
                                //
                                q.push(workItem.parent);
                            }

                            done(err);
                        });
                    }
                    else
                    {
                        log.info("Process folder contents:", workItem);

                        bridge.listFolderUsingCursor(req.user, workItem.srcEntry.path_display, false, null, null, function(err, entries)
                        {
                            if (err)
                            {
                                req.log.error("Error on list folder:", err);
                                done(err);
                            }
                            else if (entries && entries.length)
                            {
                                if ((operation === "move") || (operation === "delete"))
                                {
                                    // For move/delete:
                                    //
                                    // We pass the parent workItem as part of the new workItem, so the child workItem can complete,
                                    // decrement the parent workItem remainingEntries, and if 0, schedule the parent for deletion.
                                    //
                                    workItem.remainingEntries = entries.length;
                                }

                                entries.forEach(function(entry)
                                {
                                    var entryWorkItem = { parent: workItem, srcEntry: entry };
                                    if (operation !== "delete")
                                    {
                                        entryWorkItem.dstPath = path.join(workItem.dstPath, path.basename(entry.path_display));
                                    }
                                    q.push(entryWorkItem);
                                });

                                done();
                            }
                            else // Empty folder
                            {
                                if ((operation === "move") || (operation === "delete"))
                                {
                                    // Schedule the source folder for deletion now (it was empty)
                                    //
                                    log.info("Pushing workitem for empty dir");
                                    workItem.remainingEntries = 0;
                                    q.push(workItem);
                                }

                                if ((operation === "move") || (operation === "copy"))
                                {
                                    // If there are no entries in the source folder, we want to create the corresponding 
                                    // destination folder (since it will not be created automatically when copying contents).
                                    //
                                    bridge.createDirectory(req.user, workItem.dstPath, function(err)
                                    {
                                        if (err)
                                        {
                                            req.log.error("Error on creation of empty directory:", err);
                                        }
                                        done(err);
                                    });
                                }
                                else
                                {
                                    done();
                                }
                            }
                        });
                    }
                }

            }, _maxConcurrency);

            q.error = lodash.once(function(err, task)
            {
                q.kill();
                callback(err);
            });

            q.drain = function() 
            {
                // !!! Details?
                callback(null);
            };

            workItems.forEach(function(workItem)
            {
                q.push(workItem);
            });
        }
    }

    return bulk;
}
