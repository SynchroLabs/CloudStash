// Azure Storage driver
//
// https://github.com/Azure/azure-storage-node
//
// API docs: http://azure.github.io/azure-storage-node/BlobService.html
//
// Examples: https://docs.microsoft.com/en-us/azure/storage/blobs/storage-nodejs-how-to-use-blob-storage
//
// Azure doesn't have the concept of a "directory".  In the various Azure UX implementations that support a
// "create directory" option, all those do is create a UX context in which you can create objects to which it
// will prepend the "directory" path (but it's all virtual).  For example, if you do a "create directory" in 
// the Azure Storage Explorer, but don't put anything into it, then come back later, that "directory" will not
// be there.  These implementations (and Azure itself) treate any path element as a virtual directory (so if
// you create /foo/bar/baz/test.txt, the virtual directories foo, bar, and baz will be shown, even though they
// don't exist anywhere other than in the path to that object).
//
// In our application, we need the ability to create and persist a directory that might be empty, and we will 
// likely need to attach metadata to directories that we create at some point.  The current design for this is
// to create an object representing the directory.  We cannot create an object with a path then ends in "/" (as
// we do in S3 to denote a directory), so the plan is to create a blob named the name of the desired directory
// plus the ":" charater (the colon is not an allowed filename character in most file systems, so this should
// avoid conflict with any user file object).
//
// One issue with this design is that these ":" directory objects will show up as objects in the various Azure
// UX implementations.  This isn't really a problem, as we're only worried about the presentation of the Azure
// storage contents through our own application, but it is something to note.
//
var log = require('./../lib/logger').getLogger("azure-driver");

var async = require('async');
var path = require('path');
var stream = require('stream');

var azure = require('azure-storage');

var _directoryMetadataFilename = ":"; // See note above

module.exports = function(params, config)
{
    log.info("Using Azure store, account:", params.accountName);

    // Could also use Azure File Storage (has true directory support, case-insensitive but case preserving, 
    // but only scales to 5TB, vs 500TB for Azure Blob Storage).  One solution would be to do a driver for each.
    //
    var blobService = azure.createBlobService(
        params.accountName, 
        params.accountKey
    );

    blobService.createContainerIfNotExists(params.container, { }, function(err, result, response) 
    {
        // result is in the form...
        /*
        ContainerResult 
        {
            name: 'files',
            etag: '\"0x8D4F4406C6BE1F1\"',
            lastModified: 'Tue, 05 Sep 2017 09:28:10 GMT',
            lease: { status: 'unlocked', state: 'available' },
            requestId: 'ed72dc36-001e-010a-0d29-265cee000000',
            publicAccessLevel: null,
            created: false
        }
        */

        if (err)
        {
            log.error("Error validating/creating container '%s':", params.container, err);
        }
        else if (result.created)
        {
            log.info("Container '%s' was created", params.container);
        }
        else
        {
            log.info("Container '%s' already existed", params.container);
        }
    });

    function getEntryDetails(azureObject)
    {
        // BlobResult for directory
        //
        // {
        //     name: '1234-BEEF/000001/Photos/' 
        // }
        //
        // BlobResult for object
        // 
        // {
        //     name: '1234-BEEF/000001/IMG_0746.jpg',
        //     lastModified: 'Tue, 12 Sep 2017 01:53:20 GMT',
        //     etag: '0x8D4F9810B79AA98',
        //     contentLength: '1307098',
        //     contentSettings: [Object],
        //     blobType: 'BlockBlob',
        //     lease: [Object],
        //     serverEncrypted: 'false' 
        // }
        //
        log.info("Got Azure object (BlobResult):", azureObject)

        var item = { };
        item[".tag"] = "file";

        var fullpath = azureObject.name;
        if (fullpath.lastIndexOf("/") === fullpath.length-1)
        {
            item[".tag"] = "folder";
            fullpath = fullpath.slice(0, -1);
        }
        else if (fullpath.lastIndexOf(_directoryMetadataFilename) === fullpath.length-1)
        {
            item[".tag"] = "folder";
            fullpath = fullpath.slice(0, -1);
        }

        var displayPath = "/" + fullpath;

        // Convert to Dropbox form
        //
        item["name"] = fullpath.split(path.sep).pop();

        item["path_lower"] = displayPath.toLowerCase();
        item["path_display"] = displayPath;
        item["id"] = displayPath; // !!! Required by Dropbox - String(min_length=1)

        item["server_modified"] = azureObject.LastModified; // !!! Can't imaging this is gonna work - also remove ms for Dropbox
        item["client_modified"] = item["server_modified"]; // !!! Required by Dropbox

        item["rev"] = "000000001"; // !!! Required by Dropbox - String(min_length=9, pattern="[0-9a-f]+")

        if (azureObject.contentLength)
        {
            item["size"] = azureObject.contentLength;
        }
        // item["content_hash"]
 
        return item;
    }

    function logResult(fnName, err, result, response)
    {
        if (err)
        {
            log.error("%s error:", fnName, err);
        }
        else
        {
            log.info("%s result:", fnName, result);
        }
    }

    function getDirectoryMarkerFilename (dirName)
    {
        if (dirName.lastIndexOf("/") === dirName.length-1)
        {
            dirName = dirName.slice(0, -1);
        }

        return dirName + _directoryMetadataFilename;
    }

    var driver = 
    {
        provider: "azure",
        createDirectory: function(dirPath, callback)
        {
            // We don't really need to create the intermediate directories (as they will exist as Azure virtual directories
            // and we will report their existence properly in getDirectoryMetaData).  The only time we *need* the directory
            // marker blobs is in the case of an explicitly created directory (that may be/remain empty) or if we need to set
            // metadata on the directory (not currently supported).

            log.info("Creating dir:", dirPath);

            var options = {};
            blobService.createAppendBlobFromText(params.container, getDirectoryMarkerFilename(dirPath), "", options, function (err, result, response)
            {
                logResult("createAppendBlobFromText (createDirectory)", err, result, response);
                callback(err);
            });
        },
        deleteDirectory: function(dirPath, callback)
        {
            this.deleteObject(getDirectoryMarkerFilename(dirPath), function(err)
            {
                if (err && (err.statusCode === 404))
                {
                    // If we try to delete a directory marker and it does not exist, we won't fail/err on the delete.  It may
                    // be a virtual directory that "deleted itself" when its content disappeared.
                    //
                    // !!! If we really cared, we could call getDirectoryMetaData to confirm that the directory isn't a valid
                    //     virtual directory.  If it is a valid directory without a directory marker blob, that means that by
                    //     definition it is not empty (it is a virtual directory that can only exist by virtue of a blob contained
                    //     in that directory), in which case we might actually want to fail this (this function should only be used
                    //     to delete known-empty directories).
                    //
                    err = null;
                }
                callback(err);
            });
        },
        traverseDirectory: function(dirPath, recursive, onEntry, callback)
        {
            // If we were relying solely on our directory markers this would be a lot simpler.  But we are aiming to make things
            // work in an unsurprising way when we get pointed at any Azure container full of contents, so our directory support
            // needs to work with either an Azure "virtual directory" (that exits because it's in the path of a blob) or our 
            // explicit directory marker blobs (used to persist potentially empty directories, and to maintain directory metadata).

            var pathsProcessed = {};

            // Process a directory and any parent directories
            //
            function processDir (theDir)
            {
                if (!pathsProcessed[theDir])
                {
                    // Process parents first
                    var parentDir = path.dirname(theDir);
                    if (parentDir && (parentDir !== ".") && (parentDir !== "/"))
                    {
                        if (processDir(parentDir))
                        {
                            return true;
                        }
                    }

                    pathsProcessed[theDir] = true;
                    return onEntry(getEntryDetails({ name: theDir + "/" }));
                }

                return false;
            }

            if (recursive)
            {
                // !!! Implement - We report the directory indicator (virtual or marker) as they are encountered, and since
                //     there is no guarantee that the directory marker will sort before contents of that directory, we can't
                //     guarantee that the marker will get reported (a virtual directory may be reported before the marker), 
                //     except in the case of an empty directory, in which case the marker will be reported (since no there
                //     would be no virtual directory).
                //
                // When we do a recursive listing, we don't get any explicit information about directories, but since we are
                // visiting every blob under the given directory, we will encounter every (virtual) "directory" that exists
                // under that directory in the paths of the blobs we encounter.
                //
                // We do not want to report a directory object more than once, so we need to keep track of which directories
                // we have sent.  Also, we may have a directory marker blob, and if so, that should take precedence over any
                // virtual  directory.
                //
                log.info("Listing blobs (recursively) at prefix:", dirPath);

                var options = {};
                blobService.listBlobsSegmentedWithPrefix(params.container, dirPath + "/", null, options, function(err, result, response)
                {
                    if (!err)
                    {
                        log.info("Got result:", result);
                        for (var i = 0; i < result.entries.length; i++)
                        {
                            var entry = getEntryDetails(result.entries[i]);

                            if ((entry[".tag"] === "folder") && pathsProcessed[entry["path_display"]])
                            {
                                // Entry is a directory marker blob, but directory has already been reported
                                //
                                log.info("Skipping processing if directory marker for already reported directory:", entry["path_display"]);
                            }
                            else
                            {
                                // Process any ancestor directories of the blob encountered
                                //
                                if ((entry[".tag"] !== "folder") && processDir(path.dirname(entry["path_display"])))
                                {
                                    break;
                                }

                                if (onEntry(entry))
                                {
                                    break;
                                }
                            }

                        }
                    }

                    callback(err);
                });
            }
            else // non-recursive
            {
                // !!! Implement - For now we report the virtual directories first, which means we'd only report a directory
                //     marker object for an empty directory.
                //
                // When we do a non-recursive listing, we don't get any information (explicit or implicit) about directories
                // contained in the given directory.  Because we want to support both our directory marker objects as well
                // as Azure "virtual" directories, we have to list blob directories (to get any Azure virtual directories)
                // in addition to listing the blobs (which will also contain any directory marker blobs).
                //
                // If we have a directory marker blob, that should take precedence over any virtual directory (and we can
                // only report a directory once, so we must not report the virtual directory if a directory marker exists
                // for the same sub-directory).  
                //
                log.info("Listing blobs at prefix:", dirPath);

                async.series(
                [
                    function(done)
                    {
                        // This will get the (virtual) directories in the specified directory
                        //
                        var dirOptions = {};
                        blobService.listBlobDirectoriesSegmentedWithPrefix(params.container, dirPath + "/", null, dirOptions, function(err, result, response)
                        {
                            log.info("listBlobDirectories result:", result);
                            if (!err)
                            {
                                for (var i = 0; i < result.entries.length; i++)
                                {
                                    var entry = getEntryDetails(result.entries[i]);
                                    pathsProcessed[entry["path_display"]] = true;
                                    if (onEntry(entry))
                                    {
                                        break;
                                    }
                                }
                            }
                            done(err);
                        });
                    },
                    function(done)
                    {
                        // This will get the blobs in the specified directory
                        //
                        var options = { delimiter: "/" };
                        blobService.listBlobsSegmentedWithPrefix(params.container, dirPath + "/", null, options, function(err, result, response)
                        {
                            if (!err)
                            {
                                log.info("Got result:", result);
                                for (var i = 0; i < result.entries.length; i++)
                                {
                                    var entry = getEntryDetails(result.entries[i]);
                                    if ((entry[".tag"] === "folder") && pathsProcessed[entry["path_display"]])
                                    {
                                        // Entry is a directory marker blob, but directory has already been reported
                                        log.info("Skipping processing of directory marker for already reported directory:", entry["path_display"]);
                                    }
                                    else
                                    {
                                        if (onEntry(entry))
                                        {
                                            break;
                                        }
                                    }
                                }
                            }
                            done(err);
                        });
                    }
                ],
                function(err)
                {
                    callback(err);
                });
            }
        },
        getDirectoryMetaData: function(dirPath, callback)
        {
            // Try to get metadata for a directory marker blob
            //
            this.getObjectMetaData(getDirectoryMarkerFilename(dirPath), function(err, entry)
            {
                if (!err && !entry)
                {
                    // The directory marker blob was not found, but this may still be a valid virtual directory.
                    // Check it and see.  If it exists as a "virtual" directory (any object has this directory
                    // in its path), then return an entry for the virtual directory.
                    //
                    var parent = path.dirname(dirPath) + "/";
                    var dirOptions = {};
                    blobService.listBlobDirectoriesSegmentedWithPrefix(params.container, parent, null, dirOptions, function(err, result, response)
                    {
                        log.info("getDirectoryMetaData listBlobDirectories result:", result);
                        if (!err)
                        {
                            for (var i = 0; i < result.entries.length; i++)
                            {
                                var entry = getEntryDetails(result.entries[i]);
                                if (entry["path_display"] === dirPath)
                                {
                                    callback(null, entry);
                                    return;
                                }
                            }
                        }
                        callback(err, null);
                    });
                }
                else
                {
                    callback(err, entry);
                }
            });
        },
        getObjectMetaData: function(filename, callback)
        {
            var options = {};
            blobService.getBlobProperties(params.container, filename, options, function (err, result, response)
            {
                logResult("getBlobProperties", err, result, response);
                if (err)
                {
                    if (err.statusCode === 404)
                    {
                        callback(null, null);
                    }
                    else
                    {
                        callback(err);
                    }
                }
                else
                {
                    callback(null, getEntryDetails(result));
                }
            });
        },
        getObject: function(filename, requestHeaders, callback)
        {
            // createReadStream()
            //
            // http://azure.github.io/azure-storage-node/services_blob_blobservice.core.js.html#sunlight-1-line-1634
            //
            // ChunkStream
            //
            // http://azure.github.io/azure-sdk-for-node/azure-storage-legacy/latest/blob_internal_chunkStream.js.html
            //
            // !!! This is kind of a train wreck.  You don't get the response back on success until after the stream
            //     is consumed.  So we can't propagate any request status or headers like we do with the other transports.
            //     If there is an error, you will get that immediately on the error (without getting or consuming a stream).
            //     Presumably in the error case the response will contain the http request details, and presumably you will
            //     get a null stream back from the call.
            //
            // !!! The assumption is that if we get a stream back, then the request was a success (200 OK).  Our server logic
            //     does a metadata request immediately before this, and that will have the headers with the detail we might
            //     care about with respect to the blob itself (content-type, content-md5, content-length, etc).  Will will
            //     have to handle the range request information explicity (in fact we have to do that in the call anyway, as
            //     we would also need to convert any range indicators from the passed-in requestHeaders into options values
            //     indicating the desired range).
            //  
            var options = {};
            var readStream = blobService.createReadStream(params.container, filename, options, function (err, result, response)
            {
                // !!! If no err, the stream has been exhausted by now (due to a "quirk" in the implementation, the stream will
                //     be exhuasted whether or not it is read from - it's less a "stream" and more of an "abomination that craps
                //     out data events whenever it feels like it whether anyone is listening or not").
                //
                //     Anyway, it's too late to do something like: 
                // 
                //     callback(err, readStream, response.statusCode, null, response.headers);
                //
                logResult("createReadStream", err, result, response);
                if (err)
                {
                    callback(err);
                }
            });

            if (readStream)
            {
                log.info("getObject returning readStream");
                callback(null, readStream, 200, null, null); // No headers (preceding metadata call will have gotten most of them)
            }
        },
        putObject: function(filename, readStream, callback)
        {
            var options = {};
            var writeStream = blobService.createAppendBlobFromStream(params.container, filename, readStream, options, function (err, result, response)
            {
                logResult("createAppendBlobFromStream", err, result, response);
                callback(err);
            });
        },
        copyObject: function(filename, newFilename, callback)
        {
            var options = {};
            var sourceUri = blobService.getUrl(params.container, filename);
            blobService.startCopyBlob(sourceUri, params.container, newFilename, options, function (err, result, response)
            {
                logResult("startCopyBlob", err, result, response);
                callback(err);
            });
        },
        deleteObject: function(filename, callback)
        {
            var options = {};
            blobService.deleteBlob(params.container, filename, options, function (err, result, response)
            {
                logResult("deleteBlob", err, result, response);
                callback(err);
            });
        },
    }

    return driver;
}