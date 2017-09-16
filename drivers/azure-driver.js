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
// In our application, we need the ability to create a directory that might be empty, and we will likely need to
// attach metadata to that directory at some point.  The current design for this is to create an object representing
// the directory.  We cannot create an object with a path then ends in "/" (as we do in S3 to denote a directory),
// so the plan is to create a file "in" the directory we are creating and name it ":" (the colon is not an allowed
// filename character in most file systems, so this should avoid conflict with any user file object).
//
// One issue with this design is that these ":" directory objects will show up as objects in the various Azure
// UX implementations.  This isn't really a problem, as we're only worried about the presentation of the Azure
// storage contents through our own application, but it is something to note.
//
// In addition, when a directory is created, we need to create any intermediate directory markers that do not
// already exist.
//
var log = require('./../lib/logger').getLogger("azure-driver");

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
        // azureObject
        /* 
        {
            name: '1234-BEEF/000001/IMG_0746.jpg',
            lastModified: 'Tue, 12 Sep 2017 01:53:20 GMT',
            etag: '0x8D4F9810B79AA98',
            contentLength: '1307098',
            contentSettings: [Object],
            blobType: 'BlockBlob',
            lease: [Object],
            serverEncrypted: 'false' 
        }
        */

        log.info("Got azureObject:", azureObject)

        var item = { };
        item[".tag"] = "file";

        var fullpath = azureObject.name;
        if (fullpath.lastIndexOf(_directoryMetadataFilename) == fullpath.length-1)
        {
            item[".tag"] = "folder";
            fullpath = fullpath.slice(0, -2);
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

    var driver = 
    {
        provider: "azure",
        createDirectory: function(dirPath, callback)
        {
            var options = {};
            // !!! Must create any parent directories that don't exist...
            blobService.createAppendBlobFromText(params.container, dirPath + "/" + _directoryMetadataFilename, "", options, function (err, result, response)
            {
                logResult("createAppendBlobFromText (createDirectory)", err, result, response);
                callback(err);
            });
        },
        deleteDirectory: function(dirPath, callback)
        {
            this.deleteObject(dirPath + "/" + _directoryMetadataFilename, callback);
        },
        traverseDirectory: function(dirPath, recursive, onEntry, callback)
        {
            // !!! Combination of listBlobsSegmentedWithPrefix (and delimiter of '/') and listBlobDirectoriesSegmentedWithPrefix
            //
            // !!! With our directory marker files, we don't need to do listBlobDirectoriesSegmentedWithPrefix, since the
            //     "directories" will show us as file objects (though this will then *only* work with directories/objects
            //     create from our app - and not on storage populated some other way - we could do both to be safe, maybe
            //     driven by a config option - revisit).
            //
            log.info("Listing blobs at prefix:", dirPath);

            var options = {};
            if (!recursive)
            {
                options.delimiter = "/"
            }

            blobService.listBlobsSegmentedWithPrefix(params.container, dirPath + "/", null, options, function(err, result, response)
            {
                if (err)
                {
                    log.error("Error on traverseDirectory:", err);
                    callback(err);
                }
                else
                {
                    // result in the form:
                    /*
                    { 
                        entries:
                        [ 
                            BlobResult 
                            {
                                name: '1234-BEEF/000001/IMG_0746.jpg',
                                lastModified: 'Tue, 12 Sep 2017 01:53:20 GMT',
                                etag: '0x8D4F9810B79AA98',
                                contentLength: '1307098',
                                contentSettings: [Object],
                                blobType: 'BlockBlob',
                                lease: [Object],
                                serverEncrypted: 'false' 
                            },
                            BlobResult 
                            {
                                name: '1234-BEEF/000001/IMG_0747.jpg',
                                lastModified: 'Tue, 12 Sep 2017 01:53:36 GMT',
                                etag: '0x8D4F98114DEC1AC',
                                contentLength: '1780171',
                                contentSettings: [Object],
                                blobType: 'BlockBlob',
                                lease: [Object],
                                serverEncrypted: 'false'
                            } 
                        ],
                        continuationToken: null 
                    }

                    // Or when using listBlobDirectoriesSegmentedWithPrefix:
                    { 
                        entries:
                        [ 
                            BlobResult 
                            { 
                                name: 'Photos/' 
                            }
                        ],
                        continuationToken: null
                    }
                    */

                    log.info("Got result:", result);

                    for (var i = 0; i < result.entries.length; i++)
                    {
                        var entry = getEntryDetails(result.entries[i]);
                        if (onEntry(entry))
                        {
                            break;
                        }
                    }

                    callback();
                }
            });
        },
        getDirectoryMetaData: function(dirPath, callback)
        {
            this.getObjectMetaData(dirPath + "/" + _directoryMetadataFilename, callback);
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
            //     Presumably in the erro case the response will contain the http request details, and presumably you will
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