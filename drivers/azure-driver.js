// Azure Storage driver
//
// https://github.com/Azure/azure-storage-node
//
// API docs: http://azure.github.io/azure-storage-node/BlobService.html
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

    var driver = 
    {
        provider: "azure",
        createDirectory: function(dirPath, callback)
        {
            var options = {};
            // !!! Must create any parent directories that don't exist...
            blobService.createAppendBlobFromText(params.container, dirPath + "/" + _directoryMetadataFilename, "", options, callback)
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
                options.Delimiter = "/"
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
        },
        getObject: function(filename, requestHeaders, callback)
        {
        },
        putObject: function(filename, callback)
        {
        },
        copyObject: function(filename, newFilename, callback)
        {
        },
        deleteObject: function(filename, callback)
        {
        },
    }

    return driver;
}