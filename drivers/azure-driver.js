// Azure Storage driver
//
// https://github.com/Azure/azure-storage-node
//
var log = require('./../lib/logger').getLogger("azure-driver");

var azure = require('azure-storage');

var directoryMetadataFilename = "!";

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

    var driver = 
    {
        provider: "aws",
        createDirectory: function(user, dirPath, callback)
        {
            blobService.createAppendBlobFromText(params.container, blob, text [, options], callback)
        },
        traverseDirectory: function(user, dirPath, recursive, onEntry, callback)
        {
            // !!! Combination of listBlobsSegmentedWithPrefix (and delimiter of '/') and listBlobDirectoriesSegmentedWithPrefix
            //
            var options = {}; //{ delimiter: "/" };
            blobService.listBlobsSegmented(params.container, null, options, function(err, result, response)
            {
                if (err)
                {
                    log.error("Error on traverseDirectory:", err);
                    callback(err);
                }
                else
                {
                    // result in the form...
                    /*
                    { 
                        entries: // listBlobs
                        [ 
                            BlobResult 
                            {
                                name: 'IMG_0524.JPG',
                                lastModified: 'Tue, 05 Sep 2017 09:43:34 GMT',
                                etag: '0x8D4F4429339BDD7',
                                contentLength: '1950685',
                                contentSettings: [Object],
                                blobType: 'BlockBlob',
                                lease: [Object],
                                serverEncrypted: 'false' 
                            }
                        ],
                        continuationToken: null

                        entries: // listBlobDirectories
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
                    callback();
                }
            });
        },
        getObject: function(user, filename, requestHeaders, callback)
        {
        },
        putObject: function(user, filename, callback)
        {
        },
        copyObject: function(user, filename, newFilename, callback)
        {
        },
        deleteObject: function(user, filename, callback)
        {
        },
        getObjectMetaData: function(user, filename, callback)
        {
        }
    }

    return driver;
}