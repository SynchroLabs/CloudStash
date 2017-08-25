// Azure Storage driver
//
// https://github.com/Azure/azure-storage-node
//
var log = require('./../lib/logger').getLogger("azure-driver");

var azure = require('azure-storage');

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

    var driver = 
    {
        provider: "aws",
        createDirectory: function(user, dirPath, callback)
        {
        },
        traverseDirectory: function(user, dirPath, recursive, onEntry, callback)
        {
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
        moveObject: function(user, filename, newFilename, callback)
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