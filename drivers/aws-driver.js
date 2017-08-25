// AWS / S3 driver
//
// https://aws.amazon.com/sdk-for-node-js/
//
var log = require('./../lib/logger').getLogger("aws-driver");

var aws = require('aws-sdk');

module.exports = function(params, config)
{
    log.info("Using AWS store, accessKeyId:", params.accessKeyId);

    var s3 = new aws.S3({
        accessKeyId: params.accessKeyId,
        secretAccessKey: params.secretAccessKey
    });

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