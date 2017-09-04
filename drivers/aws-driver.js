// AWS / S3 driver
//
// https://aws.amazon.com/sdk-for-node-js/
//
var log = require('./../lib/logger').getLogger("aws-driver");

var path = require('path');

var aws = require('aws-sdk');

module.exports = function(params, config)
{
    log.info("Using AWS store, accessKeyId:", params.accessKeyId);

    var s3 = new aws.S3({
        accessKeyId: params.accessKeyId,
        secretAccessKey: params.secretAccessKey
    });

    log.debug('AWS S3 driver, user:: %s', params.user);

    function toSafePath(filePath)
    {
        // path.posix.normalize will move any ../ to the front, and the regex will remove them.
        //
        var safePath = path.posix.normalize(filePath).replace(/^(\.\.[\/\\])+/, '');
        return safePath;
    }

    function toSafeLocalPath(account_id, app_id, filePath)
    {
        if (app_id)
        {
            return path.posix.join(account_id, app_id, toSafePath(filePath)); 
        }
        else
        {
            return path.posix.join(account_id, toSafePath(filePath));
        }
    }

    function getEntryDetails(user, s3Object)
    {
        // S3 object
        /*
        { 
            Key: '1234-BEEF/000001/El Diablo_Head_RGB.png',
            LastModified: Mon Aug 28 2017 17:19:22 GMT-0700 (PDT),
            ETag: '"dec38eaf8d1114b8fc9ffa16c8417178"',
            Size: 306589,
            StorageClass: 'STANDARD',
            Owner: [Object] 
        }
        */

        log.info("Got s3Object:", s3Object)

        var item = { };
        item[".tag"] = "file";

        var fullpath = s3Object.Key;
        if (fullpath.lastIndexOf('/') == fullpath.length-1)
        {
            item[".tag"] = "folder";
            fullpath = fullpath.slice(0, -1);
        }

        var userPath = user.account_id;
        if (user.app_id)
        {
            userPath = path.posix.join(userPath, user.app_id);
        } 

        var displayPath = "/" + path.relative(userPath, fullpath);

        // Convert to Dropbox form
        //
        item["name"] = fullpath.split(path.sep).pop();

        item["path_lower"] = displayPath.toLowerCase();
        item["path_display"] = displayPath;
        item["id"] = displayPath; // !!! Required by Dropbox - String(min_length=1)

        item["server_modified"] = s3Object.LastModified; // !!! Can't imaging this is gonna work - also remove ms for Dropbox
        item["client_modified"] = item["server_modified"]; // !!! Required by Dropbox

        item["rev"] = "000000001"; // !!! Required by Dropbox - String(min_length=9, pattern="[0-9a-f]+")

        if (s3Object.Size)
        {
            item["size"] = s3Object.Size;
        }
        // item["content_hash"]

        return item;
    }

    var driver = 
    {
        provider: "aws",
        createDirectory: function(user, dirPath, callback)
        {
            var fullPath = toSafeLocalPath(user.account_id, user.app_id, dirPath) + "/";
            log.info("Creating folder:", fullPath);

            var options = { Bucket: params.bucket, Key: fullPath }
            s3.putObject(options, function(err, data)
            {
                console.log("Put dir:", data)
                callback(err);
            });
        },
        deleteDirectory: function(user, dirPath, callback)
        {
            this.deleteObject(user, dirPath + "/", callback);
        },
        getDirectoryMetaData: function(user, dirPath, callback)
        {
            this.getObjectMetaData(user, dirPath + "/", callback);
        },
        traverseDirectory: function(user, dirPath, recursive, onEntry, callback)
        {
            var fullPath = toSafeLocalPath(user.account_id, user.app_id, dirPath) + "/";
            log.info("Traversing path:", fullPath);

            var options = { Bucket: params.bucket, Prefix: fullPath }

            if (!recursive)
            {
                options.Delimiter = "/"
            }

            s3.listObjects(options, function(err, data)
            {
                if (err)
                {
                    console.log(err, err.stack); // an error occurred
                    callback(err);
                } 
                else
                {
                    console.log(data); // successful response

                    if (data.CommonPrefixes)
                    {
                        data.CommonPrefixes.forEach(function (prefixObject)
                        {
                            data.Contents.push({ Key: prefixObject.Prefix })
                        });
                    }

                    for (var i = 0; i < data.Contents.length; i++)
                    {
                        var object = data.Contents[i];
                        if (object.Key === fullPath)
                        {
                            // This is the directory itself
                            continue;
                        }
                        var entry = getEntryDetails(user, object);
                        if (onEntry(entry))
                        {
                            break;
                        }
                    }

                    callback();
                }
            });
        },
        getObject: function(user, filename, requestHeaders, callback)
        {
            var fullPath = toSafeLocalPath(user.account_id, user.app_id, filename);
            log.info("Getting object:", fullPath);

            var options = { Bucket: params.bucket, Key: fullPath }

            try 
            {
                // We want to get the statusCode and headers from this request before we call back with the stream...
                //
                // https://stackoverflow.com/questions/35782434/streaming-file-from-s3-with-express-including-information-on-length-and-filetype
                //
                // Calling createReadStream() issues a send() on the request, which will result in the 'httpHeaders'
                // event on the request object getting signalled at some point.  By processing that event, we can get
                // the statusCode, headers, and the stream all in one place to return to the caller.
                //
                var req = s3.getObject(options);
                var stream = req.createReadStream();
                req.on('httpHeaders', function (statusCode, headers) 
                {
                    // !!! We might want to look at >300 statusCode values here to send an error back instead.
                    //
                    callback(null, stream, statusCode, null, headers);
                });
                req.on('error', function(err)
                {
                    log.error("Error on getObject request", err);
                    callback(err);
                });
                stream.on('error', function(err)
                {
                    log.error("Error on getObject stream", err);
                    callback(err);
                });
            }
            catch (error)
            {
                // Catching NoSuchKey & StreamContentLengthMismatch
                log.error(error);
                callback(err);
            }
        },
        putObject: function(user, filename, readStream, callback)
        {
            var fullPath = toSafeLocalPath(user.account_id, user.app_id, filename);
            log.info("Putting object:", fullPath);

            var options = { Bucket: params.bucket, Key: fullPath, Body: readStream }

            s3.upload(options, function(err, data) 
            {
                log.info("putObject response:", data);
                callback(err, data);
            });
        },
        copyObject: function(user, filename, newFilename, callback)
        {
            var fullPathSrc = toSafeLocalPath(user.account_id, user.app_id, filename);
            var fullPathDst = toSafeLocalPath(user.account_id, user.app_id, newFilename);

            var options = { Bucket: params.bucket, CopySource: "/" + params.bucket + "/" + fullPathSrc, Key: fullPathDst }

            s3.copyObject(options, function(err, data) 
            {
                log.info("copyObject response:", data);
                callback(err);
            });
        },
        deleteObject: function(user, filename, callback)
        {
            var fullPath = toSafeLocalPath(user.account_id, user.app_id, filename);
            log.info("Deleting object:", fullPath);

            var options = { Bucket: params.bucket, Key: fullPath }

            s3.deleteObject(options, function(err, data) 
            {
                log.info("deleteObject response:", data);
                callback(err, data);
            });
        },
        getObjectMetaData: function(user, filename, callback)
        {
            var fullPath = toSafeLocalPath(user.account_id, user.app_id, filename);
            log.info("Getting object metadata:", fullPath);

            var options = { Bucket: params.bucket, Key: fullPath }

            s3.headObject(options, function(err, data)
            {
                if (err)
                {
                    if (err.code === 'NotFound')
                    {
                        callback(null, null);
                    }
                    else
                    {
                        log.error(err);
                        callback(err);
                    }
                }
                else 
                {
                    /*
                    data = {
                        AcceptRanges: "bytes", 
                        ContentLength: 3191, 
                        ContentType: "image/jpeg", 
                        ETag: "\"6805f2cfc46c0f04559748bb039d69ae\"", 
                        LastModified: <Date Representation>, 
                        Metadata: {
                        }, 
                        VersionId: "null"
                    }
                    */
                    log.info("Got object metadata:", data); // successful response

                    data.Key = fullPath;
                    data.Size = data.ContentLength;
                    callback(null, getEntryDetails(user, data));
                }
            });
        }
    }

    return driver;
}