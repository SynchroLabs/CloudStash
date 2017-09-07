var async = require('async');
var path = require('path');
var stream = require('stream');

var lodash = require('lodash');
var uuidv4 = require('uuid/v4');

// This module sits in front of the driver bridge and applies/removes users path elements as appropriate.
//
module.exports = function(config, driverBridge)
{
    var log = require('./logger').getLogger("user-bridge");

    function toSafePath(filePath)
    {
        // path.posix.normalize will move any ../ to the front, and the regex will remove them.
        //
        var safePath = path.posix.normalize(filePath).replace(/^(\.\.[\/\\])+/, '');
        return safePath;
    }

    function toSafeUserPath(account_id, app_id, filePath)
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

    function resolveEntryPaths(account_id, app_id, entry)
    {
        var userPathPrefix = '/' + account_id;
        if (app_id)
        {
            userPathPrefix = path.posix.join(userPathPrefix, app_id);
        }

        entry.path_lower = "/" + path.relative(userPathPrefix.toLowerCase(), entry.path_lower);
        entry.path_display = "/" + path.relative(userPathPrefix, entry.path_display);

        return entry
    }

    var bridge =
    {
        getEntrySortKey: function(entry)
        {
            return driverBridge.getEntrySortKey(entry);
        },
        getCursorItem: function(entry)
        {
            return driverBridge.getCursorItem(entry);
        },
        isCursorItemNewer: function(item1, item2)
        {
            return driverBridge.isAnyCursorItemNewer(item1, item2);
        },
        createDirectory: function (user, dirPath, callback)
        {
            var userDirPath = toSafeUserPath(user.account_id, user.app_id, dirPath); 
            driverBridge.createDirectory(userDirPath, callback);
        },
        deleteDirectory: function (user, dirPath, callback)
        {
            var userDirPath = toSafeUserPath(user.account_id, user.app_id, dirPath); 
            driverBridge.deleteDirectory(userDirPath, callback);
        },
        getDirectoryMetaData: function(user, dirPath, callback)
        {
            var userDirPath = toSafeUserPath(user.account_id, user.app_id, dirPath); 
            driverBridge.getDirectoryMetaData(userDirPath, function(err, entry)
            {
                if (entry)
                {
                    entry = resolveEntryPaths(user.account_id, user.app_id, entry)
                }
                callback(err, entry)
            });
        },
        getObjectMetaData: function (user, filename, callback)
        {
            var userFilePath = toSafeUserPath(user.account_id, user.app_id, filename); 
            driverBridge.getObjectMetaData(userFilePath, function(err, entry)
            {
                if (entry)
                {
                    entry = resolveEntryPaths(user.account_id, user.app_id, entry)
                }
                callback(err, entry)
            }); 
        },
        getMetaData: function(user, fileOrDirPath, type, callback)
        {
            var userFileOrDirPath = toSafeUserPath(user.account_id, user.app_id, fileOrDirPath); 
            driverBridge.getMetaData(userFileOrDirPath, type, function(err, entry)
            {
                if (entry)
                {
                    entry = resolveEntryPaths(user.account_id, user.app_id, entry)
                }
                callback(err, entry);
            });
        },
        getObject: function(user, filename, requestHeaders, callback)
        {
            var userFilePath = toSafeUserPath(user.account_id, user.app_id, filename); 
            driverBridge.getObject(userFilePath, requestHeaders, callback);
        },
        putObject: function(user, filename, readStream, callback)
        {
            var userFilePath = toSafeUserPath(user.account_id, user.app_id, filename); 
            driverBridge.putObject(userFilePath, readStream, callback);
        },
        copyObject: function(user, filename, newFilename, callback)
        {
            var userFilePath = toSafeUserPath(user.account_id, user.app_id, filename); 
            var userNewFilePath = toSafeUserPath(user.account_id, user.app_id, newFilename);

            driverBridge.copyObject(userFilePath, userNewFilePath, callback);
        },
        moveObject: function(user, filename, newFilename, callback)
        {
            var userFilePath = toSafeUserPath(user.account_id, user.app_id, filename); 
            var userNewFilePath = toSafeUserPath(user.account_id, user.app_id, newFilename);

            driverBridge.moveObject(userFilePath, userNewFilePath, callback);
        },
        deleteObject: function(user, filename, callback)
        {
            var userFilePath = toSafeUserPath(user.account_id, user.app_id, filename); 
            driverBridge.deleteObject(userFilePath, callback);
        },
        traverseDirectory: function(user, dirPath, recursive, onEntry, callback)
        {
            var userDirPath = toSafeUserPath(user.account_id, user.app_id, dirPath);

            function onEntryResolvePath(entry)
            {
                return onEntry(resolveEntryPaths(user.account_id, user.app_id, entry));
            }

            driverBridge.traverseDirectory(userDirPath, recursive, onEntryResolvePath, callback);
        },
        listFolderUsingCursor: function(user, dirPath, recursive, limit, cursor, callback)
        {
            var userDirPath = toSafeUserPath(user.account_id, user.app_id, dirPath); 
            driverBridge.listFolderUsingCursor(userDirPath, recursive, limit, cursor, function(err, items, hasMore, cursorItem)
            {
                if (items)
                {
                    for (var i = 0; i < items.length; i++)
                    {
                        items[i] = resolveEntryPaths(user.account_id, user.app_id, items[i]);
                    }
                }
                callback(err, items, hasMore, cursorItem);
            });
        },
        getLatestCursorItem: function(user, dirPath, recursive, callback)
        {
            var userDirPath = toSafeUserPath(user.account_id, user.app_id, dirPath);
            driverBridge.getLatestCursorItem(userDirPath, recursive, callback);
        },
        isAnyCursorItemNewer: function(user, dirPath, recursive, cursorItem, callback)
        {
            var userDirPath = toSafeUserPath(user.account_id, user.app_id, dirPath); 
            driverBridge.isAnyCursorItemNewer(userDirPath, recursive, cursorItem, callback);
        },
        getObjectMetaDataWithRetry: function(user, filename, isFolder, callback)
        {
            var userFilePath = toSafeUserPath(user.account_id, user.app_id, filename); 
            driverBridge.getObjectMetaDataWithRetry(userFilePath, isFolder, function(err, entry)
            {
                if (entry)
                {
                    entry = resolveEntryPaths(user.account_id, user.app_id, entry)
                }
                callback(err, entry);
            });
        },
        startMultipartUpload: function(user, readStream, callback)
        {
            var uploadPath = toSafeUserPath(user.account_id, null, "uploads"); 
            driverBridge.startMultipartUpload(uploadPath, readStream, callback);
        },
        multipartUpload: function(user, readStream, uploadId, offset, callback)
        {
            var uploadPath = toSafeUserPath(user.account_id, null, "uploads"); 
            driverBridge.multipartUpload(uploadPath, readStream, uploadId, offset, callback);
        },
        finishMultipartUpload: function(user, uploadId, filename, callback)
        {
            var uploadPath = toSafeUserPath(user.account_id, null, "uploads"); 
            var userFileName = toSafeUserPath(user.account_id, user.app_id, filename); 
            driverBridge.finishMultipartUpload(uploadPath, uploadId, userFileName, callback);
        }
    }

    return bridge;
}
