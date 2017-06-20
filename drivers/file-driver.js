// File store
//
// https://nodejs.org/api/fs.html
// https://github.com/jprichardson/node-fs-extra
//
var fs = require('fs-extra');
var path = require('path');

var uuidv4 = require('uuid/v4');

var log = require('./../lib/logger').getLogger("file-driver");

// Directory lising object format:
//
// [
//   { type: "file", name: "foo.txt" },
//   { type: "directory", name: "bar", objects: [
//       { type: "file", name: "baz.txt" }  
//   ]}
// ]
//
function getObjects(dirPath)
{
    var output = [];

    var fileObjects = fs.readdirSync(dirPath);
    for (var i = 0; i < fileObjects.length; i++)
    {
        log.debug("Found file object: %s on path: %s", fileObjects[i], dirPath);
        var objPath = path.join(dirPath, fileObjects[i]);
        var stats = fs.statSync(objPath);
        if (stats.isDirectory())
        {
            output.push({ type: "directory", name: fileObjects[i], objects: getObjects(objPath) });
        }
        else
        {
            output.push({ type: "file", name: fileObjects[i] });
        }
    }

    return output;
}

function getEntryDetails(fullpath, filename)
{
    var item = { name: filename };
    var fStat = fs.statSync(fullpath);
    item[".tag"] = fStat.isFile() ? "file" : "folder";
    item.size = fStat.size;

    return item;
}

module.exports = function(params)
{
    var basePath = params.basePath;

    log.info("Using file store, basePath:", basePath);

    function toSafePath(filePath)
    {
        // path.posix.normalize will move any ../ to the front, and the regex will remove them.
        //
        var safePath = path.posix.normalize(filePath).replace(/^(\.\.[\/\\])+/, '');

        if (path.sep != '/')
        {
            // Replace forward slash with local platform seperator
            //
            safePath = filePath.replace(/[\/]/g, path.sep);
        }

        return safePath;
    }

    function toSafeLocalUserPath(user, filePath)
    {
        return toSafePath(path.posix.join(basePath, user.account_id, filePath)); 
    }

    function toSafeLocalUserAppPath(user, filePath)
    {
        return toSafePath(path.posix.join(basePath, user.account_id, user.app_id, filePath)); 
    }

    var driver = 
    {
        provider: "file",
        doesObjectExist: function(user, filename, callback)
        {
            var filePath = toSafeLocalUserAppPath(user, filename);
            try
            {
                callback(null, fs.existsSync(filePath));
            }
            catch (err)
            {
                callback(err);
            }
        },
        createDirectory: function(user, dirPath, callback)
        {
            var fullPath = toSafeLocalUserAppPath(user, dirPath); 

            fs.mkdirs(fullPath, function(err)
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    var entry = getEntryDetails(fullPath, dirPath);
                    callback(err, entry);
                }
            });
        },
        listDirectory: function(user, dirPath, callback)
        {
            var fullPath = toSafeLocalUserAppPath(user, dirPath); 

            fs.readdir(fullPath, function(err, files) 
            {
                // If the error is 'not found' and the dir in question is the root dir, we're just
                // going to ignore that and return an empty dir lising (just means we haven't created
                // this user/app path yet because it hasn't been used yet).
                //
                if (err && ((err.code !== 'ENOENT') || (dirPath !== '')))
                {
                    callback(err);
                }

                var entries = [];

                if (files)
                {
                    log.info("Entries:", files);

                    files.forEach(function(file)
                    {
                        var entry = getEntryDetails(path.posix.join(fullPath, file), file);
                        entries.push(entry);
                    });
                }

                callback(null, entries);
            });
        },
        getObject: function(user, filename, callback)
        {
            var filePath = toSafeLocalUserAppPath(user, filename);

            try
            {
                var stats = fs.statSync(filePath);
                if (stats.isDirectory())
                {
                    var dir = getObjects(filePath);
                    callback(null, dir);
                }
                else
                {
                    callback(null, fs.createReadStream(filePath));
                }
            }
            catch (err)
            {
                if (err.code === 'ENOENT')
                {
                    // We return null content to indicate "Not found"
                    err = null;
                }
                callback(err, null);
            }
        },
        putObject: function(user, filename, callback)
        {
            var filePath = toSafeLocalUserAppPath(user, filename); 

            fs.ensureDir(path.dirname(filePath), function(err)
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    // !!! May need to use mode r+ (instead of default w) to overwrite existing file
                    //
                    callback(null, fs.createWriteStream(filePath));
                }
            });
        },
        copyObject: function(user, filename, newFilename, callback)
        {
            var filePath = toSafeLocalUserAppPath(user, filename); 
            var newFilePath = toSafeLocalUserAppPath(user, newFilename); 
            
            fs.copy(filePath, newFilePath, function(err) // Creates directories as needed
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    callback(err, getEntryDetails(newFilePath, newFilename));
                }
            });
        },
        moveObject: function(user, filename, newFilename, callback)
        {
            var filePath = toSafeLocalUserAppPath(user, filename); 
            var newFilePath = toSafeLocalUserAppPath(user, newFilename); 

            fs.move(filePath, newFilePath, function(err) // Creates directories as needed
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    callback(err, getEntryDetails(newFilePath, newFilename));
                }
            });
        },
        deleteObject: function(user, filename, callback)
        {
            // This will remove a file or a directory, so let's hope it's used correctly
            //
            var filePath = toSafeLocalUserAppPath(user, filename);

            var entry = getEntryDetails(filePath, filename);

            fs.remove(filePath, function(err)
            {
                callback(err, entry)
            });
        },
        startMultipartUpload: function(user, callback)
        {
            // File name convention:
            //
            //    <user>/uploads/<uuid>/<offset>.bin 
            //
            var uploadId = uuidv4();

            var uploadPath = toSafeLocalUserPath(user, path.join("uploads", uploadId, "0.bin"));

            fs.ensureDir(path.dirname(uploadPath), function(err)
            {
                if (err)
                {
                    callback(err);
                }
                else
                {
                    callback(null, uploadId, fs.createWriteStream(uploadPath));
                }
            });
        },
        multipartUpload: function(user, uploadId, offset, callback)
        {
            var uploadPath = toSafeLocalUserPath(user, path.join("uploads", uploadId, offset.toString() + ".bin"));
            callback(null, fs.createWriteStream(uploadPath));
        },
        finishMultipartUpload: function(user, uploadId, filename, callback)
        {
            var filePath = toSafeLocalUserAppPath(user, filename); 

            log.info("This is where we would join the file parts for upload '%s' and write to dest: %s", uploadId, filename);

            // !!! Implement
            //
            // !!! Get a directory listing of the files
            // !!! Sort them based on offset
            // !!! Verify that there are no holes
            // !!! Stream the files in order to destination file
            // !!! Pass getEntryDetails(filePath, filename) to callback
            //

            callback(null);
        }
    }

    return driver;
}
