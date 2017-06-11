// File store
//
// https://nodejs.org/api/fs.html
// https://github.com/jprichardson/node-fs-extra
//
var fs = require('fs-extra');
var path = require('path');

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

    function toSafeLocalPath(user, fileName)
    {
        // path.posix.normalize will move any ../ to the front, and the regex will remove them.
        //
        var safeFilename = path.posix.normalize(fileName).replace(/^(\.\.[\/\\])+/, '');
        var filePath = path.posix.join(basePath, user.account_id, user.app_id, safeFilename); 

        if (path.sep != '/')
        {
            // Replace forward slash with local platform seperator
            //
            filePath = filePath.replace(/[\/]/g, path.sep);
        }

        return filePath;
    }

    var driver = 
    {
        provider: "file",
        doesObjectExist: function(user, filename, callback)
        {
            var filePath = toSafeLocalPath(user, filename);
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
            var fullPath = toSafeLocalPath(user, dirPath); 

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
            var fullPath = toSafeLocalPath(user, dirPath); 

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
            var filePath = toSafeLocalPath(user, filename);

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
            var filePath = toSafeLocalPath(user, filename); 

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
            var filePath = toSafeLocalPath(user, filename); 
            var newFilePath = toSafeLocalPath(user, newFilename); 
            
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
            var filePath = toSafeLocalPath(user, filename); 
            var newFilePath = toSafeLocalPath(user, newFilename); 

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
            var filePath = toSafeLocalPath(user, filename);

            var entry = getEntryDetails(filePath, filename);

            fs.remove(filePath, function(err)
            {
                callback(err, entry)
            });
        }
    }

    return driver;
}
