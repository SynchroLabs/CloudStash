// File store
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
    item.type = fStat.isFile() ? "object" : "directory"; // !!! DropBox uses .tag for type with value "file" or "folder"
    item.size = fStat.size;

    return item;
}

module.exports = function(params)
{
    var basePath = params.basePath;

    log.info("Using file store, basePath:", basePath);

    function toSafeLocalPath(fileName)
    {
        // path.posix.normalize will move any ../ to the front, and the regex will remove them.
        //
        var safeFilename = path.posix.normalize(fileName).replace(/^(\.\.[\/\\])+/, '');
        var filePath = path.posix.join(basePath, safeFilename); 

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
        doesObjectExist: function(filename, callback)
        {
            var filePath = toSafeLocalPath(filename);
            try
            {
                callback(null, fs.existsSync(filePath));
            }
            catch (err)
            {
                callback(err);
            }
        },
        createDirectory: function(dirPath, callback)
        {
            var fullPath = toSafeLocalPath(dirPath); 

            var entry = getEntryDetails(fullPath, dirPath);

            fs.mkdir(fullPath, function(err)
            {
                callback(err, entry);
            });
        },
        listDirectory: function(dirPath, callback)
        {
            var fullPath = toSafeLocalPath(dirPath); 

            fs.readdir(fullPath, function(err, files) 
            {
                log.info("Entries:", files);

                var entries = [];

                files.forEach(function(file)
                {
                    var entry = getEntryDetails(path.posix.join(fullPath, file), file);
                    entries.push(entry);
                });

                callback(null, entries);
            });
        },
        getObject: function(filename, callback)
        {
            var filePath = toSafeLocalPath(filename);

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
        putObject: function(filename, callback)
        {
            var filePath = toSafeLocalPath(filename); 
            
            // !!! May need to create parent dirs if they don't exist (outputFile used to do that for us)
            // !!! May need to use mode r+ (instead of default w) to overwrite existing file

            callback(null, fs.createWriteStream(filePath));
        },
        deleteObject: function(filename, callback)
        {
            // This will remove a file or a directory, so let's hope it's used correctly
            //
            var filePath = toSafeLocalPath(filename);

            var entry = getEntryDetails(filePath, filename);

            fs.remove(filePath, function(err){
                callback(err, entry)
            });
        }
    }

    return driver;
}
