var path = require('path');
var nconf = require('nconf');

// Note: Since we need the config to configure the logger, we can't use the logger here.

// Get config - precendence: command line (represented by "overrides" from caller), environment, JSON config file, defaults
//
exports.getConfig = function(configFile, overrides)
{
    var conf = new nconf.Provider();

    // 1) Overrides (from the command line)
    //
    if (overrides)
    {
        conf.overrides(overrides);
    }

    // 2) Environment vars
    //
    // nconf.env();
    //
    // Parse env vars that start with "CLOUDSTASH__", with segments separated by "__", remove the prefix, put the values
    // into a memory store, and make it read-only...
    //
    conf.use("memory");
    var memStore = conf.stores["memory"];
    var mountsCreated = false;
    Object.keys(process.env).filter(function (key)
    {
        // We're only going to take env variables that start with "CLOUDSTASH__"...
        //
        return key.indexOf("CLOUDSTASH__") == 0;
    }).forEach(function (key) 
    {
        // Convert env var string values of true/false/null to native values
        //
        var value = process.env[key];
        if (value == "true")
        {
            value = true;
        }
        else if (value == "false")
        {
            value = false;
        }
        else if (value == "null")
        {
            value = null;
        }

        // We're going to use "__" as the separator for the env variable name, and we're going to prune off
        // the "CLOUDSTASH" part, and then create a normal colon-separated path from the remaining parts...
        //
        var key = key.split("__").slice(1).join(":");

        if ((key.indexOf("mounts:") == 0) && !mountsCreated)
        {
            memStore.set("mounts", []);
            mountsCreated = true;
        }

        memStore.set(key, value);
    });
    memStore.readOnly = true;

    // 3) JSON config file
    //
    if (configFile)
    {
        conf.configDetails = "Using configuration file specified on command line: " + configFile;
        conf.file('config.json', configFile);
    }
    else if (process.env['CLOUDSTASH_CONFIG'])
    {
        conf.configDetails = "Using configuration file specified in CLOUDSTASH_CONFIG: " + process.env['CLOUDSTASH_CONFIG'];
        conf.file('config.json', process.env['CLOUDSTASH_CONFIG']);
    }
    else
    {
        conf.configDetails = "Using default configuration file: config.json";
        conf.file('config.json', 'config.json');
    }
    
    // 4) Default values
    //
    var defaults = 
    {
        'PORT': 80,
        'MAX_CONCURRENCY': 4,      // Max number of parallel threads in async operations
        'MAX_INTERACTIVE': 10000,  // Max number of entries allowed in non-batch operations before we return too_many_files
        'DEFAULT_LIST_FOLDER_LIMIT': 725, // This is the Dropbox default
        'LOG_LEVEL': 'info', 
    }

    conf.defaults(defaults);

    return conf;
}