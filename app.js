var cloudStashServer = require('./lib/server');
var cloudStashConfig = require('./lib/config');
var pkg = require('./package.json');

// Process command line params
//
var commander = require('commander');
commander.version(pkg.version);
commander.option('-p, --port <n>', 'The port on which the CloudStash server will listen', parseInt);
commander.option('-c, --config <value>', 'Use the specified configuration file');
commander.parse(process.argv);

var overrides = {};

if (commander.port)
{
    overrides.PORT = commander.port;
}

var config = cloudStashConfig.getConfig(commander.config, overrides);

var loggerModule = require('./lib/logger');
loggerModule.createMainLogger(config);

var log = loggerModule.getLogger("app");

log.info("CloudStash server v%s loading - %s", pkg.version, config.configDetails);

var _jwtSecret = config.get('TOKEN_SECRET');
if (!_jwtSecret)
{
    _jwtSecret = "!!!super secret token that should be replaced with something private/secure!!!";
    log.warn("TOKEN_SECRET not specified in configuration, using default (unsafe) token secret - DO NOT USE IN PRODUCTION");
}

var server = cloudStashServer(_jwtSecret, config);
if (!server)
{
    log.error("Failed to create server, exiting");
    process.exit();
}

server.listen(config.get('PORT'), function (err) 
{
    if (err)
    {
        log.error("CloudStash server failed in listen()", err);
    }
    else
    {
        log.info('CloudStash listening on port:', this.address().port);
    }
});

server.on('error', function(err)
{
    if (err.code === 'EACCES')
    {
        log.error("PORT specified (%d) already in use", config.get('PORT'));
    }
    else
    {
        log.error("CloudStash server error:", err);
    }
});

process.on('SIGTERM', function ()
{
    log.info('SIGTERM - preparing to exit.');
    process.exit();
});

process.on('SIGINT', function ()
{
    log.info('SIGINT - preparing to exit.');
    process.exit();
});

process.on('exit', function (code)
{
    log.info('Process exiting with code:', code);
});
