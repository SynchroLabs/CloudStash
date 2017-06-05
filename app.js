var mantaBoxServer = require('./lib/server');
var mantaBoxConfig = require('./lib/config');
var pkg = require('./package.json');

// Process command line params
//
var commander = require('commander');
commander.version(pkg.version);
commander.option('-p, --port <n>', 'The port on which the MantaBox server will listen', parseInt);
commander.option('-c, --config <value>', 'Use the specified configuration file');
commander.parse(process.argv);

var overrides = {};

if (commander.port)
{
    overrides.PORT = commander.port;
}

var config = mantaBoxConfig.getConfig(commander.config, overrides);

var loggerModule = require('./lib/logger');
loggerModule.createMainLogger(config);

var log = loggerModule.getLogger("app");

log.info("MantaBox server v%s loading - %s", pkg.version, config.configDetails);

var _jwtSecret = "!!!super secret token that should be replaced with something private/secure!!!";

var server = mantaBoxServer(_jwtSecret, config);
if (!server)
{
    log.error("Failed to create server, exiting");
    process.exit();
}

server.listen(config.get('PORT'), function () 
{
    log.info('MantaBox listening on port:', this.address().port);
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
