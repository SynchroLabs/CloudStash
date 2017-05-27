var mantaBoxServer = require('./lib/server');
var mantaBoxConfig = require('./lib/config');
var pkg = require('./package.json');

var logger = require('bunyan').createLogger({name: "MantaBox"});

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

// !!! Is there any configurating we can/should do for Bunyan from our config?
//
// log4js.configure(config.get('LOG4JS_CONFIG'));

logger.info("MantaBox server v%s loading - %s", pkg.version, config.configDetails);

var _jwtSecret = "!!!super secret token that should be replaced with something private/secure!!!";

var server = mantaBoxServer(_jwtSecret);

server.listen(config.get('PORT'), function () 
{
    logger.info('MantaBox listening on port:', this.address().port);
});

process.on('SIGTERM', function ()
{
    logger.info('SIGTERM - preparing to exit.');
    process.exit();
});

process.on('SIGINT', function ()
{
    logger.info('SIGINT - preparing to exit.');
    process.exit();
});

process.on('exit', function (code)
{
    logger.info('Process exiting with code:', code);
});