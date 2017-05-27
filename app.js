var restify = require('restify');

var jwt = require('jsonwebtoken');
var restifyJwt = require('restify-jwt');

var mantaBoxConfig = require('./lib/config');
var pkg = require('./package.json');

var bunyan = require('bunyan');
var logger = bunyan.createLogger({name: "MantaBox"});

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

var server = restify.createServer({ name: 'MantaBox'});
server.use(restify.bodyParser());

// The '/login' endpoint generates and returns a JWT token.  All endpoints except '/login' will 
// require a JWT token (via Authorization header), and those endpoints can access the token payload
// via req.user.  
//
server.use(restifyJwt({ secret: _jwtSecret}).unless({path: ['/login']}));

server.post('/login', function(req, res, next)
{
    if (!req.body.username) {
        res.send(400, "Username required");
    }
    if (!req.body.password) {
        res.send(400, "Username required");
    }

    // !!! Check password

    var token = jwt.sign({ username: req.body.username, userid: 1234 }, _jwtSecret);
    res.send(token);
    next();
});

server.get('/echo/:message', function(req, res, next)
{
    logger.info("Request from user: %s, userid: %n", req.user.username, req.user.userid);
    res.send("You said: " + req.params.message);
    next();
})

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