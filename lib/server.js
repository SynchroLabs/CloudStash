var restify = require('restify');

var jwt = require('jsonwebtoken');
var restifyJwt = require('restify-jwt');

module.exports = function(_jwtSecret, config)
{
    var log = require('./../lib/logger').getLogger("server");

    var server = restify.createServer(
    { 
        name: 'MantaBox',
        log: log
    });
    
    server.use(restify.requestLogger());

    // The '/login' endpoint generates and returns a JWT token.  All endpoints except '/login' will 
    // require a JWT token (via Authorization header), and those endpoints can access the token payload
    // via req.user.  
    //
    server.use(restifyJwt({ secret: _jwtSecret}).unless({path: ['/login']}));

    // Restify bodyParser consumes the entire body in order to parse, so it prevents the ability to 
    // stream request bodies if used server-wide.  Instead, you can just add restify.bodyParser() to
    // the endpoint handler params when needed in order to call it explicity on those endpoints.
    //
    // server.use(restify.bodyParser());

    var fileDriver = require('../drivers/file-driver');
    var mantaDriver = require('../drivers/manta-driver');

    var driverConfig = config.get('driver');

    log.info("Adding driver for provider:", driverConfig.provider);

    var driver;
    if (driverConfig.provider === "file")
    {
        driver = new fileDriver(driverConfig);
    }
    else if (driverConfig.provider === "manta")
    {
        driver = new mantaDriver(driverConfig);
    }
    else
    {
        log.error("Unknown driver:", driverConfig.provider);
        return;
    }

    //
    // Auth endpoint (not from Dropbox API)
    //

    server.post('/login', restify.queryParser(), restify.bodyParser(), restify.authorizationParser(), function(req, res, next)
    {
        // Using queryParser and bodyParser will populate req.params from either URL params or
        // form encoded body.

        // The authorizationParser will process auth and populate req.authorization.  We support
        // basic auth via this mechanism.
        //
        if (req.authorization.basic)
        {
            req.params = req.authorization.basic;
        }

        req.log.info("Login attempt for username:", req.params.username);

        if (!req.params.username) {
            res.send(400, "Username required");
        }
        if (!req.params.password) {
            res.send(400, "Password required");
        }

        // !!! Check password

        // !!! Should token (JWT) contain all user data per Dropbox API, or should we fetch other stuff
        //     from auth db upon /users/get_account?
        //
        var token = jwt.sign({ account_id: "1234-BEEF" }, _jwtSecret);
        res.send(token);
        next();
    });

    //
    // "Users" API endpoints (api.dropboxapi.com/2/*)
    //

    server.post('/users/get_current_account', function(req, res, next)
    {
        req.log.info("Get current account for account_id: %s", req.user.account_id);
        // !!! May want to cleanse this (currently includes iat - "issued at time" for token)
        res.send(req.user); 
        next();
    });

    //
    // "Files" content API endpoints (content.dropboxapi.com/2/*)
    //

    var apiArgsHeader = "Dropbox-API-Arg".toLowerCase();

    function getApiArgs(req)
    {
        var apiArgs = {};

        if (req.headers && req.headers[apiArgsHeader])
        {
            // Dropbox-API-Arg contains params as JSON

            // !!! Verify that apiArgsHeader parses as JSON (throw detailed error if not)
            apiArgs = JSON.parse(req.headers[apiArgsHeader]);
        }
        else if (req.params)
        {
            apiArgs = req.params;
        }

        req.log.info("API args:", apiArgs);
        return apiArgs;
    }

    function filesDownload(req, res, next)
    {
        var apiArgs = getApiArgs(req);
        req.log.info("User", req.user);
        req.log.info("Download for account_id: %s from path: %s", req.user.account_id, apiArgs.path);

        driver.getObject(apiArgs.path, function(err, stream)
        {
            res.writeHead(200, 
            {
                'Content-Type': 'application/octet-stream'
            });
            stream.pipe(res); // Pipe store object contents to output
            stream.on('end',function() 
            {
                req.log.info("Stream written, returning result");
            });

            next();
        });
    }

    // Download supports GET with path in query params, as well as POST with path in API args header
    //
    server.get('/files/download', restify.queryParser(), filesDownload);
    server.post('/files/download', filesDownload);

    function filesUpload(req, res, next)
    {
        var apiArgs = getApiArgs(req);
        req.log.info("Upload for account_id: %d to path: %s", req.user.account_id, apiArgs.path);

        driver.putObject(apiArgs.path, function(err, stream)
        {
            req.pipe(stream); // Pipe request body contents to object in store
            req.once('end', function () 
            {
                req.log.info("Stream read, returning result");
                res.send({ name: apiArgs.path });
            });

            next();
        });
    }

    server.post('/files/upload', filesUpload);

    //
    // "Files" API endpoints (api.dropboxapi.com/2/*)
    //

    // !!! TODO - First pass for basic functionality
    //
    // '/files/copy'
    // '/files/move'
    // '/files/delete'
    // '/files/create_folder'
    // '/files/list_folder'
    // '/files/search'

    return server;
}
