var restify = require('restify');

var jwt = require('jsonwebtoken');
var restifyJwt = require('restify-jwt');

module.exports = function(_jwtSecret, config)
{
    var logger = require('bunyan').createLogger({name: "MantaBoxServer"});

    var server = restify.createServer({ name: 'MantaBox'});
    
    // Restify bodyParser consumes the entire body in order to parse, so it prevents the ability to 
    // stream request bodies if used server-wide.  Instead, you can just add restify.bodyParser() to
    // the endpoint handler params when needed in order to call it explicity on those endpoints.
    //
    // !!! server.use(restify.bodyParser());

    // The '/login' endpoint generates and returns a JWT token.  All endpoints except '/login' will 
    // require a JWT token (via Authorization header), and those endpoints can access the token payload
    // via req.user.  
    //
    server.use(restifyJwt({ secret: _jwtSecret}).unless({path: ['/login']}));

    var fileDriver = require('../drivers/file-driver');
    var mantaDriver = require('../drivers/manta-driver');

    var driverConfig = config.get('driver');

    logger.info("Adding driver for provider:", driverConfig.provider);

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
        logger.error("Unknown driver:", driverConfig.provider);
        return;
    }

    server.post('/login', restify.bodyParser(), function(req, res, next)
    {
        // !!! What about params in URL?
        // !!! What about basic auth?
        
        if (!req.body.username) {
            res.send(400, "Username required");
        }
        if (!req.body.password) {
            res.send(400, "Username required");
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
    // "Users" API endpoints
    //

    server.post('/users/get_current_account', function(req, res, next)
    {
        logger.info("Get current account for account_id: %s", req.user.account_id);
        // !!! May want to cleanse this (currently includes iat - "issued at time" for token)
        res.send(req.user); 
        next();
    });

    //
    // "Files" API endpoints
    //

    var apiArgsHeader = "Dropbox-API-Arg".toLowerCase();

    function getApiArgs(req)
    {
        // !!! Verify that headers exist
        // !!! Verify that apiArgsHeader exists
        // !!! Verify that apiArgsHeader parses as JSON
        // !!! Test how Dropbox API responds to above errors and respond similarly

        // Dropbox-API-Arg contains params as JSON
        var apiArgs = JSON.parse(req.headers[apiArgsHeader]);
        logger.info("API args:", apiArgs);
        return apiArgs;
    }

    // !!! I think GET is also supported, as is passing path in url params - check Dropbox docs
    //
    server.post('/files/download', function(req, res, next)
    {
        var apiArgs = getApiArgs(req);
        logger.info("Download for account_id: %s to path: %s", req.user.account_id, apiArgs.path);

        driver.getObject(apiArgs.path, function(err, stream)
        {
            res.writeHead(200, 
            {
                'Content-Type': 'application/octet-stream'
            });
            stream.pipe(res); // Pipe store object contents to output
            stream.on('end',function() 
            {
                logger.info("Stream written, returning result");
            });

            next();
        });
    });

    server.post('/files/upload', function(req, res, next)
    {
        var apiArgs = getApiArgs(req);
        logger.info("Upload for account_id: %d to path: %s", req.user.account_id, apiArgs.path);

        driver.putObject(apiArgs.path, function(err, stream)
        {
            req.pipe(stream); // Pipe request body contents to object in store
            req.once('end', function () 
            {
                logger.info("Stream read, returning result");
                res.send({ name: apiArgs.path });
            });

            next();
        });
    });

    return server;
}
