var restify = require('restify');

var jwt = require('jsonwebtoken');
var restifyJwt = require('restify-jwt');

module.exports = function(_jwtSecret, config)
{
    var logger = require('bunyan').createLogger({name: "MantaBoxServer"});

    var server = restify.createServer({ name: 'MantaBox'});
    server.use(restify.bodyParser());

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

    server.post('/login', function(req, res, next)
    {
        if (!req.body.username) {
            res.send(400, "Username required");
        }
        if (!req.body.password) {
            res.send(400, "Username required");
        }

        // !!! Check password

        // !!! Should token contain all user data per Dropbox API, or should we fetch other stuff
        //     from auth db upon /users/get_account?
        //
        var token = jwt.sign({ account_id: 1234 }, _jwtSecret);
        res.send(token);
        next();
    });

    server.post('/users/get_account', function(req, res, next)
    {
        logger.info("Get account for user: %s, userid: %d", req.user.username, req.user.userid);
        // !!! May want to cleanse this (currently includes iat - "issued at time" for token)
        res.send(req.user); 
        next();
    });

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

    server.post('/files/download', function(req, res, next)
    {
        var apiArgs = getApiArgs(req);
        logger.info("Download for userid: %d to path: %s", req.user.userid, apiArgs.path);

        driver.getObject(apiArgs.path, function(err, contents)
        {
            logger.info("Returning content (length: %d)", contents.length);
            res.send(contents);
            next();
        });
    });

    server.post('/files/upload', function(req, res, next)
    {
        var apiArgs = getApiArgs(req);
        logger.info("Upload for userid: %d to path: %s", req.user.userid, apiArgs.path);

        driver.putObject(apiArgs.path, req.body, function(err, contents)
        {
            logger.info("Content written");
            res.send({ name: apiArgs.path });
            next();
        });

        next();
    });

    return server;
}
