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
        var userInfo = {
            app_id:     "000001",
            account_id: "1234-BEEF"
        }

        var token = jwt.sign(userInfo, _jwtSecret);
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

        driver.getObject(req.user, apiArgs.path, function(err, stream)
        {
            if (err)
            {
                // !!!
                req.log.error("Error on download:", err);
            }

            // !!! Propagate content-type and other headers from drivers that support them.
            //

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

        req.log.info("User", req.user);

        req.log.info("Upload for account_id: %s to path: %s", req.user.account_id, apiArgs.path);

        // !!! Pass content-type and other headers to drivers that support them
        //

        driver.putObject(req.user, apiArgs.path, function(err, stream)
        {
            if (err)
            {
                // !!!
                req.log.error("Error on upload:", err);
            }

            // !!! This needs to be looked out (make sure errors are caught from both drivers)
            //
            stream.once('error', function (err)
            {
                // !!! Send error response
                log.error(err);
            });

            stream.once('close', function (res) 
            {
                // In the case of Manta, res is the full http response from the Manta write.
                //
                log.info("close", res);
            });

            req.once('end', function () 
            {
                // !!! This is called when the req/upload stream is done being read, so this is probably
                //     not the right place for us to decide we've won (or not).
                //
                req.log.info("Stream read, returning result");
                res.send({ name: apiArgs.path });
            });

            req.pipe(stream); // Pipe request body contents to object in store

            next();
        });
    }

    server.post('/files/upload', filesUpload);

    //
    // "Files" API endpoints (api.dropboxapi.com/2/*)
    //

    // !!! Dropbox nomenclature is "folder" or "file" (either of which is considered an "entry")
    //

    function filesListFolder(req, res, next)
    {
        var apiArgs = getApiArgs(req);
        req.log.info("List folders for account_id: %s at path: %s", req.user.account_id, apiArgs.path);

        driver.listDirectory(req.user, apiArgs.path, function(err, items)
        {
            if (err)
            {
                // !!!
                req.log.error("Error on list folder:", err);
            }
            res.send({ entries: items });
            next();
        });
    }

    function filesDelete(req, res, next)
    {
        var apiArgs = getApiArgs(req);
        req.log.info("Delete file for account_id: %s at path: %s", req.user.account_id, apiArgs.path);

        driver.deleteObject(req.user, apiArgs.path, function(err, item)
        {
            if (err)
            {
                // !!!
                req.log.error("Error on delete:", err);
            }
            res.send(item);
            next();
        });
    }

    function filesCreateFolder(req, res, next)
    {
        var apiArgs = getApiArgs(req);
        req.log.info("Create folder for account_id: %s at path: %s", req.user.account_id, apiArgs.path);

        driver.createDirectory(req.user, apiArgs.path, function(err, item)
        {
            if (err)
            {
                // !!!
                req.log.error("Error on create folder:", err);
            }
            res.send(item);
            next();
        });
    }

    function filesCopy(req, res, next)
    {
        var apiArgs = getApiArgs(req);
        req.log.info("Copy for account_id: %s from path: %s to path: %s", req.user.account_id, apiArgs.from_path, apiArgs.to_path);

        driver.copyObject(req.user, apiArgs.from_path, apiArgs.to_path, function(err, item)
        {
            if (err)
            {
                // !!!
                req.log.error("Error on copy:", err);
            }
            res.send(item); // This appears to be the source item
            next();
        });
    }

    function filesMove(req, res, next)
    {
        var apiArgs = getApiArgs(req);
        req.log.info("Move for account_id: %s from path: %s to path: %s", req.user.account_id, apiArgs.from_path, apiArgs.to_path);

        driver.moveObject(req.user, apiArgs.from_path, apiArgs.to_path, function(err, item)
        {
            if (err)
            {
                // !!!
                req.log.error("Error on move:", err);
            }
            res.send(item); // This appears to be the source item
            next();
        });
    }

    server.post('/files/list_folder', filesListFolder);
    server.post('/files/delete', filesDelete);
    server.post('/files/create_folder', filesCreateFolder);
    server.post('/files/copy', filesCopy);
    server.post('/files/move', filesMove);

    // !!! TODO - First pass for basic functionality
    //
    // '/files/search'

    return server;
}
