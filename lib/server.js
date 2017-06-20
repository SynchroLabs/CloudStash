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

    // ----

    // This handles exceptions thrown from any handler that are otherwise uncaught.  We mainly just
    // want to log it (without this handler, there is no automatic log event).
    //
    server.on('uncaughtException', function(req, res, route, err) 
    {
       log.error(err);
       res.send(err);
    });

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
        // !!! At very least, we should have an "expires" time (think about auth flow when that happens)
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

    function pipeRequest(req, stream, cb)
    {
        var errorSent = false;

        function onError(err)
        {
            if (!errorSent)
            {
                errorSent = true;
                req.unpipe();
                stream.end();
                cb(err);
            }
        }

        req.once('error', onError);
        stream.once('error', onError);

        stream.once('close', function(details) 
        {
            if (!errorSent)
            {
                cb(null, details);
            }
        });

        // Pipe request body contents to object in store
        //
        return req.pipe(stream); 
    }

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
                req.log.error(err, "Error starting upload");
                res.send(err); 
                return;
            }

            pipeRequest(req, stream, function(err, details)
            {
                if (err)
                {
                    req.log.error(err, "Error piping request to upload stream");
                    req.send(err);
                }
                else
                {
                    log.info("Piping request to upload stream for '%s' complete", apiArgs.path);

                    // !!! Details in the case of Manta is the full response from the Manta request
                    //

                    // !!! The entry info returned below needs to come from the driver
                    //
                    res.send({ ".tag": "file", name: apiArgs.path });
                }
            });

            next();
        });
    }

    server.post('/files/upload', filesUpload);

    // Multipart upload
    //

    function uploadSessionStart(req, res, next)
    {
        var apiArgs = getApiArgs(req);
        req.log.info("Upload session start for account_id: %s", req.user.account_id);

        // !!! Should process apiArgs.close == true (it seems like you still have to call finish, but this
        //     signals that no more parts are coming).

        driver.startMultipartUpload(req.user, function(err, sessionId, stream)
        {
            if (err)
            {
                req.log.error(err, "Error starting upload session start");
                res.send(err); 
                return;
            }

            pipeRequest(req, stream, function(err, details)
            {
                if (err)
                {
                    req.log.error(err, "Error piping request to upload session start stream");
                    req.send(err);
                }
                else
                {
                    log.info("Piping request to upload start stream succeeded");
                    res.send({ "session_id": sessionId });
                }
            });

            next();
        });
    }

    function uploadSessionAppend(apiArgs, req, res, next)
    {
        req.log.info("Append upload session %s, offset: %n", apiArgs.cursor.session_id, apiArgs.cursor.offset);

        driver.multipartUpload(req.user, apiArgs.cursor.session_id, apiArgs.cursor.offset, function(err, stream)
        {
            if (err)
            {
                req.log.error(err, "Error on upload session append");
                res.send(err); 
                return;
            }

            pipeRequest(req, stream, function(err, details)
            {
                if (err)
                {
                    req.log.error(err, "Error piping request to upload session append stream");
                    res.send(err);
                }
                else
                {
                    log.info("Piping request to upload session append stream succeeded");
                    res.send();
                }
            });

            next();
        });
    }

    function uploadSessionFinish(req, res, next)
    {
        var apiArgs = getApiArgs(req);
        req.log.info("End upload session %s, offset: %n", apiArgs.session_id, apiArgs.offset);

        driver.multipartUpload(req.user, apiArgs.cursor.session_id, apiArgs.cursor.offset, function(err, stream)
        {
            if (err)
            {
                req.log.error(err, "Error on upload session end");
                res.send(err); 
                return;
            }

            pipeRequest(req, stream, function(err, details)
            {
                if (err)
                {
                    req.log.error(err, "Error piping request to upload session end stream");
                    res.send(err);
                }
                else
                {
                    log.info("Piping request to upload session end stream succeeded");

                    driver.finishMultipartUpload(req.user, apiArgs.cursor.session_id, apiArgs.commit.path, function(err, item)
                    {
                        if (err)
                        {
                            req.log.error(err, "Error on session finish");
                            res.send(err); 
                            return;
                        }
                        else
                        {
                            res.send(item);
                        }
                    });
                }
            });

            next();
        });
    }

    // The multipart upload endpoints are "content" endpoints because start, append, and finish all have data
    // payload (file segments)
    //
    server.post('/files/upload_session/start', uploadSessionStart);
    server.post('/files/upload_session/append', function(req, res, next)
    {
        // Upgrade this to a v2 request
        //
        var apiArgs = getApiArgs(req);

        apiArgs = { "cursor": apiArgs };

        uploadSessionAppend(apiArgs, req, res, next);
    });
    server.post('/files/upload_session/append_v2', function(req, res, next)
    {
        uploadSessionAppend(getApiArgs(req), req, res, next);
    });
    server.post('/files/upload_session/finish', uploadSessionFinish);

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

    // !!! TODO - The rest of the Dropbox v2 API - Not implemented yet
    //
    function notImplemented(req, res, next)
    {
        req.log.error("API endpoint %s not implemented", req.path());
        throw new Error("Not implemented");
    }

    server.post('/files/search', notImplemented); // !!! First pass - basic functionality

    server.post('/files/alpha/get_metadata', notImplemented);
    server.post('/files/alpha/upload', notImplemented);
    server.post('/files/copy_batch', notImplemented);
    server.post('/files/copy_batch/check', notImplemented);
    server.post('/files/copy_reference/get', notImplemented);
    server.post('/files/copy_reference/save', notImplemented);
    server.post('/files/delete_batch', notImplemented);
    server.post('/files/delete_batch/check', notImplemented);
    server.post('/files/get_metadata', notImplemented);
    server.post('/files/get_preview', notImplemented);
    server.post('/files/get_temporary_link', notImplemented);
    server.post('/files/get_thumbnail', notImplemented);
    server.post('/files/list_folder/continue', notImplemented);
    server.post('/files/list_folder/get_latest_cursor', notImplemented);
    server.post('/files/list_folder/longpoll', notImplemented);
    server.post('/files/list_revisions', notImplemented);
    server.post('/files/move_batch', notImplemented);
    server.post('/files/move_batch/check', notImplemented);
    server.post('/files/permanently_delete', notImplemented);
    server.post('/files/properties/add', notImplemented);
    server.post('/files/properties/overwrite', notImplemented);
    server.post('/files/properties/remove', notImplemented);
    server.post('/files/properties/template/get', notImplemented);
    server.post('/files/properties/template/list', notImplemented);
    server.post('/files/properties/update', notImplemented);
    server.post('/files/restore', notImplemented);
    server.post('/files/save_url', notImplemented);
    server.post('/files/save_url/check_job_status', notImplemented);
    server.post('/files/upload_session/finish_batch', notImplemented);
    server.post('/files/upload_session/finish_batch/check', notImplemented);

    return server;
}
