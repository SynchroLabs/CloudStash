var restify = require('restify');

var assert = require('assert-plus');

var restifyJwt = require('restify-jwt');
var restifyCorsMiddleware = require('restify-cors-middleware');

var async = require('async');
var fs = require('fs');
var path = require('path');
var stream = require('stream');
var util = require('util');
var uuidv4 = require('uuid/v4');

var lodash = require('lodash');

var account = require('./account');
var auth = require('./auth');
var csUtil = require('./util');
var dbErr = require('./error');

module.exports = function(_jwtSecret, config)
{
    var log = require('./../lib/logger').getLogger("server");

    var _maxConcurrency = config.get('MAX_CONCURRENCY');
    var _maxInteractive = config.get('MAX_INTERACTIVE'); // More than this many operations in a non-batch call will result in too_many_files
    var _maxLongPollJitter = config.get('MAX_LONGPOLL_JITTER_SECONDS');
    var _longPollInterval =  config.get('LONGPOLL_INTERVAL_MS');
    var _defaultListFolderLimit = config.get('DEFAULT_LIST_FOLDER_LIMIT');

    var tokenMgr = require('./token')(_jwtSecret);

    // SSL support
    //
    // For raw key/cert, use SSL_KEY and SSL_CERT.  To refer to key and/or cert files, use SSL_KEY_PATH and SSL_CERT_PATH.
    //
    // Note: It will generally be the case that SSL is terminated upstream from this server.  When an upstream proxy terminates SSL, it
    //       should add an "x-arr-ssl" header to the request to indicate to this server that the connection was secure (arr is for Application
    //       Request Routing).  The upstream proxy that terminates SSL should also either deny non-SSL requests or ensure that the "x-arr-ssl" 
    //       request header is not present on non-SSL requests.  Microsoft Azure terminates SSL and adds this header automatically.
    //
    // Note: This server will serve HTTP *OR* HTTPS, but not both.  This is by design.  HTTP should only be used for local development, or
    //       in production when SSL is terminated upstream.  There is no use case where serving both HTTP and HTTPS would be appropriate.
    //
    var sslOptions = { key: config.get("SSL_KEY"), cert: config.get("SSL_CERT") };

    if (!sslOptions.key)
    {
        var keyPath = config.get("SSL_KEY_PATH");
        if (keyPath)
        {
            sslOptions.key = fs.readFileSync(keyPath);
        }
    }

    if (!sslOptions.cert)
    {
        var certPath = config.get("SSL_CERT_PATH");
        if (certPath)
        {
            sslOptions.cert = fs.readFileSync(certPath);
        }
    }

    if (!sslOptions.key || !sslOptions.cert)
    {
        sslOptions = null;
    }

    var server = restify.createServer(
    { 
        name: 'CloudStash',
        log: log,
        httpsServerOptions: sslOptions
    });
    
    server.use(restify.requestLogger());

    var cors = restifyCorsMiddleware(
    {
        origins: config.get('CORS_ORIGINS'),
        allowHeaders: ['Authorization', 'Dropbox-API-Arg']
    });

    server.pre(cors.preflight);
    server.use(cors.actual);

    var exemptions = auth.addAuthMiddleware(server, tokenMgr);

    // The auth process produces a JWT token.  All endpoints except auth endpoints require that token (via
    // Authorization header), and those endpoints can access the token payload via req.user.  
    //
    exemptions.unshift(/\/public\/?.*/);
    server.use(restifyJwt({ secret: tokenMgr.getAuthTokenSecret()}).unless({path: exemptions}));

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
       if (res.headersSent) 
       {
           log.error("Uncaught exception after response sent:", err.message, err.stack);
       }
       else
       {
           log.error("Uncaught exception, returning err:", err.message, err.stack);
           res.send(err);
       }
    });

    server.on('NotFound', function(req, res, err, callback) {
      log.error("NotFound: %s at path: %s", err.message, req.path());
      res.send(err);
      return callback();
    });

    server.on('MethodNotAllowed', function(req, res, err, callback) {
      log.error("MethodNotAllowed: %s at path: %s", err.message, req.path());
      res.send(err);
      return callback();
    });

    server.on('VersionNotAllowed', function(req, res, err, callback) {
      log.error("VersionNotAllowed: %s at path: %s", err.message, req.path());
      res.send(err);
      return callback();
    });

    server.on('UnsupportedMediaType', function(req, res, err, callback) {
      log.error("UnsupportedMediaType: %s at path: %s", err.message, req.path());
      res.send(err);
      return callback();
    });

    server.pre(function(req, res, next){
        log.info("PRE request %s %s:", req.method, req.path());
        return next();
    })

    var fileDriver = require('../drivers/file-driver');
    var mantaDriver = require('../drivers/manta-driver');

    var driverConfig = config.get('driver');

    log.info("Adding driver for provider:", driverConfig.provider);

    var driver;
    if (driverConfig.provider === "file")
    {
        driver = new fileDriver(driverConfig, config);
    }
    else if (driverConfig.provider === "manta")
    {
        driver = new mantaDriver(driverConfig, config);
    }
    else
    {
        log.error("Unknown driver:", driverConfig.provider);
        return;
    }

    var root = require('./root')(config, driver);
    var bridge = require('./bridge')(config, driver);
    var bulk = require('./bulk')(config, driver);

    // Circular ref workaround
    //
    root.setBulk(bulk);
    bridge.setRoot(root);
    bulk.setBridge(bridge);

    //
    // "Users" API endpoints (api.dropboxapi.com/2/*)
    //

    server.post('/2/users/get_account', restify.bodyParser(), function(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, true);
            assert.string(apiArgs.account_id, "account_id");
        }
        catch (err)
        {
            req.log.error("%s parameter validation failure:", req.route.path, err.message);
            res.contentType = "text/plain"; // Per Dropbox API - 400 details in text/plain
            return next(res.send(400, err.message));
        }

        req.log.info("Get account for account_id: %s", apiArgs.account_id);
        res.send(account.getAccount(apiArgs.account_id)); 
        next();
    });

    server.post('/2/users/get_current_account', function(req, res, next)
    {
        req.log.info("Get current account for account_id: %s", req.user.account_id);
        res.send(account.getAccount(req.user.account_id)); 
        next();
    });

    //
    // "Files" content API endpoints (content.dropboxapi.com/2/*)
    //

    var apiArgsHeader = "Dropbox-API-Arg".toLowerCase();

    function getApiArgs(req, isRpcEndpoint)
    {
        var apiArgs = {};

        if (isRpcEndpoint)
        {
            if (req.is('json'))
            {
                apiArgs = req.body;
            }
            else
            {
                try
                {
                    apiArgs = JSON.parse(req.body);
                }
                catch (err)
                {
                    log.error("Error parsing api args from body:", req.body, err);
                    throw err;
                }
            }
        }
        else
        {
            if (req.headers && req.headers[apiArgsHeader])
            {
                // Dropbox-API-Arg contains params as JSON

                try
                {
                    apiArgs = JSON.parse(req.headers[apiArgsHeader]);
                }
                catch (err)
                {
                    log.error("Error parsing api args from header:", req.headers[apiArgsHeader], err);
                    throw err;
                }
            }
            else if (req.params)
            {
                apiArgs = req.params;
            }
        }

        req.log.info("API args:", apiArgs);
        return apiArgs;
    }

    function returnParameterValidationError(req, res, err)
    {
        var message = util.format("%s parameter validation failure:", req.route.path, err.message);
        req.log.error(message);
        res.contentType = "text/plain"; // Per Dropbox API - 400 details in text/plain
        res.send(400, message);
    }

    function filesDownload(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, false);
            assert.string(apiArgs.path, "path");
        }
        catch (err)
        {
            return next(returnParameterValidationError(req, res, err));
        }

        req.log.info("Download for account_id: %s from path: %s", req.user.account_id, apiArgs.path);

        driver.getObjectMetaData(req.user, apiArgs.path, function(err, entry)
        {
            if (err)
            {
                req.log.error("Error on download (getting metadata):", err);
                return next(dbErr.returnDropboxError(res, err));
            }
            else if (!entry)
            {
                return next(dbErr.returnDropboxErrorNew(res, "path", "not_found", "File not found: " + apiArgs.path));
            }
            else
            {
                driver.getObject(req.user, apiArgs.path, req.headers, function(err, stream, statusCode, statusMessage, headers)
                {
                    if (err)
                    {
                        req.log.error("Error on download (getting object):", err);
                        return next(dbErr.returnDropboxError(res, err));
                    }
                    else if (statusCode === 404)
                    {
                        return next(dbErr.returnDropboxErrorNew(res, "path", "not_found", "File not found: " + apiArgs.path));
                    }
                    else
                    {
                        var respCode = statusCode;
                        var respHeaders =
                        {
                            'Content-Type': 'application/octet-stream',
                            'Dropbox-API-Result': JSON.stringify(entry)
                        }

                        if (headers)
                        {
                            // Propagate headers from drivers that support them (Manta for now).
                            //
                            if (headers['accept-ranges'])
                            {
                                respHeaders['Accept-Ranges'] = headers['accept-ranges'];
                            }
                            if (headers['content-length'])
                            {
                                respHeaders['Content-Length'] = headers['content-length'];
                            }
                            if (headers['content-md5'])
                            {
                                respHeaders['Content-MD5'] = headers['content-md5'];
                            }
                            if (headers['content-range'])
                            {
                                respHeaders['Content-Range'] = headers['content-range'];
                            }
                            if (headers['content-type'])
                            {
                                respHeaders['Content-Type'] = headers['content-type'];
                            }
                            if (headers['etag'])
                            {
                                respHeaders['ETag'] = headers['etag'];
                            }
                            if (headers['last-modified'])
                            {
                                respHeaders['Last-Modified'] = headers['last-modified'];
                            }
                        }

                        res.writeHead(respCode, respHeaders);
                        if (stream)
                        {
                            stream.pipe(res); // Pipe store object contents to output
                            stream.on('end',function() 
                            {
                                req.log.info("Stream written, returning result");
                            });
                        }
                        else
                        {
                            res.end();
                        }

                        next();
                    }
                });
            }
        });
    }

    // Download supports GET with path in query params, as well as POST with path in API args header
    //
    // !!! The "path" param can contain a prefix indicating the key to use to find the file, per this spec:
    //
    //     path String(pattern="(/(.|[\r\n])*|id:.*)|(rev:[0-9a-f]{9,})|(ns:[0-9]+(/.*)?)")
    //
    //     This means we probably need to support "id" now (in addition to the default name/path form we support
    //     already).  The "ns" (namespace-relative) prefix is only meaningful in the context of shared folders.
    //
    // !!! Since some of our backends are case-sensitive, we essentially rely on path_display (which is case-preserving),
    //     being used as the basis of the path argument here (and in move/copy/delete), at least for now.  Dropbox
    //     itself is case insensitive, so for maximum compatability (including with their sample apps), we should
    //     make our backends case insensitive, but that's a bit of a project.
    // 
    server.get('/2/files/download', restify.queryParser(), filesDownload);
    server.post('/2/files/download', filesDownload);

    function filesUpload(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, false);
            assert.string(apiArgs.path, "path");
        }
        catch (err)
        {
            return next(returnParameterValidationError(req, res, err));
        }

        req.log.info("Upload for account_id: %s to path: %s", req.user.account_id, apiArgs.path);

        // !!! Need support for apiArgs.mode === "update" (w/ .tag a and update members)

        var mode = "add";
        if (apiArgs.mode === "overwrite")
        {
            mode = "overwrite";
        }

        if ((mode === "add") && (apiArgs.autorename))
        {
            mode = "rename";
        }

        bulk.validateOperation(req.user, null, apiArgs.path, "upload", mode, null, function(err, itemCount, srcEntry, dstPath)
        {
            if (err)
            {
                req.log.error("Error validating upload:", err);
                return next(dbErr.returnDropboxError(res, err));
            }
            else
            {
                log.info("%s bulk validation:", req.route.path, dstPath);

                driver.putObject(req.user, dstPath, function(err, stream)
                {
                    if (err)
                    {
                        req.log.error(err, "Error starting upload");
                        return next(dbErr.returnDropboxError(res, err));
                    }

                    csUtil.pipeRequest(req, stream, function(err, details)
                    {
                        // In the case of Manta, "details" is the full response from the Manta request
                        //
                        if (err)
                        {
                            req.log.error(err, "Error piping request to upload stream");
                            return next(dbErr.returnDropboxError(res, err));
                        }

                        log.info("Piping request to upload stream for '%s' complete", dstPath);

                        bridge.getObjectMetaDataWithRetry(req.user, dstPath, function(err, entry)
                        {
                            if (err)
                            {
                                req.log.error("Error getting metadata on path after upload", err);
                                return next(dbErr.returnDropboxError(res, err));
                            }
                            else
                            {
                                res.send(entry);
                                next();
                            }
                        });
                    });
                });
            }
        });
    }

    server.post('/2/files/upload', filesUpload);

    function uploadSessionStart(req, res, next)
    {
        var apiArgs = getApiArgs(req, false);
        req.log.info("Upload session start for account_id: %s", req.user.account_id);

        // !!! Should process apiArgs.close == true (it seems like you still have to call finish, but this
        //     signals that no more parts are coming).

        bridge.startMultipartUpload(req.user, function(err, sessionId, stream)
        {
            if (err)
            {
                req.log.error(err, "Error starting upload session start");
                return next(dbErr.returnDropboxError(res, err));
            }

            csUtil.pipeRequest(req, stream, function(err, details)
            {
                if (err)
                {
                    req.log.error(err, "Error piping request to upload session start stream");
                    return next(dbErr.returnDropboxError(res, err));
                }
                else
                {
                    log.info("Piping request to upload start stream succeeded");
                    res.send({ "session_id": sessionId });
                    next();
                }
            });
        });
    }

    function uploadSessionAppend(apiArgs, req, res, next)
    {
        req.log.info("Append upload session %s, offset: %n", apiArgs.cursor.session_id, apiArgs.cursor.offset);

        bridge.multipartUpload(req.user, apiArgs.cursor.session_id, apiArgs.cursor.offset, function(err, stream)
        {
            if (err)
            {
                req.log.error(err, "Error on upload session append");
                return next(dbErr.returnDropboxError(res, err));
            }

            csUtil.pipeRequest(req, stream, function(err, details)
            {
                if (err)
                {
                    req.log.error(err, "Error piping request to upload session append stream");
                    return next(dbErr.returnDropboxError(res, err));
                }
                else
                {
                    log.info("Piping request to upload session append stream succeeded");
                    res.send();
                    next();
                }
            });
        });
    }

    // !!! If we err at any point, do we want to cancel this upload (delete all the parts, invalidate
    //     session_id), or do we let the caller try again?  Same issue on append.  See what DropBox does.
    //
    function uploadSessionFinish(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, false);
            assert.object(apiArgs.cursor, "cursor");
            assert.string(apiArgs.cursor.session_id, "session_id");
            assert.number(apiArgs.cursor.offset, "offset");
            assert.object(apiArgs.commit, "commit");
            assert.string(apiArgs.commit.path, "path");
        }
        catch (err)
        {
            return next(returnParameterValidationError(req, res, err));
        }

        req.log.info("End upload session %s, offset: %n", apiArgs.cursor.session_id, apiArgs.cursor.offset);

        // !!! Need support for apiArgs.mode === "update" (w/ .tag a and update members)

        var mode = "add";
        if (apiArgs.commit.mode === "overwrite")
        {
            mode = "overwrite";
        }

        if ((mode === "add") && (apiArgs.commit.autorename))
        {
            mode = "rename";
        }

        bulk.validateOperation(req.user, null, apiArgs.commit.path, "upload", mode, null, function(err, itemCount, srcEntry, dstPath)
        {
            if (err)
            {
                req.log.error("Error validating upload:", err);
                return next(dbErr.returnDropboxError(res, err));
            }
            else
            {
                log.info("%s bulk validation:", req.route.path, dstPath);

                bridge.multipartUpload(req.user, apiArgs.cursor.session_id, apiArgs.cursor.offset, function(err, stream)
                {
                    if (err)
                    {
                        req.log.error(err, "Error on upload session end");
                        return next(dbErr.returnDropboxError(res, err));
                    }

                    csUtil.pipeRequest(req, stream, function(err, details)
                    {
                        if (err)
                        {
                            req.log.error(err, "Error piping request to upload session end stream");
                            return next(dbErr.returnDropboxError(res, err));
                        }
                        else
                        {
                            log.info("Piping request to upload session end stream succeeded");

                            bridge.finishMultipartUpload(req.user, apiArgs.cursor.session_id, dstPath, function(err, item)
                            {
                                if (err)
                                {
                                    req.log.error(err, "Error on session finish");
                                    return next(dbErr.returnDropboxError(res, err));
                                }
                                else
                                {
                                    res.send(item);
                                    next();
                                }
                            });
                        }
                    });
                });
            }
        });
    }

    // The multipart upload endpoints are "content" endpoints because start, append, and finish all have data
    // payload (file segments)
    //
    server.post('/2/files/upload_session/start', uploadSessionStart);
    server.post('/2/files/upload_session/append', function(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, false);
            assert.string(apiArgs.session_id, "session_id");
            assert.number(apiArgs.offset, "offset");
        }
        catch (err)
        {
            return next(returnParameterValidationError(req, res, err));
        }

        // Upgrade this to a v2 request
        //
        apiArgs = { "cursor": apiArgs };

        uploadSessionAppend(apiArgs, req, res, next);
    });
    server.post('/2/files/upload_session/append_v2', function(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, false);
            assert.object(apiArgs.cursor, "cursor");
            assert.string(apiArgs.cursor.session_id, "session_id");
            assert.number(apiArgs.cursor.offset, "offset");
        }
        catch (err)
        {
            return next(returnParameterValidationError(req, res, err));
        }

        uploadSessionAppend(apiArgs, req, res, next);
    });
    server.post('/2/files/upload_session/finish', uploadSessionFinish);

    //
    // "Files" API endpoints (api.dropboxapi.com/2/*)
    //

    // Dropbox nomenclature is "folder" or "file" (either of which is considered an "entry")
    //

    // DropBox sample entries for files/list_folder / files/list_folder/continue:
    //
    /*
    entries: [
    {
        ".tag": "folder",
        "name": "Sample Album",
        "path_lower": "/photos/sample album",
        "path_display": "/Photos/Sample Album",
        "id": "id:8k14tg5by8UAAAAAAAABWQ"
    },
    {
        ".tag": "file",
        "name": "Boston City Flow.jpg",
        "path_lower": "/photos/sample album/boston city flow.jpg",
        "path_display": "/Photos/Sample Album/Boston City Flow.jpg",
        "id": "id:8k14tg5by8UAAAAAAAABUQ",
        "client_modified": "2011-10-01T18:16:54Z",
        "server_modified": "2013-04-11T01:08:49Z",
        "rev": "4f7042fb1f0",
        "size": 339773,
        "content_hash": "90b8323fdbe1e7a5082c77f848ffffa58a8e2ccd911e617413ebab50d6e9db1c"
    }]
    */

    function encodeCursor(user, path, recursive, limit, lastItem)
    {
        var cursor = 
        {
            user: { account_id: user.account_id, app_id: user.app_id }, 
            path: path, 
            recursive: recursive, 
            limit: limit,
            lastItem: lastItem
        }

        return new Buffer(JSON.stringify(cursor)).toString("base64");
    }

    function decodeCursor(cursorString)
    {
        return JSON.parse(Buffer(cursorString, 'base64').toString());
    }

    function filesListFolder(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, true);
            assert.string(apiArgs.path, "path");
            assert.optionalBool(apiArgs.recursive, "recursive");
        }
        catch (err)
        {
            return next(returnParameterValidationError(req, res, err));
        }

        req.log.info("List folders for app_id: %s, account_id: %s at path: %s", req.user.app_id, req.user.account_id, apiArgs.path);

        var limit = apiArgs.limit || _defaultListFolderLimit;

        bridge.listFolderUsingCursor(req.user, apiArgs.path, apiArgs.recursive, limit, null, function(err, items, hasMore, cursorItem)
        {
            if (err)
            {
                req.log.error("Error on list folder:", err);
                return next(dbErr.returnDropboxError(res, err));
            }

            var cursorString = encodeCursor(req.user, apiArgs.path, !!apiArgs.recursive, limit, cursorItem)

            res.send({ entries: items, cursor: cursorString, has_more: hasMore });
            next();
        });
    }

    function filesListFolderContinue(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, true);
            assert.string(apiArgs.cursor, "cursor");
        }
        catch (err)
        {
            return next(returnParameterValidationError(req, res, err));
        }

        req.log.info("List folders continue for account_id: %s", req.user.account_id);

        var cursor = decodeCursor(apiArgs.cursor);

        bridge.listFolderUsingCursor(req.user, cursor.path, cursor.recursive, cursor.limit, cursor.lastItem, function(err, items, hasMore, cursorItem)
        {
            if (err)
            {
                req.log.error("Error on list folder continue:", err);
                return next(dbErr.returnDropboxError(res, err));
            }

            var newCursorString = encodeCursor(req.user, cursor.path, cursor.recursive, cursor.limit, cursorItem)

            res.send({ entries: items, cursor: newCursorString, has_more: hasMore });
            next();
        });
    }

    function filesListFolderGetLatestCursor(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, true);
            assert.string(apiArgs.path, "path");
            assert.optionalBool(apiArgs.recursive, "recursive");
        }
        catch (err)
        {
            return next(returnParameterValidationError(req, res, err));
        }

        req.log.info("Get latest cursor for account_id: %s at path: %s", req.user.account_id, apiArgs.path);

        var limit = apiArgs.limit || _defaultListFolderLimit;

        bridge.getLatestCursorItem(req.user, apiArgs.path, apiArgs.recursive, function(err, cursorItem)
        {
            var cursorString = encodeCursor(req.user, apiArgs.path, apiArgs.recursive, limit, cursorItem);
            res.send({ cursor: cursorString });
            next();
        });
    }

    function filesListFolderLongPoll(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, true);
            assert.string(apiArgs.cursor, "cursor");
            assert.optionalNumber(apiArgs.timeout, "timeout");
        }
        catch (err)
        {
            return next(returnParameterValidationError(req, res, err));
        }

        req.log.info("List folder long poll for account_id: %s", req.user.account_id);

        var cursor = decodeCursor(apiArgs.cursor);

        var timeout = (apiArgs.timeout || 30) * 1000;
        if (apiArgs.timeout < 0)
        {
            // If timeout arg was provided and was negative, invert it and don't add random jitter.
            // This functionality is for unit testing where we want to control the timeout exactly.
            //
            timeout *= -1;
        }
        else
        {
            // Add random jitter to avoid the thundering herd problem
            //
            timeout += Math.random() * _maxLongPollJitter; 
        }

        var startTime = new Date();
        var newItemFound = false;

        async.whilst(
            function() 
            {
                var elapsedTime = new Date() - startTime;
                return !newItemFound && (elapsedTime < timeout); 
            },
            function(callback) 
            {
                bridge.isAnyCursorItemNewer(req.user, cursor.path, cursor.recursive, cursor.lastItem, function(err, isNewer)
                {
                    if (err)
                    {
                        callback(err);
                    }
                    else if (isNewer)
                    {
                        req.log.info("Found item later than cursor item");
                        newItemFound = true;
                        callback();
                    }
                    else
                    {
                        // !!! Might not want to do this if interval would put us past timeout
                        // !!! Might want to consider time elapsed since state of this round in determining timout interval
                        //
                        setTimeout(function() { callback(); }, _longPollInterval);
                    }
                });
            },
            function (err)
            {
                if (err)
                {
                    // !!!
                    req.log.err(err);
                    return next(dbErr.returnDropboxError(res, err));
                }
                else
                {
                    res.send({ changes: newItemFound });
                    next();
                }
            }
        );
    }

    function filesCreateFolder(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, true);
            assert.string(apiArgs.path, "path");
        }
        catch (err)
        {
            return next(returnParameterValidationError(req, res, err));
        }

        req.log.info("Create folder for account_id: %s at path: %s", req.user.account_id, apiArgs.path);

        driver.createDirectory(req.user, apiArgs.path, function(err)
        {
            if (err)
            {
                req.log.error("Error on create folder (creating folder):", err);
                return next(dbErr.returnDropboxError(res, err));
            }
            else
            {
                bridge.getObjectMetaDataWithRetry(req.user, apiArgs.path, function(err, entry)
                {
                    if (err)
                    {
                        req.log.error("Error on create folder (getting metadata):", err);
                        return next(dbErr.returnDropboxError(res, err));
                    }
                    else
                    {
                        res.send(entry);
                        next();
                    }
                });
            }
        });
    }

    // API args:
    //
    //   from_path
    //   to_path
    //   autorename
    //   overwrite (not in Dropbox, our custom extension)
    //
    function filesCopyMove(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, true);
            assert.string(apiArgs.from_path, "from_path");
            assert.string(apiArgs.to_path, "to_path");
            assert.optionalBool(apiArgs.autorename, "autorename");
            assert.optionalBool(apiArgs.autorename, "overwrite");
        }
        catch (err)
        {
            return next(returnParameterValidationError(req, res, err));
        }

        var operation;

        var matches = /\/files\/([^\/]*)/.exec(req.route.path);
        if (matches)
        {
            operation = matches[1];
        }

        var mode = apiArgs.autorename ? "rename" : "add";
        if (apiArgs.overwrite)
        {
            mode = "overwrite";
        }

        req.log.info("%s (%s) with mode %s for account_id: %s from path: %s to path: %s", req.route.path, operation, mode, req.user.account_id, apiArgs.from_path, apiArgs.to_path);

        bulk.validateOperation(req.user, apiArgs.from_path, apiArgs.to_path, operation, mode, _maxInteractive, function(err, itemCount, srcEntry, dstPath)
        {
            if (err)
            {
                req.log.error("Error validating %s:", operation, err);
                return next(dbErr.returnDropboxError(res, err));
            }
            else
            {
                log.info("%s bulk validation:", req.route.path, itemCount, srcEntry, dstPath);

                bulk.doOperation(req, operation, [{ srcEntry: srcEntry, dstPath: dstPath }], function(err)
                {
                    if (err)
                    {
                        req.log.error("Error on %s:", operation, err);
                        return next(dbErr.returnDropboxError(res, err));
                    }

                    bridge.getObjectMetaDataWithRetry(req.user, dstPath, function(err, entry)
                    {
                        if (err)
                        {
                            req.log.error("Error getting metadata on to_path after %s", operation, err);
                            return next(dbErr.returnDropboxError(res, err));
                        }
                        else
                        {
                            res.send(entry);
                            next();
                        }
                    });
                });
            }
        });
    }

    function filesDelete(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, true);
            assert.string(apiArgs.path, "path");
        }
        catch (err)
        {
            return next(returnParameterValidationError(req, res, err));
        }

        req.log.info("Delete file for account_id: %s at path: %s", req.user.account_id, apiArgs.path);

        bulk.validateOperation(req.user, apiArgs.path, null, "delete", null, _maxInteractive, function(err, itemCount, srcEntry)
        {
            if (err)
            {
                req.log.error("Error validating delete:", err);
                return next(dbErr.returnDropboxError(res, err));
            }
            else
            {
                log.info("Delete bulk validation:", itemCount, srcEntry);

                bulk.doOperation(req, "delete", [{ srcEntry: srcEntry }], function(err)
                {
                    if (err)
                    {
                        req.log.error("Error on delete:", err);
                        return next(dbErr.returnDropboxError(res, err));
                    }

                    res.send(srcEntry);
                    next();
                });
            }
        });
    }

    //
    // Batch functions
    //

    // Move and Copy are closely related enough that we can use a single handler for both (Delete is
    // different enough to warrant its own handler).
    //
    function filesMoveCopyBatch(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, true);
            assert.arrayOfObject(apiArgs.entries, "entries"); // !!! Validate entries?

        }
        catch (err)
        {
            return next(returnParameterValidationError(req, res, err));
        }

        var operation;

        var matches = /\/([^\/]*)_batch/.exec(req.route.path);
        if (matches)
        {
            operation = matches[1];
        }

        var mode = apiArgs.autorename ? "rename" : "add";
        if (apiArgs.overwrite)
        {
            mode = "overwrite";
        }

        req.log.info("%s (%s) with mode %s for account_id: %s", req.route.path, operation, mode, req.user.account_id);

        var jobId = uuidv4();

        var workItems = [];

        async.series(
        [
            function(callback) // Write in_progress status to job file
            {
                var response =
                {
                    ".tag": "in_progress"
                }

                root.putObject(req.user, "/jobs/" + jobId, response, function(err)
                {
                    if (err)
                    {
                        req.log.error("Error putting jobs file on batch %s:", operation, err);
                        next(dbErr.returnDropboxError(res, err));
                    }
                    else
                    {
                        // Return async status (job id) to caller...
                        //
                        res.send(
                        {
                            ".tag": "async_job_id",
                            "async_job_id": jobId
                        });
                        next();
                    }

                    callback(err);
                });
            },
            function(callback) // Validate bulk operation
            {
                async.eachLimit(apiArgs.entries, _maxConcurrency, function(entry, callback)
                {
                    // Validate entry.path
                    //
                    bulk.validateOperation(req.user, entry.from_path, entry.to_path, operation, mode, null, function(err, itemCount, srcEntry, dstPath)
                    {
                        if (!err)
                        {
                            workItems.push({ srcEntry: srcEntry, dstPath: dstPath });
                        }
                        callback(err);
                    });
                },
                function (err) 
                {
                    if (err)
                    {
                        // Write validation err to job file
                        //
                        req.log.error("Error validating batch %s:", operation, err);
                        var status = dbErr.getDropboxError(err);
                        status[".tag"] = "failed";

                        root.putObject(req.user, "/jobs/" + jobId, status, function(err)
                        {
                            if (err)
                            {
                                // This is a big of a bad spot, because we have no other way to communicate
                                // about this job except writing to the job file, which failed.
                                //
                                req.log.error("Error putting jobs file on batch %s (failed):", operation, err);
                            }

                            callback(err);
                        });
                    }
                    else
                    {
                        callback(err);
                    }
                });
            },
            function(callback) // Do bulk operation
            {
                log.info("Starting batch %s bulk operation", operation);

                bulk.doOperation(req, operation, workItems, function(err)
                {
                    if (err)
                    {
                        // Write validation err to job file
                        //
                        req.log.error("Error in bulk operation for batch %s:", operation, err);
                        var status = dbErr.getDropboxError(err);
                        status[".tag"] = "failed";

                        root.putObject(req.user, "/jobs/" + jobId, status, function(err)
                        {
                            if (err)
                            {
                                // This is a big of a bad spot, because we have no other way to communicate
                                // about this job except writing to the job file, which failed.
                                //
                                req.log.error("Error putting jobs file on batch %s (failed):", operation, err);
                            }

                            callback(err);
                        });
                    }
                    else
                    {
                        callback(err);
                    }
                });
            },
            function(callback) // Get entry info for dstPath items
            {
                // For each workitem, get metadata, put result back into workitem as dstEntry.
                //
                async.eachLimit(workItems, _maxConcurrency, function(workItem, callback)
                {
                    bridge.getObjectMetaDataWithRetry(req.user, workItem.dstPath, function(err, entry)
                    {
                        if (err)
                        {
                            req.log.error("Error getting metadata on to_path after %s", operation, err);
                        }
                        else
                        {
                            workItem.dstEntry = entry;
                        }

                        callback(err);
                    });
                },
                function (err) 
                {
                    if (err)
                    {
                        req.log.error("Error getting destination entries for batch %s:", operation, err);
                        // !!! Write err to job file
                        //return next(dbErr.returnDropboxError(res, err));
                    }
                    callback(err);
                });
            },
            function(callback) // Write final status (results) to job file...
            {
                var status =
                {
                    ".tag": "complete",
                    "entries": [ ]
                }

                workItems.forEach(function(workItem)
                {
                    status.entries.push(workItem.dstEntry);
                });

                root.putObject(req.user, "/jobs/" + jobId, status, function(err)
                {
                    if (err)
                    {
                        // This is a big of a bad spot, because we have no other way to communicate
                        // about this job except writing to the job file, which failed.
                        //
                        req.log.error("Error putting jobs file on batch %s (complete):", operation, err);
                    }

                    callback(err);
                });
            }
        ],
        function(err, results) 
        {
            if (err)
            {
                // I don't think we need this (each function will handle it's own error)
                req.log.error("Error in batch %s:", operation, err);
            }
            else
            {
                req.log.error("batch %s complete", operation);
            }
        });
    }

    function filesDeleteBatch(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, true);
            assert.arrayOfObject(apiArgs.entries, "entries"); // !!! Validate entries?
        }
        catch (err)
        {
            return next(returnParameterValidationError(req, res, err));
        }

        var operation = "delete";

        req.log.info("Delete batch for account_id", req.user.account_id);

        var jobId = uuidv4();

        var workItems = [];

        async.series(
        [
            function(callback) // Write in_progress status to job file
            {
                var response =
                {
                    ".tag": "in_progress"
                }

                root.putObject(req.user, "/jobs/" + jobId, response, function(err)
                {
                    if (err)
                    {
                        req.log.error("Error putting jobs file on batch delete:", err);
                        next(dbErr.returnDropboxError(res, err));
                    }
                    else
                    {
                        // Return async status (job id) to caller...
                        //
                        res.send(
                        {
                            ".tag": "async_job_id",
                            "async_job_id": jobId
                        });
                        next();
                    }

                    callback(err);
                });
            },
            function(callback) // Validate bulk operation
            {
                async.eachLimit(apiArgs.entries, _maxConcurrency, function(entry, callback)
                {
                    req.log.info("Validating entry:", entry);
                    // Validate entry.path
                    //
                    bulk.validateOperation(req.user, entry.path, null, operation, null, null, function(err, itemCount, srcEntry)
                    {
                        if (!err)
                        {
                            workItems.push({ "srcEntry": srcEntry });
                        }
                        callback(err);
                    });
                },
                function (err) 
                {
                    if (err)
                    {
                        // Write validation err to job file
                        //
                        req.log.error("Error validating batch %s:", operation, err);
                        var status = dbErr.getDropboxError(err);
                        status[".tag"] = "failed";

                        root.putObject(req.user, "/jobs/" + jobId, status, function(err)
                        {
                            if (err)
                            {
                                // This is a big of a bad spot, because we have no other way to communicate
                                // about this job except writing to the job file, which failed.
                                //
                                req.log.error("Error putting jobs file on batch %s (failed):", operation, err);
                            }

                            callback(err);
                        });
                    }
                    else
                    {
                        callback(err);
                    }
                });
            },
            function(callback) // Do bulk operation
            {
                log.info("Starting batch delete bulk operation");

                bulk.doOperation(req, operation, workItems, function(err)
                {
                    if (err)
                    {
                        // Write validation err to job file
                        //
                        req.log.error("Error in bulk operation for batch %s:", operation, err);
                        var status = dbErr.getDropboxError(err);
                        status[".tag"] = "failed";

                        root.putObject(req.user, "/jobs/" + jobId, status, function(err)
                        {
                            if (err)
                            {
                                // This is a big of a bad spot, because we have no other way to communicate
                                // about this job except writing to the job file, which failed.
                                //
                                req.log.error("Error putting jobs file on batch %s (failed):", operation, err);
                            }

                            callback(err);
                        });
                    }
                    else
                    {
                        callback(err);
                    }
                });
            },
            function(callback) // Write final status (results) to job file...
            {
                var status =
                {
                    ".tag": "complete",
                    "entries": [ ]
                }

                workItems.forEach(function(workItem)
                {
                    status.entries.push(workItem.srcEntry);
                });

                root.putObject(req.user, "/jobs/" + jobId, status, function(err)
                {
                    if (err)
                    {
                        // This is a big of a bad spot, because we have no other way to communicate
                        // about this job except writing to the job file, which failed.
                        //
                        req.log.error("Error putting jobs file on batch delete (complete):", err);
                    }

                    callback(err);
                });
            }
        ],
        function(err, results) 
        {
            if (err)
            {
                // I don't think we need this (each function will handle it's own error)
                req.log.error("Error in batch delete:", err);
            }
            else
            {
                req.log.error("batch delete complete");
            }
        });
    }

    // !!! If there is a failure after some number of successful operations, do we communicate
    //     both the successfully completed entries and the failure?  Do we delete the remaining
    //     upload files?  Or is the client gonna maybe try this again?
    //
    // !!! For each entry, "close" needs to be true for the last upload_session/start or 
    //     upload_session/append_v2 call.
    //
    function filesUploadSessionFinishBatch(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, true);
            assert.arrayOfObject(apiArgs.entries, "entries"); // !!! Validate entries?
            apiArgs.entries.forEach(function(entry, index) 
            {
                assert.object(entry.cursor, "cursor[" + index + "]");
                assert.string(entry.cursor.session_id, "session_id[" + index + "]");
                assert.number(entry.cursor.offset, "offset[" + index + "]");
                assert.object(entry.commit, "commit[" + index + "]");
                assert.string(entry.commit.path, "path[" + index + "]");
            });
        }
        catch (err)
        {
            return next(returnParameterValidationError(req, res, err));
        }

        req.log.info("Upload session finish batch for account_id", req.user.account_id);

        var jobId = uuidv4();

        var workItems = [];

        async.series(
        [
            function(callback) // Write in_progress status to job file
            {
                var response =
                {
                    ".tag": "in_progress"
                }

                root.putObject(req.user, "/jobs/" + jobId, response, function(err)
                {
                    if (err)
                    {
                        req.log.error("Error putting jobs file on batch upload session finish:", err);
                        next(dbErr.returnDropboxError(res, err));
                    }
                    else
                    {
                        // Return async status (job id) to caller...
                        //
                        res.send(
                        {
                            ".tag": "async_job_id",
                            "async_job_id": jobId
                        });
                        next();
                    }

                    callback(err);
                });
            },
            function(callback) // Validate bulk operation
            {
                async.eachLimit(apiArgs.entries, _maxConcurrency, function(entry, callback)
                {
                    // !!! Need support for apiArgs.mode === "update" (w/ .tag a and update members)

                    var mode = "add";
                    if (entry.commit.mode === "overwrite")
                    {
                        mode = "overwrite";
                    }

                    if ((mode === "add") && (entry.commit.autorename))
                    {
                        mode = "rename";
                    }

                    bulk.validateOperation(req.user, null, entry.commit.path, "upload", mode, null, function(err, itemCount, srcEntry, dstPath)
                    {
                        if (!err)
                        {
                            workItems.push({ "session_id": entry.cursor.session_id, "dstPath": dstPath });
                        }
                        callback(err);
                    });
                },
                function (err) 
                {
                    if (err)
                    {
                        // Write validation err to job file
                        //
                        req.log.error("Error validating session finish batch:", err);
                        var status = dbErr.getDropboxError(err);
                        status[".tag"] = "failed";

                        root.putObject(req.user, "/jobs/" + jobId, status, function(err)
                        {
                            if (err)
                            {
                                // This is a big of a bad spot, because we have no other way to communicate
                                // about this job except writing to the job file, which failed.
                                //
                                req.log.error("Error putting jobs file on session finish batch (failed):", err);
                            }

                            callback(err);
                        });
                    }
                    else
                    {
                        callback(err);
                    }
                });
            },
            function(callback) // Do bulk operation
            {
                log.info("Starting session finish batch bulk operation");

                // workItem = { "session_id": entry.cursor.session_id, "dstPath": dstPath };

                // DropBox specifies that these are to be done one at a time (maybe so you could get
                // a list of completions along with any error?) - anyway, that's what we do.
                //
                async.eachSeries(workItems, function(workItem, callback)
                {
                    bridge.finishMultipartUpload(req.user, workItem.session_id, workItem.dstPath, function(err, item)
                    {
                        workItem.entry = item;
                        callback(err);
                    });
                },
                function (err) 
                {
                    if (err)
                    {
                        // Write validation err to job file
                        //
                        req.log.error("Error validating session finish batch:", err);
                        var status = dbErr.getDropboxError(err);
                        status[".tag"] = "failed";

                        root.putObject(req.user, "/jobs/" + jobId, status, function(err)
                        {
                            if (err)
                            {
                                // This is a big of a bad spot, because we have no other way to communicate
                                // about this job except writing to the job file, which failed.
                                //
                                req.log.error("Error putting jobs file on session finish batch (failed):", operation, err);
                            }

                            callback(err);
                        });
                    }
                    else
                    {
                        callback(err);
                    }
                });
            },
            function(callback) // Write final status (results) to job file...
            {
                var status =
                {
                    ".tag": "complete",
                    "entries": [ ]
                }

                workItems.forEach(function(workItem)
                {
                    status.entries.push(workItem.entry);
                });

                root.putObject(req.user, "/jobs/" + jobId, status, function(err)
                {
                    if (err)
                    {
                        // This is a big of a bad spot, because we have no other way to communicate
                        // about this job except writing to the job file, which failed.
                        //
                        req.log.error("Error putting jobs file on session finish batch (complete):", err);
                    }

                    callback(err);
                });
            }
        ],
        function(err, results) 
        {
            if (err)
            {
                // I don't think we need this (each function will handle it's own error)
                req.log.error("Error in session finish batch:", err);
            }
            else
            {
                req.log.error("session finish batch complete");
            }
        });
    }

    // This implementation handles all the xxxxx_batch/check routes (they all do exactly the same thing)
    //
    function filesJobBatchCheck(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, true);
            assert.string(apiArgs.async_job_id, "async_job_id");
        }
        catch (err)
        {
            return next(returnParameterValidationError(req, res, err));
        }

        root.getObject(req.user, "/jobs/" + apiArgs.async_job_id, function(err, object)
        {
            if (err)
            {
                res.log.error("Error getting job status file for job:", apiArgs.async_job_id);
                return next(dbErr.returnDropboxError(res, err));
            }
            else if (object)
            {
                if ((object[".tag"] === "complete") || (object[".tag"] === "failed"))
                {
                    // If "complete", delete job file
                    req.log.info("%s - job is '%s', delete job", req.route.path, object[".tag"]);
                    root.deleteObject(req.user, "/jobs/" + apiArgs.async_job_id, function (err)
                    {
                        // We intentionally didn't wait for this, since there's nothing we can really do
                        // about this failure, and the abandoned job sweeper will catch this later anyway.
                        //
                        if (err)
                        {
                            req.log.error("Error deleting completed job: %s", apiArgs.async_job_id);
                        }
                    });
                }

                req.log.info("%s - returning job with status: %s", req.route.path, object[".tag"]);
                res.send(object);
                next();
            }
            else // Job file did not exist
            {
                res.log.error("%s - job file not found for job:", req.route.path, apiArgs.async_job_id);
                next(dbErr.returnDropboxErrorNew(res, "invalid_async_job_id", null, "Job file not found"));
            }
        });
    }

    // Get Metadata - This is the equivalent of listing directory info for a single object (single entry return)
    //
    function filesGetMetaData(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, true);
            assert.string(apiArgs.path, "path");
        }
        catch (err)
        {
            return next(returnParameterValidationError(req, res, err));
        }

        req.log.info("Get object metadata for account_id: %s at path: %s", req.user.account_id, apiArgs.path);

        driver.getObjectMetaData(req.user, apiArgs.path, function(err, item)
        {
            if (err)
            {
                req.log.error("Error on get metadata:", err);
                return next(dbErr.returnDropboxError(res, err));
            }
            else if (item)
            {
                res.send(item);
            }
            else
            {
                dbErr.returnDropboxErrorNew(res, "path", "not_found");
            }

            next();
        });
    }

    // !!! files/search - The query mechanism is pretty weak - we could easily extend by adding DOS wildcards, regex, etc
    //
    function filesSearch(req, res, next)
    {
        var apiArgs;

        try
        {
            apiArgs = getApiArgs(req, true);
            assert.string(apiArgs.path, "path");
            assert.string(apiArgs.query, "query");
        }
        catch (err)
        {
            return next(returnParameterValidationError(req, res, err));
        }

        req.log.info("File search account_id: %s at path: %s with query: %s", req.user.account_id, apiArgs.path, apiArgs.query);

        function isMatch(entry)
        {
            // !!! This needs some work to match the nutty DropBox match semantics...
            //
            // apiArgs.query
            // 
            //   * split on spaces into multiple tokens
            //   * in order to match, filename must contain all tokens
            //   * last token is prefix match (assumed wildcard at end)
            //
            //      Per docs: For file name searching, the last token is used for prefix matching (i.e. "bat c" matches
            //                "bat cave" but not "batman car").
            //
            // apiArgs.mode
            //
            //   * "filename" or "deleted_filename" ("filename_and_content" not supported)
            //
            return entry.name.includes(apiArgs.query);
        }

        var start = apiArgs.start || 0;
        var limit = apiArgs.max_results || 100;

        var matches = [];

        function onEntry(entry)
        {
            if (isMatch(entry))
            {
                var match = { match_type: { ".tag": "filename" }, metadata: entry };

                // This will insert into "entries" such that "entries" will be/stay in sorted order
                //
                matches.splice(lodash.sortedIndexBy(matches, match, function(o){ return bridge.getEntrySortKey(o.metadata); }), 0, match);

                // This will keep the list from growing beyond more than one over the limit (we purposely
                // leave the "extra" entry so that at the end we will be able to see that we went past
                // the limit).
                //
                if (matches.length > (start + limit + 1))
                {
                    matches.splice(start + limit + 1);
                }
            }
        }

        driver.traverseDirectory(req.user, apiArgs.path, true, onEntry, function(err, stopped)
        {
            if (err)
            {
                req.log.error("Error on get metadata:", err);
                return next(dbErr.returnDropboxError(res, err));
            }
            else
            {
                var hasMore = false;

                if (matches.length > (start + limit))
                {
                    matches.splice(start + limit);
                    hasMore = true;
                }

                if (start)
                {
                    matches.splice(0, start);
                }

                log.info("matches xxx:", matches);

                res.send({ matches: matches, more: hasMore, start: start + matches.length });
                next();
            }
        });
    }

    server.post('/2/files/list_folder', restify.bodyParser(), filesListFolder);
    server.post('/2/files/list_folder/continue', restify.bodyParser(), filesListFolderContinue);
    server.post('/2/files/list_folder/get_latest_cursor', restify.bodyParser(), filesListFolderGetLatestCursor);
    server.post('/2/files/list_folder/longpoll', restify.bodyParser(), filesListFolderLongPoll);
    server.post('/2/files/delete', restify.bodyParser(), filesDelete);
    server.post('/2/files/create_folder', restify.bodyParser(), filesCreateFolder);
    server.post('/2/files/copy', restify.bodyParser(), filesCopyMove);
    server.post('/2/files/move', restify.bodyParser(), filesCopyMove);
    server.post('/2/files/get_metadata', restify.bodyParser(), filesGetMetaData);
    server.post('/2/files/search', restify.bodyParser(), filesSearch);

    // Batch operations
    //
    server.post('/2/files/copy_batch', restify.bodyParser(), filesMoveCopyBatch);
    server.post('/2/files/move_batch', restify.bodyParser(), filesMoveCopyBatch);
    server.post('/2/files/delete_batch', restify.bodyParser(), filesDeleteBatch);
    server.post('/2/files/upload_session/finish_batch', restify.bodyParser(), filesUploadSessionFinishBatch);

    // The batch/check functions are actually all the same underlying implementation
    //
    server.post('/2/files/copy_batch/check', restify.bodyParser(), filesJobBatchCheck);
    server.post('/2/files/delete_batch/check', restify.bodyParser(), filesJobBatchCheck);
    server.post('/2/files/move_batch/check', restify.bodyParser(), filesJobBatchCheck);
    server.post('/2/files/upload_session/finish_batch/check', restify.bodyParser(), filesJobBatchCheck);

    // !!! TODO - The rest of the Dropbox v2 API - Not implemented yet
    //
    function notImplemented(req, res, next)
    {
        req.log.error("API endpoint %s not implemented", req.path());
        throw new Error("Not implemented");
    }

    // Save URL (can finish in call or via async job)

    server.post('/2/files/save_url', notImplemented);
    server.post('/2/files/save_url/check_job_status', notImplemented);

    // Properties

    server.post('/2/files/properties/add', notImplemented);
    server.post('/2/files/properties/overwrite', notImplemented);
    server.post('/2/files/properties/remove', notImplemented);
    server.post('/2/files/properties/template/get', notImplemented);
    server.post('/2/files/properties/template/list', notImplemented);
    server.post('/2/files/properties/update', notImplemented);

    // Preview (document) and Thumbnail (image) support

    server.post('/2/files/get_preview', notImplemented);
    server.post('/2/files/get_thumbnail', notImplemented);

    // !!! Revision support (probably not supporting this)

    server.post('/2/files/list_revisions', notImplemented);
    server.post('/2/files/permanently_delete', notImplemented);
    server.post('/2/files/restore', notImplemented);

    // !!! Multi-user / sharing (probably not supporting this)

    server.post('/2/files/copy_reference/get', notImplemented);
    server.post('/2/files/copy_reference/save', notImplemented);

    // Other

    server.post('/2/files/alpha/get_metadata', notImplemented);
    server.post('/2/files/alpha/upload', notImplemented);
    server.post('/2/files/get_temporary_link', notImplemented);

    // Serve static files...
    //
    server.get(/\/public\/?.*/, restify.serveStatic({
       directory: './web'
    }));

    return server;
}
