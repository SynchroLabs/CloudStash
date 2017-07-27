var restify = require('restify');

var assert = require('assert-plus');

var jwt = require('jsonwebtoken');
var restifyJwt = require('restify-jwt');

var async = require('async');
var path = require('path');
var fs = require('fs');
var uuidv4 = require('uuid/v4');
var stream = require('stream');

var lodash = require('lodash');
var handlebars = require('handlebars');

var account = require('./account');
var dbErr = require('./error');

module.exports = function(_jwtSecret, config)
{
    var log = require('./../lib/logger').getLogger("server");

    var maxConcurrency = config.get('MAX_CONCURRENCY');
    var maxInteractive = config.get('MAX_INTERACTIVE'); // More than this many operations in a non-batch call will result in too_many_files
    var defaultListFolderLimit = config.get('DEFAULT_LIST_FOLDER_LIMIT'); // This appears to be the DropBox limit

    // We encode the auth code and token with different secret keys as a way of enforcing type (the Restify
    // JWT auth middleware conisders anything that decrypted to be valid, so we need to distinguish between
    // auth tokens and other things we encode using JWT - so you can't try to pass off an auth code as an
    // auth token, for example).
    //
    var _jwtAuthCodeSecret = _jwtSecret + "authCode";
    var _jwtAuthTokenSecret = _jwtSecret + "authToken";

    function createAuthCode(app_id, account_id)
    {
        var code = 
        {
            app_id: app_id,
            account_id: account_id
        }
        
        return jwt.sign(code, _jwtAuthCodeSecret);
    }

    function getAuthCode(code)
    {
        return jwt.verify(code, _jwtAuthCodeSecret);
    }

    // !!! Should token contain all user data per Dropbox API, or should we jusr fetch other stuff
    //     as needed (thinking about /users/get_account)?
    //
    // !!! Do we have an iat (issued-at-time) provided by the jwt signer?  Should we add one if not?
    //
    function createAuthToken(app_id, account_id)
    {
        var token = 
        {
            app_id: app_id,
            account_id: account_id
        }
        
        return jwt.sign(token, _jwtAuthTokenSecret);
    }

    function getAuthToken(token)
    {
        return jwt.verify(code, _jwtAuthTokenSecret);
    }

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

    // The '/login' endpoint generates and returns a JWT token.  All endpoints except '/login' will 
    // require a JWT token (via Authorization header), and those endpoints can access the token payload
    // via req.user.  
    //
    server.use(restifyJwt({ secret: _jwtAuthTokenSecret}).unless({path: [/\/public\/?.*/, '/login', '/oauth2/authorize', '/oauth2/token', '/1/connect']}));

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
       log.error("Uncaught exception:", err.message, err.stack);
       res.send(err);
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

    //
    // !!! Auth endpoint (not from Dropbox API) - Provide the client_id, email, password, get a bearer token
    //     back without any of the OAuth shenanigans (for testing purposes only).
    //
    server.post('/login', restify.queryParser(), restify.bodyParser(), restify.authorizationParser(), function(req, res, next)
    {
        // Using queryParser and bodyParser will populate req.params from either URL params or
        // form encoded body.

        // The authorizationParser will process auth and populate req.authorization.  We support
        // basic auth via this mechanism.
        //
        // !!! No way to get client id with basic auth, so maybe we don't need/want this?  But maybe we want
        //     to support basic auth for general site login from the web ux later?
        //
        if (req.authorization.basic)
        {
            req.params.email = req.authorization.basic.username;
            req.params.password = req.authorization.basic.password;
        }

        try
        {
            assert.string(req.params.email, "email");
            assert.string(req.params.password, "password");
            assert.string(req.params.client_id, "client_id");
        }
        catch (err)
        {
            req.log.error("%s parameter validation failure:", req.route.path, err.message);
            return next(res.send(400, err));
        }

        req.log.info("Login attempt for email:", req.params.email);

        var userAccount = account.validateLogin(req.params.client_id, req.params.email, req.params.password);
        if (userAccount)
        {
            var token = createAuthToken(req.params.client_id, userAccount.account_id);
            res.send(token);
        }
        else
        {
            res.send(403, "Authentication failed");
        }

        next();
    });

    var authorizeTmpl = handlebars.compile(fs.readFileSync('./web/templates/authorize.hbs').toString());

    function writeResponseBody(res, body)
    {
        // I attempted adding a text/html formatter, but that had a bunch of unintended side effects, most
        // notably, it returned JSON objects as text/html by default (unless there was an Accept header that
        // specified application/json).  So the formatter isn't just "format the thing as the c/t I told you
        // it was", it also coerces the format based on "Accept" (defaulting to text/html if that formatter exists).
        //
        // Below was suggested as an alternative in the Restify docs and seems to work (with no side-effects).
        // Resitfy 5.x has a sendRaw() method which I think would accomplish the same thing a little more cleanly.
        //
        res.writeHead(200, {
          'Content-Length': Buffer.byteLength(body),
          'Content-Type': 'text/html'
        });
        res.write(body);
        res.end();
    }

    // This is a "special" endpoint that the official DropBox APIs use instead of using OAuth like
    // any reasonably implementation would do.
    //
    server.get('/1/connect', restify.queryParser(), function(req, res, next)
    {
        // https://www.dropbox.com/1/connect?k=p4f6ljgau5kpd7g&state=oauth2%3A12345678901234567890123456789012

        // k = app key
        // n = already authorized uid (uid is DropBox account_id for user)
        // api = api (or default if not provided)
        // state = opaque state to be passed back (in practice: "oauth2:[32 char nonce]")

        try
        {
            assert.string(req.params.k, "k");
        }
        catch (err)
        {
            req.log.error("%s parameter validation failure:", req.route.path, err.message);
            return next(res.send(400, err));
        }

        var tmplParams =
        {
            client_id: req.params.k,
            state: req.params.state
        }

        var body = authorizeTmpl(tmplParams);
        writeResponseBody(res, body);
        next();
    });

    server.post('/1/connect', restify.bodyParser(), function(req, res, next)
    {
        // When login submitted from /connect form (GET request above), it is posted here.
        //
        req.log.info("/1/connect (POST) - form posted for email: %s, state: %s", req.params.email, req.params.state);

        // Verify client_id and user/pass
        //
        var userAccount = account.validateLogin(req.params.client_id, req.params.email, req.params.password);
        if (userAccount)
        {
            req.log.info("Login succeeded");

            // !!! There is a potential security issue here, as the client app never provides its
            //     secret (AFAICT) with this mode of authorization.  Investigate.
            //
            var token = createAuthToken(userAccount.app_id, userAccount.account_id);

            // redirects to db-[app_id]://1/connect? 
            //   oauth_token=oauth2:
            //   oauth_token_secret=[oauth token]
            //   uid=[account_id]
            //   state=[passed-in state]

            var redirect = "db-" + userAccount.app_id + "://1/connect";

            // oauth_token, oauth_token_secret, uid
            //
            redirect += "?oauth_token=oauth2:&oauth_token_secret=" + token + "&uid=" + userAccount.account_id;

            if (req.params.state)
            {
                redirect += "&state=" + req.params.state;
            }

            req.log.info("Redirecting to:", redirect);

            res.redirect(redirect, next);
        }
        else
        {
            req.params.error = "Login failed";
            var body = authorizeTmpl(req.params);
            writeResponseBody(res, body);
            next();
        }
    });

    //
    // OAuth2 endpoints (www.dropbox.com/oauth2)
    //
    server.get('/oauth2/authorize', restify.queryParser(), function(req, res, next)
    {
        // response_type (String) - The grant type requested, either token or code.
        // client_id (String) - The app's key, found in the App Console.
        // redirect_uri (String?) - Where to redirect the user after authorization has completed. This must be the exact URI registered in the App Console; even 'localhost' must be listed if it is used for testing. All redirect URIs must be HTTPS except for localhost URIs. A redirect URI is required for the token flow, but optional for the code flow. If the redirect URI is omitted, the code will be presented directly to the user and they will be invited to enter the information in your app.
        // state (String?) - Up to 500 bytes of arbitrary data that will be passed back to your redirect URI. This parameter should be used to protect against cross-site request forgery (CSRF). See Sections 4.4.1.8 and 4.4.2.5 of the OAuth 2.0 threat model spec.
        // require_role (String?) - If this parameter is specified, the user will be asked to authorize with a particular type of Dropbox account, either work for a team account or personal for a personal account. Your app should still verify the type of Dropbox account after authorization since the user could modify or remove the require_role parameter.
        // force_reapprove (Boolean?) - Whether or not to force the user to approve the app again if they've already done so. If false (default), a user who has already approved the application may be automatically redirected to the URI specified by redirect_uri. If true, the user will not be automatically redirected and will have to approve the app again.
        // disable_signup (Boolean?) - When true (default is false) users will not be able to sign up for a Dropbox account via the authorization page. Instead, the authorization page will show a link to the Dropbox iOS app in the App Store. This is only intended for use when necessary for compliance with App Store policies.
        // locale (String?) - If the locale specified is a supported language, Dropbox will direct users to a translated version of the authorization website. Locale tags should be IETF language tags.
        // force_reauthentication (Boolean?) - When true (default is false) users will be signed out if they are currently signed in. This will make sure the user is brought to a page where they can create a new account or sign in to another account. This should only be used when there is a definite reason to believe that the user needs to sign in to a new or different account.

        try
        {
            assert.string(req.params.response_type, "response_type");
            assert.string(req.params.client_id, "client_id");
        }
        catch (err)
        {
            req.log.error("%s parameter validation failure:", req.route.path, err.message);
            return next(res.send(400, err));
        }

        // !!! Probably want to test/assert params here so we don't return login form and start that process
        //     if the preconditions are not met.
        //
        // !!! account.validateRedirect(req.params.client_id, req.params.redirect_uri);
        //
        req.log.info("oauth2/authorize (GET) - params:", req.params);

        var body = authorizeTmpl(req.params);
        writeResponseBody(res, body);
        next();
    });

    server.post('/oauth2/authorize', restify.bodyParser(), function(req, res, next)
    {
        // When login submitted from auth form (GET request above), it is posted here.
        //
        req.log.info("oauth2/authorize (POST) - form posted for email: %s, state: %s", req.params.email, req.params.state);

        // Verify client_id, user/pass, and redirect as appropriate (based on response_type)
        //
        var userAccount = account.validateLogin(req.params.client_id, req.params.email, req.params.password);
        if (userAccount)
        {
            req.log.info("Login succeeded");

            // !!! In case some smartass changed this from the original request, re-verify it here:
            //    
            //     account.validateRedirect(req.params.client_id, req.params.redirect_uri);

            var redirect = req.params.redirect_uri;

            if (req.params.response_type === 'code')
            {
                // JWT { type: "access_code", client_id, account_id, timestamp }
                //
                var code = createAuthCode(userAccount.app_id, userAccount.account_id);

                // code, state
                //
                redirect += "?code=" + code; 
            }
            else // token
            {
                // !!! There is a potential security issue here, as the client app never provides its
                //     secret (AFAICT) with this mode of authorization.  Investigate.
                //
                var token = createAuthToken(userAccount.app_id, userAccount.account_id);

                // access_token, token_type (bearer), account_id, state
                //
                redirect += "?access_token=" + token + "&token_type=bearer&account_id=" + userAccount.account_id;
            }

            if (req.params.state)
            {
                redirect += "&state=" + req.params.state;
            }

            req.log.info("Redirecting to:", redirect);

            res.redirect(redirect, next);
        }
        else
        {
            req.params.error = "Login failed";
            var body = authorizeTmpl(req.params);
            writeResponseBody(res, body);
            next();
        }
    });

    server.post('/oauth2/token', restify.queryParser(), restify.bodyParser(), restify.authorizationParser(), function(req, res, next)
    {
        // code (String) - The code acquired by directing users to /oauth2/authorize?response_type=code.
        // grant_type (String) - The grant type, which must be authorization_code.
        // client_id (String?) - If credentials are passed in POST parameters, this parameter should be present and should be the app's key (found in the App Console).
        // client_secret (String?) - If credentials are passed in POST parameters, this parameter should be present and should be the app's secret.
        // redirect_uri (String?) - Only used to validate that it matches the original /oauth2/authorize, not used to redirect again.

        var client_id;
        var client_secret;

        if (req.authorization.basic)
        {
            // If basic auth is used here, username is client_id and password is client_secret
            //
            client_id = req.authorization.basic.username;
            client_secret = req.authorization.basic.password;
        }
        else
        {
            client_id = req.params.client_id;
            client_secret = req.params.client_secret;
        }

        // Decode code (JWT) and make sure it is legit, and that the client/app id matches client_id
        //
        var accessCode = getAuthCode(req.params.code);
        if (accessCode.app_id === client_id)
        {
            if (account.validateApp(client_id, client_secret))
            {
                // Winner!  Return bearer token (and account_id).
                //
                var token = createAuthToken(accessCode.app_id, accessCode.account_id);
                res.send({"access_token": token, "token_type": "bearer", "account_id": accessCode.account_id })
            }
            else
            {
                req.log.error("oauth2/token - incorrect client_secret");
                res.send(403, "Authentication failed");
            }
        }
        else
        {
            req.log.error("oauth2/token - client_id didn't match code");
            res.send(403, "Authentication failed");
        }
    });

    //
    // "Users" API endpoints (api.dropboxapi.com/2/*)
    //

    server.post('/2/users/get_account', restify.bodyParser(), function(req, res, next)
    {
        var apiArgs = getApiArgs(req, true);

        try
        {
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
                    // !!! Should we throw a better Dropbox error?
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
                    // !!! Should we throw a better Dropbox error?
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

    function filesDownload(req, res, next)
    {
        var apiArgs = getApiArgs(req, false);

        try
        {
            assert.string(apiArgs.path, "path");
        }
        catch (err)
        {
            req.log.error("%s parameter validation failure:", req.route.path, err.message);
            res.contentType = "text/plain"; // Per Dropbox API - 400 details in text/plain
            return next(res.send(400, err.message));
        }

        req.log.info("Download for account_id: %s from path: %s", req.user.account_id, apiArgs.path);

        driver.getObject(req.user, apiArgs.path, function(err, entry, stream)
        {
            if (err)
            {
                req.log.error("Error on download:", err);
                return next(dbErr.returnDropboxError(res, err));
            }
            else if (stream)
            {
                // !!! Propagate content-type and other headers from drivers that support them.
                //
                res.writeHead(200, 
                {
                    'Content-Type': 'application/octet-stream',
                    'Dropbox-API-Result': JSON.stringify(entry)
                });
                stream.pipe(res); // Pipe store object contents to output
                stream.on('end',function() 
                {
                    req.log.info("Stream written, returning result");
                });

                next();
            }
            else
            {
                return next(dbErr.returnDropboxErrorNew(res, "path", "not_found", "File not found: " + apiArgs.path));
            }
        });
    }

    // Download supports GET with path in query params, as well as POST with path in API args header
    //
    // !!! Also, important: These endpoints also support HTTP GET along with ETag-based caching (If-None-Match)
    //     and HTTP range requests.
    //
    //     This means we need to make sure the ETag and other headers get passed through to the drivers (specifically
    //     Manta) such that ETag-based caching (along with presumably If-Modified-Since) as well as range requests
    //     work properly.  And test this (natch).
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
        var apiArgs = getApiArgs(req, false);

        try
        {
            assert.string(apiArgs.path, "path");
        }
        catch (err)
        {
            req.log.error("%s parameter validation failure:", req.route.path, err.message);
            res.contentType = "text/plain"; // Per Dropbox API - 400 details in text/plain
            return next(res.send(400, err.message));
        }

        req.log.info("Upload for account_id: %s to path: %s", req.user.account_id, apiArgs.path);

        // !!! Pass content-type and other headers to drivers that support them
        //

        driver.putObject(req.user, apiArgs.path, function(err, stream)
        {
            if (err)
            {
                req.log.error(err, "Error starting upload");
                return next(dbErr.returnDropboxError(res, err));
            }

            pipeRequest(req, stream, function(err, details)
            {
                if (err)
                {
                    req.log.error(err, "Error piping request to upload stream");
                    return next(dbErr.returnDropboxError(res, err));
                }
                else
                {
                    log.info("Piping request to upload stream for '%s' complete", apiArgs.path);

                    // !!! Details in the case of Manta is the full response from the Manta request
                    //
                    log.info("Put response:", details);

                    // req headers
                    /*
                    { 
                        'accept': 'application/json',
                        'content-type': 'text/plain',
                        'expect': '100-continue',
                        'x-request-id': 'b3c6da1b-fb1b-4f1a-bd73-ba5c3bec3bd5',
                        'transfer-encoding': 'chunked',
                        'date': 'Tue, 27 Jun 2017 09:49:00 GMT',
                        'authorization': 'Signature keyId="/synchro/keys/8c:09:65:e3:74:54:52:3f:c1:82:3b:5d:cd:09:bc:f4",algorithm="rsa-sha256",headers="date",signature="oQW4FdUEobR2JPmeAPCXO5Il0UNibij0RP2N8Sjwxbsp0tbyN9rZ9+rzqU0V9Mt2uD5UmohP119EHWBcjGE1opUqYPDa6nuQ2j+s8xogWrgv50e4Fjz8z7qudUo0nXng3rqU5CY7O8FvWAEMih64WmV22BXrWqRMwhtM3yHyUBEoijGaPV0/xfbFvQ3E5jdgm+ye9UzYx5T0hxLRAj+gk/kSLinxA55hXrkcN475U4zIq5xRrat8qjvui8rk0A6JpMKbap86arU9Gg7wS/lxbm5GVxMGUIa6X8TITeJhxF1aIJ/ggXV5eILJS3IKQ9E893JCOLzqZw856u7ohQjrZw=="',
                        'user-agent': 'restify/1.4.1 (x64-darwin; v8/4.6.85.31; OpenSSL/1.0.2g) node/5.8.0',
                        'accept-version': '~1.0',
                        'host': 'us-east.manta.joyent.com' 
                    }
                    */

                    // res (details)
                    /*
                    statusCode: 204,
                    statusMessage: 'No Content',
                    headers:
                    { 
                        'etag': '79e13f21-21c9-c49c-b99c-d8dc019a3410',
                        'last-modified': 'Tue, 27 Jun 2017 09:49:01 GMT',
                        'computed-md5': 'XktjRTneAeEWI25gOIGhUQ==',
                        'date': 'Tue, 27 Jun 2017 09:49:01 GMT',
                        'server': 'Manta',
                        'x-request-id': 'b3c6da1b-fb1b-4f1a-bd73-ba5c3bec3bd5',
                        'x-response-time': '440',
                        'x-server-name': '1a1ff4e5-4e04-4e60-b993-689f95b67e89',
                        'connection': 'keep-alive',
                        'x-request-received': 1498556940981,
                        'x-request-processing-time': 917 
                    }
                    */

                    // Subsequent entry info
                    /*
                    { 
                        name: 'bar.txt',
                        etag: '79e13f21-21c9-c49c-b99c-d8dc019a3410',
                        size: 11,
                        type: 'object',
                        mtime: '2017-06-27T09:49:01.561Z',
                        durability: 2,
                        parent: '/synchro/stor/mantabox/1234-BEEF/000001' 
                    }
                    */

                    // !!! For some cases, like a single file copy, the file name not be there yet and we may
                    //     need to retry.  We proably need a wrapper for this that auto-retires specifically
                    //     for cases like this where we know the item is eventually going to show up.
                    //
                    driver.getObjectMetaData(req.user, apiArgs.path, function(err, entry)
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
                }
            });

            next();
        });
    }

    server.post('/2/files/upload', filesUpload);

    // Multipart upload
    //

    function startMultipartUpload(user, callback)
    {
        if (driver.startMultipartUpload)
        {
            driver.startMultipartUpload(user, callback);
        }
        else
        {
            var uploadId = uuidv4();
            var uploadPath = path.join("uploads", uploadId, "0.bin");

            driver.putObject(stripAppId(user), uploadPath, function(err, stream)
            {
                callback(err, uploadId, stream);
            });
        }
    }

    function multipartUpload(user, uploadId, offset, callback)
    {
        if (driver.multipartUpload)
        {
            driver.multipartUpload(user, uploadId, offset, callback);
        }
        else
        {
            var uploadPath = path.join("uploads", uploadId, offset.toString() + ".bin");
            driver.putObject(stripAppId(user), uploadPath, callback);
        }
    }

    function finishMultipartUpload(user, uploadId, filename, callback)
    {
        if (driver.finishMultipartUpload)
        {
            driver.finishMultipartUpload(user, uploadId, filename, callback);
        }
        else
        {
            var uploadDirPath = path.join("uploads", uploadId);

            getAccountDirectoryEntries(user, uploadDirPath, function(err, entries)
            {
                if (err)
                {
                    callback(err);
                }
                else if (!entries)
                {
                    callback(new Error("No files uploaded with the id:", uploadId));
                }
                else
                {
                    // Sort entries by offset
                    //
                    entries.sort(function(a, b)
                    {
                        return a.offset - b.offset;
                    });

                    log.info("Entries", entries);

                    // !!! Verify that we start at 0 and there are no holes
                    //

                    driver.putObject(user, filename, function(err, writeStream)
                    {
                        async.eachSeries(entries, function (entry, callback) 
                        {
                            log.info("Processing upload entry:", entry);
                            var currentFile = path.join(uploadDirPath, entry.name);
                            driver.getObject(stripAppId(user), currentFile, function(err, entry, readStream)
                            {
                                readStream.on('end', function ()
                                {
                                    log.info("Done piping stream for entry:", entry);
                                    callback();
                                });

                                log.info("Pipe stream for entry:", entry);
                                readStream.pipe(writeStream, { end: false });
                            });
                        }, 
                        function(err)
                        {
                            log.info("Done appending uploaded files");
                            writeStream.end();

                            deleteAccountDirectory(user, uploadDirPath, function(err)
                            {
                                // We hope this worked, but we're not going to error out just
                                // because the upload dir delete failed.
                                //
                                if (err)
                                {
                                    log.error("Failed to delete upload dir:", uploadDirPath, err);
                                }

                                driver.getObjectMetaData(user, filename, function(err, srcEntry)
                                {
                                    callback(null, srcEntry);
                                });
                            });
                        });
                    });
                }
            });
        }
    }

    function uploadSessionStart(req, res, next)
    {
        var apiArgs = getApiArgs(req, false);
        req.log.info("Upload session start for account_id: %s", req.user.account_id);

        // !!! Should process apiArgs.close == true (it seems like you still have to call finish, but this
        //     signals that no more parts are coming).

        startMultipartUpload(req.user, function(err, sessionId, stream)
        {
            if (err)
            {
                req.log.error(err, "Error starting upload session start");
                return next(dbErr.returnDropboxError(res, err));
            }

            pipeRequest(req, stream, function(err, details)
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

        multipartUpload(req.user, apiArgs.cursor.session_id, apiArgs.cursor.offset, function(err, stream)
        {
            if (err)
            {
                req.log.error(err, "Error on upload session append");
                return next(dbErr.returnDropboxError(res, err));
            }

            pipeRequest(req, stream, function(err, details)
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

    function uploadSessionFinish(req, res, next)
    {
        var apiArgs = getApiArgs(req, false);
        req.log.info("End upload session %s, offset: %n", apiArgs.cursor.session_id, apiArgs.cursor.offset);

        multipartUpload(req.user, apiArgs.cursor.session_id, apiArgs.cursor.offset, function(err, stream)
        {
            if (err)
            {
                req.log.error(err, "Error on upload session end");
                return next(dbErr.returnDropboxError(res, err));
            }

            pipeRequest(req, stream, function(err, details)
            {
                if (err)
                {
                    req.log.error(err, "Error piping request to upload session end stream");
                    return next(dbErr.returnDropboxError(res, err));
                }
                else
                {
                    log.info("Piping request to upload session end stream succeeded");

                    finishMultipartUpload(req.user, apiArgs.cursor.session_id, apiArgs.commit.path, function(err, item)
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

    // The multipart upload endpoints are "content" endpoints because start, append, and finish all have data
    // payload (file segments)
    //
    server.post('/2/files/upload_session/start', uploadSessionStart);
    server.post('/2/files/upload_session/append', function(req, res, next)
    {
        // Upgrade this to a v2 request
        //
        var apiArgs = getApiArgs(req, false);

        apiArgs = { "cursor": apiArgs };

        uploadSessionAppend(apiArgs, req, res, next);
    });
    server.post('/2/files/upload_session/append_v2', function(req, res, next)
    {
        uploadSessionAppend(getApiArgs(req), req, res, next);
    });
    server.post('/2/files/upload_session/finish', uploadSessionFinish);

    //
    // "Files" API endpoints (api.dropboxapi.com/2/*)
    //

    // !!! Dropbox nomenclature is "folder" or "file" (either of which is considered an "entry")
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

    function encodeCursor(path, recursive, limit, lastItem)
    {
        var cursor = 
        { 
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

    // Helper (driver call with fallback)
    //
    function getEntrySortKey(entry)
    {
        if (driver.getEntrySortKey)
        {
            return driver.getEntrySortKey(entry);
        }
        else
        {
            return entry["server_modified"] + entry["path_display"];
        }
    }

    // Helper (driver call with fallback)
    //
    function getCursorItem(entry)
    {
        if (driver.getCursorItem)
        {
            return driver.getCursorItem(entry);
        }
        else
        {
            var cursorItem = null;

            if (entry)
            {
                cursorItem = 
                {
                    "server_modified": entry["server_modified"],
                    "path_display": entry["path_display"]
                }
            }

            return cursorItem;
        }
    }

    // Helper
    //
    function isCursorItemNewer(item1, item2)
    {
        return (!item1 || (getEntrySortKey(item1) < getEntrySortKey(item2)));
    }

    // Helper (driver call with fallback)
    //
    function listFolderUsingCursor(user, dirPath, recursive, limit, cursor, callback)
    {
        if (driver.listFolderUsingCursor)
        {
            driver.listFolderUsingCursor(user, dirPath, recursive, limit, cursor, callback);
        }
        else // Fall back to traverse (brute force)
        {
            var entries = [];

            limit = limit || 999999;

            function onEntry(entry)
            {
                // If there is a cursor, only process entries greater than the cursor
                //
                if (!cursor || (getEntrySortKey(cursor) < getEntrySortKey(entry)))
                {
                    // This will insert into "entries" such that "entries" will be/stay in sorted order
                    //
                    entries.splice(lodash.sortedIndexBy(entries, entry, function(o){ return getEntrySortKey(o); }), 0, entry);

                    // This will keep the list from growing beyond more than one over the limit (we purposely
                    // leave the "extra" entry so that at the end we will be able to see that we went past
                    // the limit).
                    //
                    if (entries.length > limit + 1)
                    {
                        entries.splice(limit + 1);
                    }
                }
            }

            driver.traverseDirectory(user, dirPath, recursive, onEntry, function(err, stopped)
            {
                if (err)
                {
                    // !!! req logger?
                    log.error("Traversal error on listFolderUsingCursor:", err);
                    callback(err);
                }
                else
                {
                    var hasMore = false;
                    var cursorItem = cursor && cursor.lastItem;

                    if (entries.length > limit)
                    {
                        entries.splice(limit);
                        hasMore = true;
                    }

                    if (entries.length > 0)
                    {
                        cursorItem = getCursorItem(entries[entries.length-1]);
                    }

                    callback(null, entries, hasMore, cursorItem);
                }
            });
        }
    }

    // Helper (driver call with fallback)
    //
    function getLatestCursorItem(user, path, recursive, callback)
    {
        if (driver.getLatestCursorItem)
        {
            driver.getLatestCursorItem(user, path, recursive, callback);
        }
        else // Fall back to traverse (brute force)
        {
            var latestEntry = null;

            function onEntry(entry)
            {
                if (!latestEntry)
                {
                    latestEntry = entry;
                }
                else
                {
                    var entrySortKey = getEntrySortKey(entry);
                    var latestEntrySortKey = getEntrySortKey(latestEntry);

                    if (entrySortKey > latestEntrySortKey)
                    {
                        latestEntry = entry;
                    }
                }
            }

            driver.traverseDirectory(user, path, recursive, onEntry, function(err, stopped)
            {
                if (err)
                {
                    // !!! req logger?
                    log.error("Traversal error on getLatestCursorItem:", err);
                    callback(err);
                }
                else
                {
                    callback(null, getCursorItem(latestEntry));
                }
            });
        }
    }

    // Helper (driver call with fallback)
    //
    function isAnyCursorItemNewer(user, path, recursive, cursorItem, callback)
    {
        if (driver.isAnyCursorItemNewer)
        {
            driver.isAnyCursorItemNewer(user, path, recursive, cursorItem, callback);
        }
        else if (driver.getLatestCursorItem)
        {
            driver.getLatestCursorItem(user, path, recursive, function(err, latestCursorItem)
            {
                callback(err, isCursorItemNewer(latestCursorItem, cursorItem));
            });
        }
        else // Fall back to traverse (brute force)
        {
            function onEntry(entry)
            {
                return isCursorItemNewer(getCursorItem(entry), cursorItem);
            }

            driver.traverseDirectory(user, path, recursive, onEntry, function(err, stopped)
            {
                if (err)
                {
                    // !!! req logger?
                    log.error("Traversal error on isAnyCursorItemNewer:", err);
                    callback(err);
                }
                else
                {
                    callback(null, stopped);
                }
            });
        }
    }

    function filesListFolder(req, res, next)
    {
        var apiArgs = getApiArgs(req, true);

        try
        {
            assert.string(apiArgs.path, "path");
            assert.optionalBool(apiArgs.recursive, "recursive");
        }
        catch (err)
        {
            req.log.error("%s parameter validation failure:", req.route.path, err.message);
            res.contentType = "text/plain"; // Per Dropbox API - 400 details in text/plain
            return next(res.send(400, err.message));
        }

        req.log.info("List folders for app_id: %s, account_id: %s at path: %s", req.user.app_id, req.user.account_id, apiArgs.path);

        var limit = apiArgs.limit || defaultListFolderLimit;

        listFolderUsingCursor(req.user, apiArgs.path, apiArgs.recursive, limit, null, function(err, items, hasMore, cursorItem)
        {
            if (err)
            {
                req.log.error("Error on list folder:", err);
                return next(dbErr.returnDropboxError(res, err));
            }

            var cursorString = encodeCursor(apiArgs.path, !!apiArgs.recursive, limit, cursorItem)

            res.send({ entries: items, cursor: cursorString, has_more: hasMore });
            next();
        });
    }

    function filesListFolderContinue(req, res, next)
    {
        var apiArgs = getApiArgs(req, true);

        try
        {
            assert.string(apiArgs.cursor, "cursor");
        }
        catch (err)
        {
            req.log.error("%s parameter validation failure:", req.route.path, err.message);
            res.contentType = "text/plain"; // Per Dropbox API - 400 details in text/plain
            return next(res.send(400, err.message));
        }

        req.log.info("List folders continue for account_id: %s", req.user.account_id);

        var cursor = decodeCursor(apiArgs.cursor);

        listFolderUsingCursor(req.user, cursor.path, cursor.recursive, cursor.limit, cursor.lastItem, function(err, items, hasMore, cursorItem)
        {
            if (err)
            {
                req.log.error("Error on list folder continue:", err);
                return next(dbErr.returnDropboxError(res, err));
            }

            var newCursorString = encodeCursor(cursor.path, cursor.recursive, cursor.limit, cursorItem)

            res.send({ entries: items, cursor: newCursorString, has_more: hasMore });
            next();
        });
    }

    function filesListFolderGetLatestCursor(req, res, next)
    {
        var apiArgs = getApiArgs(req, true);

        try
        {
            assert.string(apiArgs.path, "path");
            assert.optionalBool(apiArgs.recursive, "recursive");
        }
        catch (err)
        {
            req.log.error("%s parameter validation failure:", req.route.path, err.message);
            res.contentType = "text/plain"; // Per Dropbox API - 400 details in text/plain
            return next(res.send(400, err.message));
        }

        req.log.info("Get latest cursor for account_id: %s at path: %s", req.user.account_id, apiArgs.path);

        var limit = apiArgs.limit || defaultListFolderLimit;

        getLatestCursorItem(req.user, apiArgs.path, apiArgs.recursive, function(err, cursorItem)
        {
            var cursorString = encodeCursor(apiArgs.path, apiArgs.recursive, limit, cursorItem);
            res.send({ cursor: cursorString });
            next();
        });
    }

    function filesListFolderLongPoll(req, res, next)
    {
        var apiArgs = getApiArgs(req, true);

        try
        {
            assert.string(apiArgs.cursor, "cursor");
            assert.optionalNumber(apiArgs.timeout, "timeout");
        }
        catch (err)
        {
            req.log.error("%s parameter validation failure:", req.route.path, err.message);
            res.contentType = "text/plain"; // Per Dropbox API - 400 details in text/plain
            return next(res.send(400, err.message));
        }

        req.log.info("List folder long poll for account_id: %s", req.user.account_id);

        var cursor = decodeCursor(apiArgs.cursor);

        var timeout = (apiArgs.timeout || 30) * 1000; // !!! Plus "up to 90 seconds of random jitter added to avoid the thundering herd problem"

        var interval = 1000; // !!! Configurable?  Per-driver?

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
                isAnyCursorItemNewer(req.user, cursor.path, cursor.recursive, cursor.lastItem, function(err, isNewer)
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
                        setTimeout(function() { callback(); }, interval);
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
        var apiArgs = getApiArgs(req, true);

        try
        {
            assert.string(apiArgs.path, "path");
        }
        catch (err)
        {
            req.log.error("%s parameter validation failure:", req.route.path, err.message);
            res.contentType = "text/plain"; // Per Dropbox API - 400 details in text/plain
            return next(res.send(400, err.message));
        }

        req.log.info("Create folder for account_id: %s at path: %s", req.user.account_id, apiArgs.path);

        driver.createDirectory(req.user, apiArgs.path, function(err, item)
        {
            if (err)
            {
                req.log.error("Error on create folder:", err);
                return next(dbErr.returnDropboxError(res, err));
            }
            res.send(item);
            next();
        });
    }

    // In Dropbox
    //
    //   * When move/copy/delete, item returned is a single item representing the destination object (file or folder)
    //   * conflict detection (for autorename) is only at top level item (no folder merging w/ rename of contents)
    //   * folder items don't have mtime
    //
    // Dropbox errors
    //
    //   too_many_files
    //   duplicated_or_nested_paths
    //   cant_move_folder_into_itself
    //   other
    //
    function validateBulkOperation(user, srcpath, dstpath, operation, mode, maxItems, callback)
    {
        // operation = move, copy, delete
        // mode (for move/copy) = add (no overwrite if conflict), overwrite, rename
        // maxItems = If we exceed this, stop and return too_many_files
        //
        var itemCount = 0;

        log.info("srcpath", srcpath);

        async.waterfall([
            function(callback) // Get source entry metadata 
            {
                log.info("Getting source entry");
                driver.getObjectMetaData(user, srcpath, function(err, srcEntry)
                {
                    log.info("Got callback");
                    if (err)
                    {
                        callback(err);
                    }
                    else if (!srcEntry)
                    {
                        callback(new dbErr.DropboxError("from_lookup", "not_found", "Source file not found")); 
                    }
                    else
                    {
                        log.info("Got srcEntry", srcEntry);
                        callback(null, srcEntry);
                    }
                });
            },
            function(srcEntry, callback) // Ensure entry count is less than maxItems if maxItems specified
            {
                log.info("Validating item count, if applicable");
                if (maxItems)
                {
                    if (srcEntry[".tag"] === "file")
                    {
                        itemCount = 1;
                        callback(null, srcEntry);
                    }
                    else
                    {
                        function onEntry(entry)
                        {
                            return (++itemCount > maxItems);
                        }

                        driver.traverseDirectory(user, srcpath, true, onEntry, function(err, stopped)
                        {
                            if (stopped)
                            {
                                err = new dbErr.DropboxError("too_many_files");
                            }
                            callback(err, srcEntry);
                        });
                    }
                }
                else
                {
                    callback(null, srcEntry);
                }
            },
            function(srcEntry, callback) // Get destination entry metadata (if move/copy)
            {
                log.info("Getting destination entry if applicable");
                if ((operation === "move") || (operation === "copy"))
                {
                    driver.getObjectMetaData(user, dstpath, function(err, dstEntry)
                    {
                        if (err)
                        {
                            callback(err);
                        }
                        else
                        {
                            callback(null, srcEntry, dstEntry);
                        }
                    });
                }
                else
                {
                    callback(null, srcEntry, null);
                }
            },
            function(srcEntry, dstEntry, callback) // Resolve destination conflicts (if applicable)
            {
                log.info("Resolving destination conflicts (if applicable)");
                try 
                {
                    if (operation === "delete")
                    {
                        callback(null, srcEntry, null);
                    }
                    else // move/copy
                    {
                        if (dstEntry)
                        {
                            if (mode === "add")
                            {
                                // FAIL - dstEntry exists
                                //
                                callback(new dbErr.DropboxError("to", "conflict", "Destination already exists"));
                            }
                            else if (mode === "rename")
                            {
                                // Determine a new valid (non-existant) dst path we can use
                                //
                                var validIndex = null;

                                var start = 1;
                                var tries = 10;
                                var indexes = [];

                                var parsedPath = path.parse(dstpath);
                                var dstName = parsedPath.name;

                                // If dstpath already contains an index suffix, start after that...
                                //
                                var matches = /^(.*)\s\((\d*)\)$/.exec(dstName);
                                if (matches)
                                {
                                    dstName = matches[1];
                                    start = parseInt(matches[2]) + 1;
                                }

                                for (var i = 0; i < tries; i++)
                                {
                                    indexes[i] = i + start;
                                }

                                // For each candate, do driver.getObjectMetaData, if doesn't exist, good, else increment and repeat
                                //
                                async.someSeries(indexes, function(index, callback)
                                {
                                    var testPath = path.join(parsedPath.dir, dstName + " (" + index + ")") + parsedPath.ext;
                                    log.info("Getting metadata for", testPath);
                                    driver.getObjectMetaData(user, testPath, function(err, entry)
                                    {
                                        if (err)
                                        {
                                            callback(err);
                                        }
                                        else
                                        {
                                            if (!entry)
                                            {
                                                validIndex = index;
                                            }
                                            callback(null, !entry);
                                        }
                                    });
                                },
                                function(err, result)
                                {
                                    if (err)
                                    {
                                        callback(err);
                                    }
                                    else if (result)
                                    {
                                        // A valid index was found (one of the series functions returned true)
                                        //
                                        log.info("Found usable index:", validIndex);
                                        var newDstPath = path.join(parsedPath.dir, dstName + " (" + validIndex + ")") + parsedPath.ext;
                                        callback(null, srcEntry, newDstPath);
                                    }
                                    else
                                    {
                                        // No valid index was found
                                        callback(new Error("No available paths for autorename"));
                                    }
                                });

                                // In case of folder, do we need to create it?
                            }
                            else // mode === "overwrite"
                            {
                                if (dstEntry[".tag"] === srcEntry[".tag"])
                                {
                                    // They're the same type, so we're good to overwrite
                                    //
                                    callback(null, srcEntry, dstEntry.path_display);
                                }
                                else
                                {
                                    // FAIL - dstEntry exists, but is not same type as srcEntry
                                    //
                                    callback(new dbErr.DropboxError("to", "conflict", "Destination already exists and is not the same type as source"));
                                }
                            }
                        }
                        else // dest entry doesn't exist
                        {
                            // If folder, make sure dstpath exists, if file, make sure parent of dstpath exists
                            //
                            var createDstPath = dstpath;
                            if (srcEntry[".tag"] === "file")
                            {
                                createDstPath = path.dirname(dstpath);
                            }

                            driver.createDirectory(user, createDstPath, function(err, dir)
                            {
                                callback(null, srcEntry, dstpath);
                            });
                        }
                    }
                } 
                catch (err)
                { 
                    log.error("Error resolving destination conflicts", err);
                    callback(err); 
                }
            }
        ], 
        function (err, srcEntry, dstPath)
        {
            if (err)
            {
                log.error("err validating:", err);
                callback(err);
            }
            else
            {
                // The idea is that by this point we have a valid operation with a valid dstPath, if applicable 
                // (name conflicts resolved, and if folder, will have been created), such that we're ready to just complete
                // the move/copy/delete operation without having to check for further conflicts.  Note that dstPath may 
                // not match dstpath (if rename due to conflict occurred).
                //
                log.info("validated:", itemCount, srcEntry, dstPath);
                callback(null, itemCount, srcEntry, dstPath);
            }
        });
    }

    function doBulkOperation(req, operation, workItems, callback)
    {
        // operation = move, copy, delete
        //
        // workItems is an array of: { [parent], srcEntry, dstPath, [remainingEntries] } 
        //
        //     "parent" refers to the workItem for the parent directory of the given workItem (if any)
        //
        //     "remainingEntries" is the count of child entries (will be decrememnted on move/delete as children deleted) 
        //
        var q = async.queue(function(workItem, done) 
        {
            log.info("workItem:", workItem);

            if (workItem.srcEntry[".tag"] === "file")
            {
                // File operation
                //
                if (operation === "copy")
                {
                    log.info("Copy file src to dst:", workItem);
                    driver.copyObject(req.user, workItem.srcEntry.path_display, workItem.dstPath, function(err, entry)
                    {
                        if (err)
                        {
                            req.log.error("Error on copy:", err);
                        }

                        done(err);
                    });
                }
                else if (operation === "move")
                {
                    log.info("Move file src to dst:", workItem);
                    driver.moveObject(req.user, workItem.srcEntry.path_display, workItem.dstPath, function(err, entry)
                    {
                        if (err)
                        {
                            req.log.error("Error on move:", err);
                        }

                        if (workItem.parent && (--workItem.parent.remainingEntries === 0))
                        {
                            // Scheule parent workItem for deletion
                            //
                            q.push(workItem.parent);
                        }

                        done(err);
                    });
                }
                else if (operation === "delete")
                {
                    log.info("Delete file src to dst:", workItem);
                    driver.deleteObject(req.user, workItem.srcEntry.path_display, function(err, entry)
                    {
                        if (err)
                        {
                            req.log.error("Error on delete:", err);
                        }

                        if (workItem.parent && (--workItem.parent.remainingEntries === 0))
                        {
                            // Scheule parent workItem for deletion
                            //
                            q.push(workItem.parent);
                        }

                        done(err);
                    });
                }
            }
            else
            {
                // Folder operation
                //
                if (workItem.remainingEntries === 0) // Only move/delete operations are tracking remaining entries
                {
                    log.info("Deleting folder itself:", workItem);
                    driver.deleteObject(req.user, workItem.srcEntry.path_display, function(err, entry)
                    {
                        if (err)
                        {
                            // !!! If it fails because it's not empty it could be a lag in deleted objects showing
                            //     as deleted, so we want to reschedule it and try again after a short delay.
                            //
                            req.log.error("Error on delete of directory:", err);
                        }

                        if (workItem.parent && (--workItem.parent.remainingEntries === 0))
                        {
                            // Scheule parent workItem for deletion
                            //
                            q.push(workItem.parent);
                        }

                        done(err);
                    });
                }
                else
                {
                    log.info("Process folder contents:", workItem);

                    listFolderUsingCursor(req.user, workItem.srcEntry.path_display, false, null, null, function(err, entries)
                    {
                        if (err)
                        {
                            req.log.error("Error on list folder:", err);
                            done(err);
                        }
                        else if (entries && entries.length)
                        {
                            if ((operation === "move") || (operation === "delete"))
                            {
                                // For move/delete:
                                //
                                // We pass the parent workItem as part of the new workItem, so the child workItem can complete,
                                // decrement the parent workItem remainingEntries, and if 0, schedule the parent for deletion.
                                //
                                workItem.remainingEntries = entries.length;
                            }

                            entries.forEach(function(entry)
                            {
                                var entryWorkItem = { parent: workItem, srcEntry: entry };
                                if (operation !== "delete")
                                {
                                    entryWorkItem.dstPath = path.join(workItem.dstPath, path.basename(entry.path_display));
                                }
                                q.push(entryWorkItem);
                            });

                            done();
                        }
                        else // Empty folder
                        {
                            if ((operation === "move") || (operation === "delete"))
                            {
                                // Schedule the source folder for deletion now (it was empty)
                                //
                                log.info("Pushing workitem for empty dir");
                                workItem.remainingEntries = 0;
                                q.push(workItem);
                            }

                            if ((operation === "move") || (operation === "copy"))
                            {
                                // If there are no entries in the source folder, we want to create the corresponding 
                                // destination folder (since it will not be created automatically when copying contents).
                                //
                                driver.createDirectory(req.user, workItem.dstPath, function(err, entry)
                                {
                                    if (err)
                                    {
                                        req.log.error("Error on creation of empty directory:", err);
                                    }
                                    done(err);
                                });
                            }
                            else
                            {
                                done();
                            }
                        }
                    });
                }
            }

        }, maxConcurrency);

        q.error = lodash.once(function(err, task)
        {
            q.kill();
            callback(err);
        });

        q.drain = function() 
        {
            // !!! Details?
            callback(null);
        };

        workItems.forEach(function(workItem)
        {
            q.push(workItem);
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
        var apiArgs = getApiArgs(req, true);

        try
        {
            assert.string(apiArgs.from_path, "from_path");
            assert.string(apiArgs.to_path, "to_path");
            assert.optionalBool(apiArgs.autorename, "autorename");
            assert.optionalBool(apiArgs.autorename, "overwrite");
        }
        catch (err)
        {
            req.log.error("%s parameter validation failure:", req.route.path, err.message);
            res.contentType = "text/plain"; // Per Dropbox API - 400 details in text/plain
            return next(res.send(400, err.message));
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

        validateBulkOperation(req.user, apiArgs.from_path, apiArgs.to_path, operation, mode, maxInteractive, function(err, itemCount, srcEntry, dstPath)
        {
            if (err)
            {
                req.log.error("Error validating %s:", operation, err);
                return next(dbErr.returnDropboxError(res, err));
            }
            else
            {
                log.info("%s bulk validation:", req.route.path, itemCount, srcEntry, dstPath);

                doBulkOperation(req, operation, [{ srcEntry: srcEntry, dstPath: dstPath }], function(err)
                {
                    if (err)
                    {
                        req.log.error("Error on %s:", operation, err);
                        return next(dbErr.returnDropboxError(res, err));
                    }

                    // !!! For some cases, like a single file copy, the file name not be there yet and we may
                    //     need to retry.  We proably need a wrapper for this that auto-retires specifically
                    //     for cases like this where we know the item is eventually going to show up.
                    //
                    driver.getObjectMetaData(req.user, dstPath, function(err, entry)
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
        var apiArgs = getApiArgs(req, true);

        try
        {
            assert.string(apiArgs.path, "path");
        }
        catch (err)
        {
            req.log.error("%s parameter validation failure:", req.route.path, err.message);
            res.contentType = "text/plain"; // Per Dropbox API - 400 details in text/plain
            return next(res.send(400, err.message));
        }

        req.log.info("Delete file for account_id: %s at path: %s", req.user.account_id, apiArgs.path);

        validateBulkOperation(req.user, apiArgs.path, null, "delete", null, maxInteractive, function(err, itemCount, srcEntry)
        {
            if (err)
            {
                req.log.error("Error validating delete:", err);
                return next(dbErr.returnDropboxError(res, err));
            }
            else
            {
                log.info("Delete bulk validation:", itemCount, srcEntry);

                doBulkOperation(req, "delete", [{ srcEntry: srcEntry }], function(err)
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
    // Batch helper functions for reading/writing/deleting JSON content to store
    //

    function stripAppId(user)
    {
        return { account_id: user.account_id };
    }

    function putAccountObject(user, path, content, callback)
    {
        var contentString = content;
        if (typeof content === 'object')
        {
            contentString = JSON.stringify(content, null, 4);
        }

        driver.putObject(stripAppId(user), path, function(err, writeStream)
        {
            if (err)
            {
                callback(err);
            }
            else
            {
                var readStream = new stream.Readable();
                readStream.push(contentString);
                readStream.push(null);

                pipeRequest(readStream, writeStream, function(err, details)
                {
                    callback(err);
                });
            }
        });
    }

    function getAccountObject(user, path, callback)
    {
        driver.getObject(stripAppId(user), path, function(err, entry, stream)
        {
            if (err)
            {
                callback(err);
            }
            else if (stream)
            {
                var chunks = [];

                stream.on("data", function (chunk)
                {
                    chunks.push(chunk);
                });

                stream.on("end", function () 
                {
                    callback(null, JSON.parse(Buffer.concat(chunks)));
                });

                stream.once('error', function(err)
                {
                    callback(err);
                });
            }
            else
            {
                // Object didn't exist
                callback(null);
            }
        });
    }

    function deleteAccountObject(user, path, callback)
    {
        driver.deleteObject(stripAppId(user), path, function(err)
        {
            callback(err);
        });
    }

    function createAccountDirectory(user, path, callback)
    {
        driver.createDirectory({account_id: user.account_id}, path, function(err)
        {
            callback(err);
        });
    }

    function deleteAccountDirectory(user, path, callback)
    {
        var req = { user: stripAppId(user), log: log };
        var srcEntry = { ".tag": "folder", path_display: path };
        doBulkOperation(req, "delete", [{ srcEntry: srcEntry }], function(err)
        {
            log.info("Account directory deleted:", path);
            callback(err);
        });
    }

    function getAccountDirectoryEntries(user, path, callback)
    {
        var entries = [];

        function onEntry(entry)
        {
            entries.push(entry);
        }

        driver.traverseDirectory({account_id: user.account_id}, path, false, onEntry, function(err, stopped)
        {
            callback(err, entries);
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
        var apiArgs = getApiArgs(req, true);

        try
        {
            assert.array(apiArgs.entries, "entries");
        }
        catch (err)
        {
            req.log.error("%s parameter validation failure:", req.route.path, err.message);
            res.contentType = "text/plain"; // Per Dropbox API - 400 details in text/plain
            return next(res.send(400, err.message));
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

                putAccountObject(req.user, "/jobs/" + jobId, response, function(err)
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
                async.eachLimit(apiArgs.entries, maxConcurrency, function(entry, callback)
                {
                    // Validate entry.path
                    //
                    validateBulkOperation(req.user, entry.from_path, entry.to_path, operation, mode, null, function(err, itemCount, srcEntry, dstPath)
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

                        putAccountObject(req.user, "/jobs/" + jobId, status, function(err)
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

                doBulkOperation(req, operation, workItems, function(err)
                {
                    if (err)
                    {
                        // Write validation err to job file
                        //
                        req.log.error("Error in bulk operation for batch %s:", operation, err);
                        var status = dbErr.getDropboxError(err);
                        status[".tag"] = "failed";

                        putAccountObject(req.user, "/jobs/" + jobId, status, function(err)
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
                async.eachLimit(workItems, maxConcurrency, function(workItem, callback)
                {
                    // !!! For some cases, like a single file copy, the file name not be there yet and we may
                    //     need to retry.  We proably need a wrapper for this that auto-retires specifically
                    //     for cases like this where we know the item is eventually going to show up.
                    //
                    driver.getObjectMetaData(req.user, workItem.dstPath, function(err, entry)
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

                putAccountObject(req.user, "/jobs/" + jobId, status, function(err)
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
        var apiArgs = getApiArgs(req, true);

        try
        {
            assert.array(apiArgs.entries, "entries");
        }
        catch (err)
        {
            req.log.error("%s parameter validation failure:", req.route.path, err.message);
            res.contentType = "text/plain"; // Per Dropbox API - 400 details in text/plain
            return next(res.send(400, err.message));
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

                putAccountObject(req.user, "/jobs/" + jobId, response, function(err)
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
                async.eachLimit(apiArgs.entries, maxConcurrency, function(entry, callback)
                {
                    // Validate entry.path
                    //
                    validateBulkOperation(req.user, entry.path, null, operation, null, null, function(err, itemCount, srcEntry)
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

                        putAccountObject(req.user, "/jobs/" + jobId, status, function(err)
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

                doBulkOperation(req, operation, workItems, function(err)
                {
                    if (err)
                    {
                        // Write validation err to job file
                        //
                        req.log.error("Error in bulk operation for batch %s:", operation, err);
                        var status = dbErr.getDropboxError(err);
                        status[".tag"] = "failed";

                        putAccountObject(req.user, "/jobs/" + jobId, status, function(err)
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

                putAccountObject(req.user, "/jobs/" + jobId, status, function(err)
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

    // This implementation handles all the xxxxx_batch/check routes (they all do exactly the same thing)
    //
    function filesJobBatchCheck(req, res, next)
    {
        var apiArgs = getApiArgs(req, true);

        try
        {
            assert.string(apiArgs.async_job_id, "async_job_id");
        }
        catch (err)
        {
            req.log.error("%s parameter validation failure:", req.route.path, err.message);
            res.contentType = "text/plain"; // Per Dropbox API - 400 details in text/plain
            return next(res.send(400, err.message));
        }

        getAccountObject(req.user, "/jobs/" + apiArgs.async_job_id, function(err, object)
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
                    deleteAccountObject(req.user, "/jobs/" + apiArgs.async_job_id, function (err)
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
        var apiArgs = getApiArgs(req, true);

        try
        {
            assert.string(apiArgs.path, "path");
        }
        catch (err)
        {
            req.log.error("%s parameter validation failure:", req.route.path, err.message);
            res.contentType = "text/plain"; // Per Dropbox API - 400 details in text/plain
            return next(res.send(400, err.message));
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
        var apiArgs = getApiArgs(req, true);

        try
        {
            assert.string(apiArgs.path, "path");
            assert.string(apiArgs.query, "query");
        }
        catch (err)
        {
            req.log.error("%s parameter validation failure:", req.route.path, err.message);
            res.contentType = "text/plain"; // Per Dropbox API - 400 details in text/plain
            return next(res.send(400, err.message));
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
                matches.splice(lodash.sortedIndexBy(matches, match, function(o){ return getEntrySortKey(o.metadata); }), 0, match);

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

    server.post('/2/files/copy_batch', restify.bodyParser(), filesMoveCopyBatch);
    server.post('/2/files/move_batch', restify.bodyParser(), filesMoveCopyBatch);
    server.post('/2/files/delete_batch', restify.bodyParser(), filesDeleteBatch);

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

    // Batch operations

    server.post('/2/files/upload_session/finish_batch', notImplemented);

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
