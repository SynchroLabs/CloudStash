var restify = require('restify');

var fs = require('fs');

var assert = require('assert-plus');
var handlebars = require('handlebars');

var account = require('./account');

// Passport auth (via local provider)
//
var passport = require('passport');

exports.addAuthMiddleware = function(server, strategy, tokenManager)
{
    // Auth endpoint (not from Dropbox API) - Provide the client_id, email, password, get a bearer token
    // back without any of the OAuth shenanigans (for testing purposes only).
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

        passport.authenticate('local', function(err, user, info) 
        {
            if (err) 
            { 
                return next(err); 
            }
            else if (!user) 
            { 
                res.send(403, "Authentication failed");
            }
            else
            {
                // Authenticated user
                //
                var token = tokenManager.createAuthToken(req.params.client_id, user.id);
                res.send(token);
            }
        })(req, res, next);
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
        passport.authenticate('local', function(err, user, info) 
        {
            if (err) 
            { 
                return next(err); 
            }
            else if (!user) 
            { 
                req.params.error = "Login failed";
                var body = authorizeTmpl(req.params);
                writeResponseBody(res, body);
                next();
            }
            else
            {
                // Authenticated user
                //
                req.log.info("Login succeeded");

                // !!! There is a potential security issue here, as the client app never provides its
                //     secret (AFAICT) with this mode of authorization.  Investigate.
                //
                var token = tokenManager.createAuthToken(req.params.client_id, user.id);

                // redirects to db-[app_id]://1/connect? 
                //   oauth_token=oauth2:
                //   oauth_token_secret=[oauth token]
                //   uid=[account_id]
                //   state=[passed-in state]

                var redirect = "db-" + req.params.client_id + "://1/connect";

                // oauth_token, oauth_token_secret, uid
                //
                redirect += "?oauth_token=oauth2:&oauth_token_secret=" + token + "&uid=" + user.id;

                if (req.params.state)
                {
                    redirect += "&state=" + req.params.state;
                }

                req.log.info("Redirecting to:", redirect);

                res.redirect(redirect, next);
            }
        })(req, res, next);
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

        if (strategy === 'local')
        {
            var body = authorizeTmpl(req.params);
            writeResponseBody(res, body);
        }
        else if (strategy === 'saml')
        {
            req.query.RelayState = JSON.stringify(
            {
                response_type: req.params.response_type,
                client_id: req.params.client_id,
                redirect_uri: req.params.redirect_uri
            });
            passport.authenticate('saml')(req, res, next);
        }

        next();
    });

    server.post('/oauth2/authorize', restify.bodyParser({ mapParams: false }), function(req, res, next)
    {
        var oauthState = req.params;
        if (req.body.RelayState) // For SAML
        {
            oauthState = JSON.parse(req.body.RelayState); 
        }

        // When login submitted from auth form (GET request above), it is posted here.
        //
        req.log.info("oauth2/authorize (POST) - form posted for email: %s, state: %s", oauthState.email, oauthState.state);

        // Verify client_id, user/pass, and redirect as appropriate (based on response_type)
        //
        passport.authenticate(strategy, function(err, user, info) 
        {
            if (err) 
            { 
                req.log.error("Login error", err);
                return next(err); 
            }
            else if (!user) 
            { 
                req.log.info("Login failed");
                req.params.error = "Login failed";
                var body = authorizeTmpl(req.params);
                writeResponseBody(res, body);
                next();
            }
            else
            {
                // Authenticated user
                //
                req.log.info("Login succeeded");

                // !!! In case some smartass changed this from the original request, re-verify it here:
                //    
                //     account.validateRedirect(req.params.client_id, req.params.redirect_uri);

                var redirect = oauthState.redirect_uri;

                if (oauthState.response_type === 'code')
                {
                    // JWT { type: "access_code", client_id, account_id, timestamp }
                    //
                    var code = tokenManager.createAuthCode(oauthState.client_id, user.id);

                    // code, state
                    //
                    redirect += "?code=" + code; 
                }
                else // token
                {
                    // !!! There is a potential security issue here, as the client app never provides its
                    //     secret (AFAICT) with this mode of authorization.  Investigate.
                    //
                    var token = tokenManager.createAuthToken(oauthState.client_id, user.id);

                    // access_token, token_type (bearer), account_id, state
                    //
                    redirect += "?access_token=" + token + "&token_type=bearer&account_id=" + user.id;
                }

                if (req.params.state)
                {
                    redirect += "&state=" + req.params.state;
                }

                req.log.info("Redirecting to:", redirect);

                res.redirect(redirect, next);
            }
        })(req, res, next);
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
        var accessCode = tokenManager.getAuthCode(req.params.code);
        if (accessCode.app_id === client_id)
        {
            if (account.validateApp(client_id, client_secret))
            {
                // Winner!  Return bearer token (and account_id).
                //
                var token = tokenManager.createAuthToken(accessCode.app_id, accessCode.account_id);
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

    return ['/login', '/oauth2/authorize', '/oauth2/token', '/1/connect', '/2/files/list_folder/longpoll'];
}
