var jwt = require('jsonwebtoken');

module.exports = function(secret)
{
    var log = require('./logger').getLogger("token");

    // We encode the auth code and token with different secret keys as a way of enforcing type (the Restify
    // JWT auth middleware considers anything that decrypted to be valid, so we need to distinguish between
    // auth tokens and other things we encode using JWT - so you can't try to pass off an auth code as an
    // auth token, for example).
    //
    var _jwtAuthCodeSecret = secret + "authCode";
    var _jwtAuthTokenSecret = secret + "authToken";

    var tokenManager =
    {
        createAuthCode: function(app_id, account_id)
        {
            var code = 
            {
                app_id: app_id,
                account_id: account_id
            }
            
            return jwt.sign(code, _jwtAuthCodeSecret);
        },
        getAuthCode: function(code)
        {
            return jwt.verify(code, _jwtAuthCodeSecret);
        },
        createAuthToken: function(app_id, account_id)
        {
            // !!! Do we have an iat (issued-at-time) provided by the jwt signer?  Should we add one if not?
            //
            var token = 
            {
                app_id: app_id,
                account_id: account_id
            }
            
            return jwt.sign(token, _jwtAuthTokenSecret);
        },
        getAuthToken: function(token)
        {
            return jwt.verify(code, _jwtAuthTokenSecret);
        },
        getAuthTokenSecret: function()
        {
            return _jwtAuthTokenSecret;
        }
    }

    return tokenManager;
}