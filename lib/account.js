// User account info
//
// In production, user account management and auth will be delegated to an external system.  This module is intended
// to be a lightweight auth system suitable for prototyping and testing only.
//
// ACCOUNT
//
// Store:
//
// account_id (unique) - GUID (in Dropbox it's 40 chars, regular GUID is 36 with dashes, 32 without)
// email (unique)
// password_hash - Generated using bcrypt - use bcrypt to compare - https://www.npmjs.com/package/bcryptjs
// given_name
// surname
// email_verified (bool)
// disabled (bool)
//
// Report via Dropbox API:
//
// account_id
// name {
//   given_name
//   surname
//   familiar_name (generage from given_name)
//   display_name (generate from given_name + surname)
//   abbreviated_name (generate from initials)
// }
// email
// email_verified (bool)
// disabled (bool)
//
// ----
//
// Create Account - email + password + given_name + surname
//
// Authenticate - email + password
//
// ----
//
// Authenticate on behalf of user as application
//
// email + password + application_id (00001 is "User file storage" / default?)
//
// ----
//
/*
var bcrypt = require('bcryptjs');

bcrypt.hash("xxxxxx", 10, function(err, hash)
{
});

bcrypt.compare("xxxxxx", hash, function(err, res) 
{
    if (res)
    {
        // win
    }
    else
    {
        // lose
    }
});
*/

// For apps, we need to store:
//
// * client_id (unique, index/key)
// * client_secret
// * app_name
// publisher
// description
// website
// icon_sm
// icon_lg
// valid_redirect_uris
// allow_implicit_grant (boolean, def true)
//
exports.validateApp = function(app_id, app_secret)
{
    if ((app_id === '000001') && (app_secret === 'one'))
    {
        return true; // Return more client app details?
    }
    else
    {
        return null;
    }
}

exports.validateRedirect = function(app_id, redirect)
{
    // !!!
    return true;
}

exports.validateLogin = function(app_id, email, password)
{
    // !!! This assumes that the user has approved access by the app (we need to ask and track that at some point)
    //
    if ((email === 'user@synchro.io') && (password === 'password'))
    {
        return {
            app_id: app_id,
            account_id: "1234-BEEF",
            email: email 
        }
    }
    else
    {
        return null;
    }
}
