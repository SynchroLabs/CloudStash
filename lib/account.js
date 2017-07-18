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
// JWT contains account_id and application_id
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
        // The return here is going to be used as the "token" (bearer token) for the session. Given that...
        //
        // !!! Should token (JWT) contain all user data per Dropbox API, or should we fetch other stuff
        //     from auth db upon /users/get_account?
        //
        // !!! At very least, we should have an "expires" time (think about auth flow when that happens)
        //
        // !!! Or should we return this (without the "type") and make someone higher up (or in server) responsible
        //     for managing this as "token" data.
        //
        return {
            type: "authenticatedUser",
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
