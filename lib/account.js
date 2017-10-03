// User account info
//
// In production, user account management and auth will be delegated to an external system.  This module is intended
// to be a lightweight auth system suitable for prototyping and testing only.
//

// For apps, we need to store:
//
// * client_id (unique, index/key) - (In Dropbox - 15 hex digits)
// * client_secret (In Dropbox - 15 hex digits)
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
