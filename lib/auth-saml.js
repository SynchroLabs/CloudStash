var passport = require('passport');
var SamlStrategy = require('passport-saml').Strategy;

var lastProfile;

function verify(req, samlProfile, callback)
{
    req.log.info("Validated user:", samlProfile);

    /*
    { 
        issuer: 'https://app.onelogin.com/saml/metadata/709631',
        sessionIndex: '_b8b527e0-8a98-0135-1e19-06dabf4aaf98',
        nameID: 'bob@synchro.io',
        nameIDFormat: 'urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress',
        nameQualifier: undefined,
        spNameQualifier: undefined,
        PersonImmutableID: 'bob',
        'User.LastName': 'Dickinson',
        'User.FirstName': 'Robert',
        'User.email': 'bob@synchro.io',
        memberOf: undefined,
        getAssertionXml: [Function]
    }
    */

    // http://passportjs.org/docs/profile
    //
    var profile = 
    {
        provider: "saml", 
        id: samlProfile.PersonImmutableID,
        displayName: samlProfile['User.FirstName'] + ' ' + samlProfile['User.LastName'],
        name: {
            givenName: samlProfile['User.FirstName'],
            familyName: samlProfile['User.LastName']
        },
        emails: [ { value: samlProfile['User.email'] } ]
    }

    lastProfile = profile;
    callback(null, profile);
}

exports.addStrategyMiddleware = function(callbackUrl, entryPoint, issuer)
{
    var options =
    {
        callbackUrl: callbackUrl,
        entryPoint: entryPoint,
        issuer: issuer,
        passReqToCallback: true
    }

    passport.use(new SamlStrategy(options, verify));
}

exports.getAccount = function(accountId, cb)
{
    // !!!
    cb(null, lastProfile);
}
