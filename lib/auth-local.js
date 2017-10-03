var passport = require('passport');
var LocalStrategy = require('passport-local').Strategy;

var bcrypt = require('bcryptjs');

// A valid hash for 'password' is '$2a$10$6rfh7kA8K8XwjLz1U/fZU.ZFit7xFqmHbEnh3skB3I/yxHFaATFzG'
//
var users = 
[
    { id: "1234-BEEF", givenName: "Test", familyName: "User", email: "user@synchro.io", passwordHash: "$2a$10$6rfh7kA8K8XwjLz1U/fZU.ZFit7xFqmHbEnh3skB3I/yxHFaATFzG" } 
]

function generatePasswordHash(password, callback)
{
    // callback(err, hash)
    //
    bcrypt.hash(password, 10, callback);
}

function comparePasswordToHash(password, hash, callback)
{
    // callback(err, result) - where result is boolean representing match verification state
    //
    bcrypt.compare(password, hash, callback);
}

function profileFromUser(user)
{
    // http://passportjs.org/docs/profile
    //
    var profile = 
    {
        provider: "local", 
        id: user.id,
        displayName: user.givenName + ' ' + user.familyName,
        name: {
            givenName: user.givenName,
            familyName: user.familyName
        },
        emails: [ { value: user.email } ]
    }

    return profile;
}

function findUser(field, value, cb) 
{
    for (var i = 0; i < users.length; i++) 
    {
        if (users[i][field] === value)
        {
            return cb(null, users[i]);
        }
    };

    return cb(null, null);
}

function verify(req, username, password, cb)
{
    // !!! If we want to validate that the user has authorized the specific app that is attempting to
    //     auth as the user, we can get the app ID from req.params.client_id
    //

    findUser("email", username, function(err, user) 
    {
        if (err)
        {
            return cb(err);
        }

        if (!user)
        {
            return cb(null, false); // User not found
        }

        comparePasswordToHash(password, user.passwordHash, function(err, result)
        {
            if (err)
            {
                return cb(err);
            }
            else if (!result)
            {
                return cb(null, false); // Password mismatch
            }
            else
            {
                var userInfo = profileFromUser(user);
                req.log.info("Validated user:", userInfo);
                return cb(null, userInfo);
            }
        })
    });
}

exports.addStrategyMiddleware = function()
{
    var options =
    {
        usernameField: "email",
        passReqToCallback: true
    }

    passport.use(new LocalStrategy(options, verify));
}

exports.getAccount = function(accountId, cb)
{
    findUser("id", accountId, function(err, user) 
    {
        if (err)
        {
            return cb(err);
        }
        else if (!user)
        {
            return cb(null, false);
        }
        else
        {
            var userInfo = profileFromUser(user);
            return cb(null, userInfo)
        }
    });
}
