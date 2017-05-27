var restify = require('restify');

var jwt = require('jsonwebtoken');
var restifyJwt = require('restify-jwt');

module.exports = function(_jwtSecret)
{
    var logger = require('bunyan').createLogger({name: "MantaBoxServer"});

    var server = restify.createServer({ name: 'MantaBox'});
    server.use(restify.bodyParser());

    // The '/login' endpoint generates and returns a JWT token.  All endpoints except '/login' will 
    // require a JWT token (via Authorization header), and those endpoints can access the token payload
    // via req.user.  
    //
    server.use(restifyJwt({ secret: _jwtSecret}).unless({path: ['/login']}));

    server.post('/login', function(req, res, next)
    {
        if (!req.body.username) {
            res.send(400, "Username required");
        }
        if (!req.body.password) {
            res.send(400, "Username required");
        }

        // !!! Check password

        var token = jwt.sign({ username: req.body.username, userid: 1234 }, _jwtSecret);
        res.send(token);
        next();
    });

    server.get('/echo/:message', function(req, res, next)
    {
        logger.info("Request from user: %s, userid: %d", req.user.username, req.user.userid);
        res.send("You said: " + req.params.message);
        next();
    });

    return server;
}
