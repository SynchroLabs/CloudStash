var stream = require('stream');

exports.pipeRequest = function(req, stream, cb)
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
