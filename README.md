# MantaBox

End-user cloud file storage front end for the Joyent Manta object store

Dropbox v2 HTTP API (only semi-RESTful)

https://www.dropbox.com/developers/documentation/http/documentation

Restify 
  restify - http://restify.com/

JWT for stateless auth
  jsonwebtoken - https://www.npmjs.com/package/jsonwebtoken
  restify-jwt - https://www.npmjs.com/package/restify-jwt

Logging
  Bunyan

Test framework helper
  supertest - https://www.npmjs.com/package/supertest

API Specification / Documentation
  Swagger - http://swagger.io/

Interactive testing
  Postman - can import Swagger file

----

To run on custom port and format Bunyan logging output for the console:

    node app.js -p 1337 | ./node_modules/.bin/bunyan 

To run Mocha unit tests

    ./node_modules/.bin/mocha

Maybe: https://www.npmjs.com/package/mocha-pretty-bunyan-nyan