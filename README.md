# CloudStash

[![Build Status](https://travis-ci.org/SynchroLabs/CloudStash.svg?branch=master)](https://travis-ci.org/SynchroLabs/CloudStash)
[![Coverage Status](https://coveralls.io/repos/github/SynchroLabs/CloudStash/badge.svg?branch=master)](https://coveralls.io/github/SynchroLabs/CloudStash?branch=master)

End-user cloud file storage API backed by cloud storage from any provider

Presents the Dropbox v2 HTTP API

https://www.dropbox.com/developers/documentation/http/documentation

## Getting Started

To run:

    npm start

To run on a custom port:

    node app.js -p 1337 | ./node_modules/.bin/bunyan 

To run Mocha unit tests with coverage:

    npm test

## Storage Configuration

In your config.json, add a `driver` key with values as shown below for your desired storage provider:

### Local file storage

    "driver": 
    { 
        "provider": "file", 
        "basePath": "your/local/dir" 
    }

### Amazon S3 storage

    "driver":
    {
        "provider": "aws",
        "user": "your_aws_user",
        "accessKeyId": "your_aws_access_key_id",
        "secretAccessKey": "your_aws_secret_key",
        "bucket": "your_aws_bucket"
    }

### Azure storage

    "driver":
    {
        "provider": "azure",
        "accountName": "your_account_name", 
        "accountKey": "your_account_key",
        "container": "your_azure_container"
    }

### Joyent Manta storage

    "driver":
    {
        "provider": "manta", 
        "keyId": "your_key_id",
        "url": "https://us-east.manta.joyent.com",
        "user": "your_manta_user",
        "key": "-----BEGIN RSA PRIVATE KEY-----\nxxxxxxxxxxxxx\n-----END RSA PRIVATE KEY-----",
        "basePath": "/your/manta/path"
    }

## SSL Configuration

For SSL, add the following keys to your config.json:

    "PORT": 443,
    "SSL_CERT_PATH": "ssl.crt",
    "SSL_KEY_PATH": "ssl.key"

## Authorization Configuration

### Local user/pass

For local user/pass authentication, no configuration is required.

### SAML

For SAML authorization, set the following keys in your config.json (example provider OneLogin shown):

    "AUTH_STRATEGY": "saml",
    "SAML_CALLBACK": "http://yourhost/oauth2/authorize",
    "SAML_ENTRYPOINT": "https://your_subdomain.onelogin.com/trust/saml2/http-post/sso/your_app_id",
    "SAML_ISSUER": "https://app.onelogin.com/saml/metadata/your_app_id"
