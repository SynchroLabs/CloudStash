# Latest Node.js 8.x LTS
FROM node:alpine

# NOTE: Set any CloudStash env variables here
#
ENV CLOUDSTASH__PORT 443

# --- You shouldn't need to touch anything below this ---

# Create app directory
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# Bundle app source
COPY . /usr/src/app

# Install deps (/node_modules not copied above due to setting in .dockerignore)
RUN npm install

# Expose the CLOUDSTASH__PORT set above
EXPOSE $CLOUDSTASH__PORT 

# Becaue of some issues with orderly shutdown using "npm start" we are using "node app.js"
CMD [ "node", "app.js" ]
