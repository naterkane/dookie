#!/usr/bin/env bash
docker run -d --name dookietest -p 27017:27017 mongo:latest
#UP=$(docker inspect -f {{.State.Running}} dookietest)
#until [ $UP == true ]; do
    echo 'waiting for mongo to start...'
    sleep 5;
#    let UP=$(docker inspect -f {{.State.Running}} dookietest)
#done;
node_modules/.bin/mocha ./test/*.test.js --exit 
echo 'stopping container...'
docker rm -f -v dookietest && echo 'done 🤪'
