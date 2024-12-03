#! /bin/bash

docker run --rm -p 6789:6789 -v ${PWD}:/app --env-file .env top-streamers-mage