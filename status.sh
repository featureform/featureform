#!/bin/sh
while true; do
    supervisorctl status
    sleep 5 # sleep for 60 seconds
done