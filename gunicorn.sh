#!/bin/sh
gunicorn search:application -w 4 -b 0.0.0.0:5000
