#!/bin/sh
gunicorn recherche:application -w 4 -b 0.0.0.0:5000
