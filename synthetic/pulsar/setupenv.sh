#!/bin/sh
python3 -m virtualenv env
. env/bin/activate
pip install wipac-mqclient wipac-mqclient-pulsar
