#!/bin/bash

virtualenv venv
source venv/bin/activate
easy_install boto==2.29.1
easy_install s3cmd
