#!/bin/bash

# Source the virtual environment
source /home/ec2-user/mpcs-cc/bin/activate

# Change to the annotator directory
cd /home/ec2-user/mpcs-cc/gas/ann

# Run the annotator with unbuffered output for logging
python -u annotator.py