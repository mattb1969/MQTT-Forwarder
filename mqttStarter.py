#!/usr/bin/env python3
"""
This script ensure that if the python script fails, it is auto restarter

"""

from subprocess import run
from time import sleep

# The main script to run and restart if the core program fails
mainProgram = "./mqttForwarder.py" 
restartWaitTime = 2

def start_script():
    try:
        # Use 'run' to fire up the main program
        run(mainProgram, check=True) 
    except:
        # Script crashed, lets restart it!
        handle_crash()

def handle_crash():
    sleep(restartWaitTime)  # Restarts the script after 2 seconds
    start_script()

start_script()
