#!/usr/bin/env python3
#
# main application code
#
#

#This one may not be needed if this work is done in the classes??
from threading import Timer
import paho.mqtt.client as mqtt

import time
import logging
import logging.config
import dictLogging

# Removed as no longer used
#import systemSettings as SS


import mqttTransceiver
import mqttTransformer
from systemSettings import variablesReceiver as varRec
from systemSettings import variablesSender as varSend


restartWaitTime = 2                 #Used to delay the restart of the main function


def SetupLogging():
    """
    Setup the logging defaults
    Using the logger function to span multiple files.
    """
    global gbl_log
    # Create a logger with the name of the function
    logging.config.dictConfig(dictLogging.log_cfg)
    gbl_log = logging.getLogger("MAIN")

    gbl_log.info("\n\n")
    gbl_log.info("Logging Started, current level is %s" % gbl_log.getEffectiveLevel())

    return

def checkConnected(mqttChannel,first=False):
    # Check rthe status of the receiver and attempt to reconnect if required
    # On first attempt, wait before reconnecting, else try reconnet
    # 
    starttime = time.time()

    gbl_log.info("Checking for connection")

    # If I am connecting for the first time, set a wait time before attmepting to reconnect
    if(first):
        #This is the first time I have connected
        waittime = time.time()

    while (not mqttChannel.connectionStatus()):
        if ((time.time() - waittime) > varRec.waitTime):
            # Exceeded waittime
            gbl_log.debug("Exceeded waittime, attempting reconnect")
            mqttChannel.dis
            mqttChannel.reconnect()
            waittime = time.time()
        elif ((starttime + varRec.retry) > time.time()):
            # time delay has passed, try reconnecting
            gbl_log.debug("Retry time exceeded, trying reconnect")
            mqttChannel.reconnect()
        elif ((starttime + varRec.timeout) < time.time()):
            # Reached timeout, therefore abondon attempt
            gbl_log.info("Failed to get a positive conection status, exitting wait loop")
            break

        time.sleep(0.1)
    
    gbl_log.info("Final check Connected status:%s", mqttChannel.connectionStatus())
    return mqttChannel.connectionStatus()

def main():
    """
    Main function / orchestration of code
    """

    endtime = 0.0

    # Configure Receiver
    receiver = mqttTransceiver.mqttTransceiver(varRec.instance, varRec.clientId, varRec.hostURL)
    gbl_log.info("Started the mqtt Transceiver as Receiver")

    receiver.connect(varRec.username, varRec.apiKey)

    receiver.startLooping()

    #checkConnected(receiver, True)
    while (not receiver.connectionStatus()):
        gbl_log.info("Error code:%s", receiver.connectionErrorCode())
        time.sleep(0.1)

    #ToDo Add these to the System Settings class
    receiver.subscription("#")
    receiver.subscription('+/+/devices/+/up')
    #ToDo validate that we have got subscription

    # Configure Transmitter
    transmitter = mqttTransceiver.mqttTransceiver(varSend.instance, varSend.clientId, varSend.hostURL)
    gbl_log.info("Started the mqtt Transceiver as Transmitter")

    transmitter.connect(varSend.username, varSend.password)

    transmitter.startLooping()

    #checkConnected(transmitter, True)
    while (not transmitter.connectionStatus()):
        time.sleep(0.1)

    # Configure Transformer
    transformer = mqttTransformer.mqttTransformer()
    gbl_log.info("Started the mqttTransformer")

    #receiver.startLooping()

    #endtime = time.time() + 3000
    while (True): # (time.time() < endtime):

        if receiver.dataReady():
            transformer.getData(receiver.getReceivedMessage())
            transformer.transform("TTN")
            receiver.dataProcessed()

            #Send data
            transmitter.publish(varSend.pubTopic, transformer.decodedJsonMessage())
            
            #ToDo Add in a loop for connection with timeout
            #Bug This is currently not responding.
            while (not transmitter.publishStatus()):
                time.sleep(0.1)
        else:
            time.sleep(0.1)

    #ToDo Feels like this is buring CPU usage, need to consider lowering it, maybe with a sleep or something.
    #       Can loopforever be used in some way to do this?

    receiver.stopLooping()
    gbl_log.info("End of Processing, program exit")

    return

def handleCrash():
    time.sleep(restartWaitTime)  # Restarts the script after 2 seconds
    startScript()


def startScript():
    """
    Routine to run the main funciton, capturing errors and restarting if failed
    """
    gbl_log.info("Starting the startScript")

    while(True):
        try:
            # Use 'run' to fire up the main program
            gbl_log.info("Main TRY loop started")
            main() 
        except KeyboardInterrupt:
            # Keyboard interrupt, so stop
            return
        except Exception as err:
            # Script crashed, lets restart it!
            gbl_log.info("Exception has been triggered:%s", err)
            gbl_log.error("Exception arguments:%s",err.args)
            time.sleep(restartWaitTime)


if __name__ == "__main__":
    SetupLogging()

    startScript()


