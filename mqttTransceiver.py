"""
Contains the class that holds the methods to receive the various mqtt messages

It is antiicpated that this class will be called once per receiving stream

Basic functions
- Conection
- Disconnect
- Reconect
- Receive
- Data Ready

"""



import paho.mqtt.client as mqtt

import logging


log = logging.getLogger(__name__)

class mqttTransceiver():

    def __init__(self, instance, client, host, port = 1883, keepalive=60):
        """
        Class object for the reception of MQTT messages
        client = the name to use to connect to the host
        host = MQTT server address
        port = the port on the host to connect to (default 1883)
        keepalive = the maximum period allowed for communications with the broker (default=60S)
        """
        self.log = logging.getLogger(instance)
        self.log.info("Logging Instance:%s", self.log)
        
        self.instance = instance
        self.client_id = client
        self.host_id = host
        self.host_port = port
        self.host_keepalive = keepalive

        self.connected = False       # Used to track if the client is connected or not
        self.topics = {}                # Dictionary of all the topics and qos values required
        self.messageReceived = False
        self.connectionError = -100           #Holds the connection response code

        self.mqtt = mqtt.Client(self.client_id)
        self.log.info("Client connection with response %s", self.mqtt)
        #ToDo add in required on_ messages.
        #ToDo make the naming convention better and more standard.
        self.mqtt.on_connect = self.onConnectCallback
        self.mqtt.on_message = self.onReceive
        #on subscribe
        #on publish
        #

        return
    
    def connect(self, username, password):
        # do something here
        response = -1

        self.mqtt.username_pw_set(username, password)
        #ToDo Put a try block around this
        response = self.mqtt.connect(self.host_id, self.host_port, self.host_keepalive)
        self.log.debug("Started the Client connect call, response:%s", response)
        return

    def onConnectCallback(self,client, userdata, flags, rc):
        # Activated when received a callback from connection
        self.log.debug("OnConnectCallback responded with code %d", rc)

        #ToDo Handle different response codes from TTN
        """0: Connection successful 1: Connection refused - incorrect protocol version 2: Connection refused - invalid client identifier 
        3: Connection refused - server unavailable 4: Connection refused - bad username or password 5: Connection refused - not authorised 
        6-255: Currently unused."""
        #ToDo Handle different responses from ThingsBoard
        """0x00 Connected - Successfully connected to ThingsBoard MQTT server.
        0x04 Connection Refused, bad username or password - Username is empty.
        0x05 Connection Refused, not authorized - Username contains invalid $ACCESS_TOKEN."""
        self.connectionError = rc
        if (rc == 0):
            self.connected = True
        else:
            self.connected = False
        return

    def reconnect(self):
        # Attempt to reconnect to the mqtt broker
        # Responds with a onConnect callback
        self.mqtt.reconnect()
        self.log.info("Reconnection request made")
        return

    def disconnect(self):
        # Diosconnect from the mqtt server
        self.mqtt.disconnect()
        self.log.info("Disconnection request made")
        return

    def connectionStatus(self):
        # Return the status of the connection
        return self.connected

    def connectionErrorCode(self):
        # Return the error code from connection
        return self.connectionError

    def startProcessingForever(self):

        self.mqtt.loop_forever()
        self.log.debug("Looping Forever as a blocking thread")
        return

    def startLooping(self):
        #start the background looping
        self.mqtt.loop_start()
        self.log.debug("Started Looping of background thread")
        return

    def stopLooping(self):
        # Stop the background looping
        self.mqtt.loop_stop()
        self.log.debug("Stopped Looping of background thread")
        return

    def disconnect(self):
        #ToDo do something here

        # Will generate a on_disconnect callback

        return

    def reconnect(self):
        #do something here
        self.mqtt.reconnect()
        self.log.info("Reconection attempt started")
        return

    def subscription(self, topic, qos=0):
        #Subscribe to a topic.
        #can be called multiple times for multiple topics
        self.topics[topic] = qos                # Adds the topic if new, updates the qos value if not

        self.mqtt.subscribe([(topic, qos)])
        self.log.info("Updated subscription list, Topics subscribed to: %s", self.topics)
        return

    def unsubscribe(self,topic):
        #remove a topic from subscription

        if topic in self.topics:
            # Remove the topic
            self.topics.pop(topic)

            self.mqtt.unsubscribe(topic)
        else:
            self.log.debug("Tried to remove '%s', but not found so no action taken", topic)

        self.log.info("Updated subscription list, Topics subscribed to: %s", self.topics)
        return

    def unsubscribeAll(self):
        #remove all subscriptions

        return

    def on_subscribe(self):
        #call back as to when subscribed

        return

    def on_unsubscribe(self):
        #called when a unsubscribe is called

        return

    def onReceive(self, client, userdata, message):
        #This method is called when a a message is received via MQTT
        # It is not expected to be called externally.

        self.messageReceived = True

        self.receivedMessage = message.payload.decode("utf-8")

        self.log.debug("Message received = %s" ,str(message.payload.decode("utf-8")))
        self.log.debug("Message topic = %s",message.topic)
        self.log.debug("Message qos = %s",message.qos)
        self.log.debug("Message retain flag = %s",message.retain)

        return

    def dataReady(self):
        # This can be used to indicate when data is ready to be processed

        return self.messageReceived

    def getReceivedMessage(self):
        # Gives the message back to the calling class
        return self.receivedMessage

    def dataProcessed(self):
        # Used to respond back to confirm that the data received has been processed
        self.messageReceived = False
        return

    def publish(self, topic, message):
        # Send some data to the broker
        #ToDo Returns an object that include .rc that indicates errors
        self.mqtt.publish(topic, message)
        self.published = False
        self.log.debug("Message Published: %s", message)
        self.log.debug("Message Queue published to:%s", topic)
        return
    
    def onPublishCallback(self,client, userdata, mid):
        # Handle the message published
        self.published = True
        self.log.debug("On Publish Callback Received now")

        return

    def publishStatus(self):
        #Returns the current publication status
        return self.published
