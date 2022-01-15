"""

This class is intended to be used to take a given mqtt message and formats the conetnts
for the required mqtt stream.

It handles all the issues associated with the receiving and transmitting file structure

Basic functions
- get data
- transform data

"""

import paho.mqtt.client as mqtt

import logging
import base64
import json
import datetime




class mqttTransformer():

    def __init__(self):
        # do someting here
        self.log = logging.getLogger("TRANSFORMER")

        self.rawMsgText = ""
        self.decodedPayload = ""
        self.decodedMsgTime = 0
        self.messageStatus = False
        self.incomingMessage = ""
        self.rawMessage = {}

        return
    
    def getData(self,message):
        # Get the message to decode
        self.incomingMessage = message
        self.log.debug("Incoming message received: %s", self.incomingMessage)
        return

    def transform(self,format):
        # Validate & Transform the message
        # valid values for format
        #   TTN
        #   AWS
        # Return the status of the message transformation

        self.log.debug("transform called with format %s", format)

        # Validate & extract Message
        # Check the json object contains device_id, received_at, uplink_message with frm_payload
        if (format == "TTN"):
            self.messageJson = json.loads(self.incomingMessage)

            try:
                self.deviceId = self.messageJson["end_device_ids"]["device_id"]
                self.decodedMsgTime = self.messageJson["received_at"]
                self.rawMsgText = self.messageJson["uplink_message"]["frm_payload"]
                self.decodedPayload = self.messageJson["uplink_message"]["decoded_payload"]
                self.messageStatus = True
            except:
                self.deviceId = ""
                self.decodedMsgTime = 0
                self.rawMsgText = ""
                self.decodedPayload = ""
                self.messageStatus = False
                self.log.debug("Failed to extract required elements from JSON object")

        elif (format == "AWS"):
            print("Not Supported")
            self.messageStatus = False

        else:
            self.messageStatus = False
        
        self.log.debug("Extracted deviceId: %s", self.deviceId)
        self.log.debug("Extracted receivedTime: %s", self.decodedMsgTime)
        self.log.debug("Extracted raw payload: %s", self.rawMsgText)
        self.log.debug("Extracted decoded payload: %s", self.decodedPayload)
        #Create a JSON object with all the consituent parts in it, plus individual bits as required


        return self.messageStatus

    def rawMessageText(self):
        # Return the decoded message
        return self.rawMsgText

    def decodedMessageText(self):
        # Return the decoded message
        return self.decodedPayload

    def decodedMessageTime(self):
        # Return the time of the decoded message
        return self.decodedMsgTime
    
    def decodedDeviceId(self):
        # Return the Device id
        return self.deviceId

    def decodedJsonMessage(self):
        # Return the message in JSON format
        # JSON object key values: 'ts' for timestamp -  milliseconds unix timestamp and a sub object of 'values'
        # {"ts":1451649600512, "values":{"key1":"value1", "key2":"value2"}}
        
        #ToDo Build JSON object to return
        self.rawMessage = {}
        self.rawMessage.update({"device_id": self.deviceId})
        # Convert Time - "received_at":"2021-12-27T16:45:31.225072707Z" to Unix Tiomestamp in mS
        msgTime = datetime.datetime.strptime(self.decodedMsgTime[:-4], '%Y-%m-%dT%H:%M:%S.%f')
        self.rawMessage.update({"ts":msgTime.timestamp()*1000})
        # Add the already decoded values
        self.rawMessage.update({"values":self.decodedPayload})


        self.jsonMessage = json.dumps(self.rawMessage)
        return self.jsonMessage


"""
Message Received
{"end_device_ids":
{"device_id":"eui-70b3d57ed004a82e",
        "application_ids":{"application_id":"mbhome-tech-test-01"},
                "dev_eui":"70B3D57ED004A82E",
                "join_eui":"0000000000000000"},
        "correlation_ids":["as:up:01FQYCGS6QCF6XZ95QAP6DHJBK",
            "rpc:/ttn.lorawan.v3.AppAs/SimulateUplink:89d08133-8d89-4276-bdb3-21ada6ed5e53"],
        "received_at":"2021-12-27T16:45:31.225072707Z",
        "uplink_message":{
            "f_port":1,
            "frm_payload":"NDU2Nzg5QA==",
            "rx_metadata":[{"gateway_ids":{"gateway_id":"test"},"rssi":42,"channel_rssi":42,"snr":4.2}],"settings":{"data_rate":{"lora":{"bandwidth":125000,"spreading_factor":7}}}},"simulated":true}
"""

if __name__ == "__main__":

    # Test routine for the JSON extraction
    gbl_log = logging.getLogger("TRANSFORMER")
    
    transformer = mqttTransformer()
    gbl_log.info("Started the mqttTransformer")

    transformer.getData(""" {"end_device_ids":{"device_id":"eui-a8610a3335336007","application_ids":{"application_id":"hm-weather-01"},
    "dev_eui":"A8610A3335336007","join_eui":"70B3D57ED002EAB7","dev_addr":"260B20DA"},"correlation_ids":["as:up:01FRAZ39Q6S7BDMDR7YFN4BDFQ",
    "gs:conn:01FQCAJZC60WQYQXH9HPGNKE3E","gs:up:host:01FQCAJZD5GKSHR8ZPHJQ2XC4D","gs:uplink:01FRAZ39G9DSVRA94KB5BBQFV8",
    "ns:uplink:01FRAZ39GDMT41A7E7C3HGK0WS","rpc:/ttn.lorawan.v3.GsNs/HandleUplink:01FRAZ39GD4951ZTMG46PYKNMS",
    "rpc:/ttn.lorawan.v3.NsAs/HandleUplink:01FRAZ39Q5PWBHHVH6S9ZZ6K86"],"received_at":"2022-01-01T14:01:05.512249358Z",
    "uplink_message":{"session_key_id":"AX4VuLk8fmxUQwl3eXjF8g==","f_port":2,"f_cnt":114,"frm_payload":"AABODDgYXAFHA+0DAAAAAAAsAL8=",
    "decoded_payload":{"AirPressure":0,"Battery":11264,"BatteryChargeStatus":2,"BatteryFaults":0,"BatteryPowerStatus":1,"Direction":315,
    "Humidity":83.9,"Rainfall":0,"SoilTemp":0,"Speed":62,"Status":191,"Sunshine":0,"Temperature":14.799999999999997},
    "rx_metadata":[{"gateway_ids":{"gateway_id":"gwid-ukhm20210002","eui":"647FDAFFFE0099FD"},"timestamp":98868291,
    "rssi":-57,"channel_rssi":-57,"snr":11.2,"uplink_token":"Ch8KHQoRZ3dpZC11a2htMjAyMTAwMDISCGR/2v/+AJn9EMO4ki8aDAihvcGOBhDZu/OHASC4y4mo8LLqAQ==",
    "channel_index":2}],"settings":{"data_rate":{"lora":{"bandwidth":125000,"spreading_factor":7}},"coding_rate":"4/5",
    "frequency":"868500000","timestamp":98868291},"received_at":"2022-01-01T14:01:05.293789172Z","confirmed":true,
    "consumed_airtime":"0.077056s","version_ids":{"brand_id":"arduino","model_id":"mkr-wan-1310","hardware_version":"1.0",
    "firmware_version":"1.2.0","band_id":"EU_863_870"},"network_ids":{"net_id":"000013","tenant_id":"ttn","cluster_id":"ttn-eu1"}}}""")

    transformer.transform("TTN")


    transformer.getData("""{"end_device_ids":{"device_id":"eui-a8610a3335336007","application_ids":{"application_id":"hm-weather-01"},
    "dev_eui":"A8610A3335336007","join_eui":"70B3D57ED002EAB7","dev_addr":"260B20DA"},"correlation_ids":["as:up:01FRAYZB454E612WCWTXZ502NQ",
    "gs:conn:01FQCAJZC60WQYQXH9HPGNKE3E","gs:up:host:01FQCAJZD5GKSHR8ZPHJQ2XC4D","gs:uplink:01FRAYZAX4HWS8N1R6FXKWA82D",
    "ns:uplink:01FRAYZAXAG5TSQ0HSV6ZFX4WS","rpc:/ttn.lorawan.v3.GsNs/HandleUplink:01FRAYZAXANQT4JQPMRXQTQRFP",
    "rpc:/ttn.lorawan.v3.NsAs/HandleUplink:01FRAYZB3YFJ5KWY8X8H1S89J1"],"received_at":"2022-01-01T13:58:55.881553157Z",
    "uplink_message":{"session_key_id":"AX4VuLk8fmxUQwl3eXjF8g==","f_port":2,"f_cnt":110,"frm_payload":"AAAAAOwTXQE+A+0DAAAAAAAsAL8=",
    "decoded_payload":{"AirPressure":0,"Battery":11264,"BatteryChargeStatus":2,"BatteryFaults":0,"BatteryPowerStatus":1,"Direction":0,
    "Humidity":83,"Rainfall":0,"SoilTemp":0,"Speed":51,"Status":191,"Sunshine":0,"Temperature":14.899999999999999},
    "rx_metadata":[{"gateway_ids":{"gateway_id":"gwid-ukhm20210002","eui":"647FDAFFFE0099FD"},"timestamp":4264215867,"rssi":-56,
    "channel_rssi":-56,"snr":9.8,"uplink_token":"Ch8KHQoRZ3dpZC11a2htMjAyMTAwMDISCGR/2v/+AJn9ELuKq/EPGgwIn7zBjgYQoPfutgIg+NzJuI2v6gE=",
    "channel_index":3}],"settings":{"data_rate":{"lora":{"bandwidth":125000,"spreading_factor":7}},"coding_rate":"4/5","frequency":"867100000",
    "timestamp":4264215867},"received_at":"2022-01-01T13:58:55.658369078Z","confirmed":true,"consumed_airtime":"0.071936s",
    "version_ids":{"brand_id":"arduino","model_id":"mkr-wan-1310","hardware_version":"1.0","firmware_version":"1.2.0","band_id":"EU_863_870"},
    "network_ids":{"net_id":"000013","tenant_id":"ttn","cluster_id":"ttn-eu1"}}}""")

    transformer.transform("TTN")

    transformer.decodedJsonMessage()
