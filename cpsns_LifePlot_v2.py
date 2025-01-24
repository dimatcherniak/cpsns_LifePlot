"""
This program will
1. read CP-SENS MQTT messages, both data and metadata
2. plot the data
"""
import matplotlib.pyplot as plt
import numpy as np
from paho.mqtt.client import Client as MQTTClient
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.client import MQTTv311
import queue
import struct
import time
import argparse
import json
import sys

HOST_DEFAULT = "dtl-server-2.st.lab.au.dk"
PORT_DEFAULT = 8090
USERNAME_DEFAULT = "hbk1"
PASSWORD_DEFAULT = "hbk1shffd"
MQTT_TOPIC_DEFAULT = "cpsens/+/+/1/acc/raw/+"  # "cpsens/+/+/1/+/+/data"

myDict = {}
bReadingMyDict = False
bWritingMyDict = False
strMQTTTopic = MQTT_TOPIC_DEFAULT


mqttc = MQTTClient(callback_api_version=CallbackAPIVersion.VERSION2, protocol=MQTTv311)


def on_connect(mqttc, userdata, flags, rc, properties=None):
    print("connected with response code %s" % rc)
    mqttc.subscribe(strMQTTTopic)


def on_subscribe(self, mqttc, userdata, msg, granted_qos):
    print("mid/response = " + str(msg) + " / " + str(granted_qos))


def on_message(client, userdata, msg):
    global myDict, bReadingMyDict, bWritingMyDict
    topic = msg.topic
    substrings = topic.split('/')
    bIsMetadata = True
    if substrings[-1] == "data":
        bIsMetadata = False
    elif substrings[-1] == "metadata":
        bIsMetadata = True
    else:
        raise Exception("Unknown topic: " + substrings[-1])

    # Create a tuple made of the topic string without the last element (data/metadata)
    myKey = tuple(substrings[:-1])

    while bReadingMyDict:
        # make the thread sleep
        # print("Waiting for bReadingMyDict")
        time.sleep(0.0001)
    bWritingMyDict = True

    if bIsMetadata:
        # Process JSON metadata
        # Add the key to the dictionary
        if myKey not in myDict:
            # Parse the payload
            json_metadata = json.loads(msg.payload)
            nSamples = json_metadata['Data']['Samples']
            cType = json_metadata['Data']['Type'][0]
            Fs = json_metadata["Analysis chain"][-1]["Sampling"] # take the sampling freq. from the last element of the analysis chain!
            # Need to know: physical quantity, units
            strPhysQuantity = json_metadata["Analysis chain"][-1]["Output"]
            strUnits = json_metadata["Data"]["Unit"]
            # Only for metadata ver.>=2
            try:
                secAtAcqusitionStart = json_metadata["TimeAtAquisitionStart"]["Seconds"]
                nanosec = json_metadata["TimeAtAquisitionStart"]["Nanosec"]
            except Exception as e:
                print("Incompatible version. Use earlier version of cpsns_LifePlot!", file=sys.stderr)
            
            usTimeStamp = 0
            #                0         1      2   3                4         5              6     7                     8
            #myDict[myKey] = [nSamples, cType, Fs, strPhysQuantity, strUnits, queue.Queue(), None, secAtAcqusitionStart, usTimeStamp]
            myDict[myKey] = {
                "SamplesInPayload": nSamples, 
                "DataType": cType, 
                "SampleRate": Fs, 
                "PhysicalQuantity": strPhysQuantity, 
                "Units": strUnits, 
                "PayloadQueue": queue.Queue(), 
                "SecAtAcqusiitionStart": secAtAcqusitionStart, 
                "Nanosec": nanosec,
                "SampleWhenEntryCreated": 0,
                "Data": None,
                "PlotLine": None
            }
    else:
        if myKey in myDict:
            # Parse the payload
            payload = msg.payload
            descriptorLength, metadataVer = struct.unpack_from('HH', payload)
            # how many samples and what's its type, float or double?
            cType = myDict[myKey]["DataType"]
            nSamples = myDict[myKey]["SamplesInPayload"]
            if nSamples == -1: # unknown or variable
                # calculate nSamples from the payload length
                payload_len = len(payload)
                nSamples = round((payload_len-descriptorLength)/struct.calcsize(cType))
            # Data
            strBinFormat = str(nSamples) + str(cType)  # e.g., '640f' for 640 floats
            # data
            data = np.array(struct.unpack_from(strBinFormat, payload, descriptorLength))
            # time stamp of the payload
            secFromEpoch = struct.unpack_from('Q', payload, 4)[0]
            nanosec = struct.unpack_from('Q', payload, 12)[0]
            # nSamples
            nSamplesFromDAQStart = 0
            if metadataVer >= 2:
                nSamplesFromDAQStart = struct.unpack_from('Q', payload, 20)[0]
            else:
                raise Exception("Incompatible version. Use earlier version of cpsns_LifePlot!")
            if myDict[myKey]["SampleWhenEntryCreated"] == 0:
                # First time
                myDict[myKey]["SampleWhenEntryCreated"] = nSamplesFromDAQStart

            myDict[myKey]["PayloadQueue"].put({"SampleFromDAQStart": nSamplesFromDAQStart, "PayloadData": data})
        else:
            print("Waiting for the metadata...")
    
    bWritingMyDict = False


def main():
    global strMQTTTopic
    global myDict, bReadingMyDict, bWritingMyDict
    # Parse command line parameters
    # Create the parser
    parser = argparse.ArgumentParser(description="This Python script reads the time data from MQTT and outputs it on a life graph.")
    parser.add_argument('--host', type=str, help='Specify the host to connect to. Defaults to ' + HOST_DEFAULT, default=HOST_DEFAULT)
    parser.add_argument('--port', type=int, help='Connect to the port specified. Defaults to ' + str(PORT_DEFAULT), default=PORT_DEFAULT)
    parser.add_argument('--username', type=str, help='Provide a username to be used for authenticating with the broker. See also the --pw argument. Defaults to ' + USERNAME_DEFAULT, default=USERNAME_DEFAULT)
    parser.add_argument('--pw', type=str, help='Provide a password to be used for authenticating with the broker. See also the --username option. Defaults to ' + PASSWORD_DEFAULT, default=PASSWORD_DEFAULT)
    parser.add_argument('--topic', type=str, help='The topic parameter. Defaults to ' + MQTT_TOPIC_DEFAULT, default=MQTT_TOPIC_DEFAULT)

    # Parse the arguments
    args = parser.parse_args()

    strMQTTTopic = args.topic

    # Set username and password
    mqttc.username_pw_set(args.username, args.pw)

    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.on_subscribe = on_subscribe
    mqttc.connect(args.host, args.port, 60)

    mqttc.loop_start()

    # initiate the plt
    plt.ion()
    fig, ax = plt.subplots()
    # New: grid lines
    ax.grid(True)
    # Labels
    ax.set_xlabel('Time, s')

    # common time axis
    tAxisCommon = None

    last_redraw_time = time.time()
    while True:
        bNeedToReset = False
        # Dima 18-Dec
        current_time = time.time()
        if current_time-last_redraw_time >= 0.1:
            bNeedToRedraw = True
            last_redraw_time=current_time
        else:
            bNeedToRedraw = False

        if bNeedToRedraw:
            while bWritingMyDict:
                # make the thread sleep
                # print("Waiting for bWritingMyDict")
                time.sleep(0.0001)
            bReadingMyDict = True    
            
            for key, val in myDict.items():
                if val["PayloadQueue"].empty():
                    continue

                # Define the global time axis
                if tAxisCommon is None:
                    TimeToCover = 3 # seconds
                    tAxisCommon = {"Fs": val["SampleRate"], "AxisStartsAtSample": val["SampleWhenEntryCreated"], "AxisLength": int(round(TimeToCover * val["SampleRate"]))}
                    #print(f'AxisLength = {tAxisCommon["AxisLength"]}')

                if val["Data"] is None:
                    # allocate
                    val["Data"] = np.full(tAxisCommon["AxisLength"], 0, dtype=np.float32)

                while not val["PayloadQueue"].empty():
                    # place the data where it is suposed to be
                    payload = val["PayloadQueue"].get()
                    inx = payload["SampleFromDAQStart"]
                    inxToPlaceData = inx - tAxisCommon["AxisStartsAtSample"]
                    if inxToPlaceData<0:
                        ## ignore this payload...
                        #print(f"key={key}. Negative index: {inxToPlaceData}. The payload is ignored!")  
                        #continue
                        # need to reset!
                        bNeedToReset = True
                        break

                    data = payload["PayloadData"]
                    if inxToPlaceData+len(data) < tAxisCommon["AxisLength"]:
                        # okay
                        val["Data"][inxToPlaceData:inxToPlaceData+len(data)] = data
                    else:
                        # need to "scroll"... by 
                        scrl = 1 + inxToPlaceData+len(data) - tAxisCommon["AxisLength"]
                        #print(f"key={key}. Scrolling by {scrl} elements")  
                        # scroll the time axis
                        tAxisCommon["AxisStartsAtSample"] += scrl
                        if scrl < tAxisCommon["AxisLength"]:
                            # scroll all other dataset
                            for kk in myDict:
                                if myDict[kk]["Data"] is None:
                                    continue
                                myDict[kk]["Data"] = np.roll(myDict[kk]["Data"], -scrl)
                                if kk == key:
                                    myDict[kk]["Data"][inxToPlaceData-scrl:inxToPlaceData+len(data)-scrl] = data
                                else:
                                    myDict[kk]["Data"][-scrl:] = np.full(scrl, 0, dtype=np.float32)                                                
                        else:
                            # need to reset!
                            bNeedToReset = True
                            break

                # add a line if it is not here
                if val["PlotLine"] is None:
                    ta = np.linspace(0, tAxisCommon["AxisLength"]/tAxisCommon["Fs"], tAxisCommon["AxisLength"])
                    line, = ax.plot(ta, val["Data"], label=str(key))
                    plt.legend(loc="upper left")
                    val["PlotLine"] = line

                line = val["PlotLine"]
                line.set_ydata(val["Data"])

            if bNeedToReset:
                print("Long break in the data detected. Resetting the plot", file=sys.stderr)
                bNeedToReset = False
                # remove the lines
                for kk in myDict:
                    if myDict[kk]["PlotLine"] is not None:
                        myDict[kk]["PlotLine"].remove()

                # reset the dictionary
                myDict = {}
                tAxisCommon = None

                # remove the legend
                plt.legend().remove()
                plt.show()

            bReadingMyDict = False

            fig.canvas.draw()
            fig.canvas.flush_events()

        time.sleep(0.1) # Dima 16-Dec


if __name__ == "__main__":
    main()
