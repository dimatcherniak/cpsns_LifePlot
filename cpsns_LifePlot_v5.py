"""
This program will
1. Get the configuration from private and public JSON configuration files
1. read CP-SENS MQTT messages v.2, both data and metadata
2. plot the data

Rendering uses PyQtGraph (Qt/PySide6). Matplotlib re-rasterised the whole canvas
on every frame, which could not keep up with 16 channels at this message rate.
"""
import numpy as np
import pyqtgraph as pg
from PySide6 import QtCore, QtWidgets
import ssl
from paho.mqtt.client import Client as MQTTClient
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.client import MQTTv311
import queue
import struct
import time
import argparse
import json
import sys
import os

HOST_DEFAULT = "dtl-server-2.st.lab.au.dk"
PORT_DEFAULT = 8090
MQTT_TOPIC_DEFAULT = "cpsens/+/+/1/acc/raw/+"  # "cpsens/+/+/1/+/+/data"
YLIM_DEFAULT = 3 # MATLAB's ylim([-3 3])
BUFFER_TO_DRAW_DEFAULT = 3 # seconds

# Default configuration files
PRIVATE_CONFIG_FILE_DEFAULT = "private_config.json" # locate this file in the private folder and chmod 600 it.
PUBLIC_CONFIG_FILE_DEFAULT = "public_config.json"   # this file can be located in a public folder

# Rendering defaults, overridable from the "Graph" section of the public config.
UPDATE_INTERVAL_MS_DEFAULT = 33
ANTIALIAS_DEFAULT = False
USE_OPENGL_DEFAULT = False
# "xy", "x", "y" or "none". Horizontal grid lines span the whole plot width and
# are repainted every frame, so "xy" roughly halves the frame rate under WSLg.
GRID_DEFAULT = "x"
# --line_width. Qt rasterises 1 px pens with a fast path; wider pens go through
# full stroking and cost roughly half the frame rate per extra pixel.
LINE_WIDTH_DEFAULT = 1
# --theme. pg.intColor() spreads hues at full saturation, which assumes a dark
# background: yellow and cyan are close to invisible on white.
THEME_DEFAULT = "dark"

json_config_private = {}
json_config_public = {}
strMQTTTopic = MQTT_TOPIC_DEFAULT

# The MQTT thread only ever puts here, the GUI thread only ever gets. This
# replaces the old bReadingMyDict/bWritingMyDict spin locks, which were racy.
incoming = queue.Queue()

# Owned by the MQTT thread: decoding a data payload needs its metadata.
channel_meta = {}


def _channel_label(key):
    """Short legend entry built from the topic tuple."""
    return "/".join(key[2:]) if len(key) > 2 else "/".join(key)


def _decode_metadata(payload):
    json_metadata = json.loads(payload)
    try:
        time_at_start = json_metadata["TimeAtAquisitionStart"]
    except KeyError:
        print("Incompatible version. Use earlier version of cpsns_LifePlot!", file=sys.stderr)
        return None
    return {
        "SamplesInPayload": json_metadata["Data"]["Samples"],
        "DataType": json_metadata["Data"]["Type"][0],
        # take the sampling freq. from the last element of the analysis chain!
        "SampleRate": json_metadata["Analysis chain"][-1]["Sampling"],
        "PhysicalQuantity": json_metadata["Analysis chain"][-1]["Output"],
        "Units": json_metadata["Data"]["Unit"],
        "SecAtAcqusiitionStart": time_at_start["Seconds"],
        "Nanosec": time_at_start["Nanosec"],
    }


def _decode_data(meta, payload):
    """Returns (absolute index of the first sample, samples as float32)."""
    # Trying to load in big-endian and little-endian ways
    char_LE, char_BE = '<', '>'
    _, metadataVer_LE = struct.unpack_from(char_LE + 'HH', payload)
    _, metadataVer_BE = struct.unpack_from(char_BE + 'HH', payload)
    char_Endian = char_LE if metadataVer_LE < metadataVer_BE else char_BE

    descriptorLength, metadataVer = struct.unpack_from(char_Endian + 'HH', payload)
    if metadataVer < 2:
        raise Exception("Incompatible version. Use earlier version of cpsns_LifePlot!")

    cType = meta["DataType"]
    nSamples = meta["SamplesInPayload"]
    if nSamples == -1:  # unknown or variable
        nSamples = (len(payload) - descriptorLength) // struct.calcsize(cType)

    # np.frombuffer avoids building a 640-element Python tuple per message
    data = np.frombuffer(payload, dtype=np.dtype(char_Endian + cType),
                         count=nSamples, offset=descriptorLength)
    nSamplesFromDAQStart = struct.unpack_from(char_Endian + 'Q', payload, 20)[0]
    return nSamplesFromDAQStart, data.astype(np.float32)


def _start_simulator(window, n_channels):
    """Feed synthetic blocks through `incoming` at the real rate, for testing the render path."""
    fs, block = 1024.0, 64
    meta = {
        "SamplesInPayload": block,
        "DataType": "f",
        "SampleRate": fs,
        "PhysicalQuantity": "Acceleration",
        "Units": "m/s^2",
        "SecAtAcqusiitionStart": 0,
        "Nanosec": 0,
    }
    keys = [("cpsens", "sim", "dev", "1", "acc", "raw", f"ch{i + 1}") for i in range(n_channels)]
    for key in keys:
        incoming.put(("meta", key, meta))

    cursor = {"sample": 0}
    rng = np.random.default_rng(0)

    def emit():
        n0 = cursor["sample"]
        t = (np.arange(block) + n0) / fs
        for i, key in enumerate(keys):
            y = 3.0 * np.sin(2.0 * np.pi * (i + 1) * t) # + 0.2 * rng.standard_normal(block)
            incoming.put(("data", key, n0, y.astype(np.float32)))
        cursor["sample"] = n0 + block

    timer = QtCore.QTimer(window)
    timer.timeout.connect(emit)
    timer.start(int(1000 * block / fs))
    return timer


class LivePlotWindow(QtWidgets.QMainWindow):
    """Scrolling multi-channel time plot fed from the MQTT thread through `incoming`.

    Each channel keeps a buffer of 2*n samples; the visible window is the
    contiguous view buf[head:head+n]. Scrolling advances `head` instead of
    calling np.roll, so a payload costs one memcpy of its own length.
    """

    def __init__(self, graph_cfg, line_width=LINE_WIDTH_DEFAULT,
                 theme=THEME_DEFAULT):
        super().__init__()
        self.time_to_cover = float(graph_cfg["TimeToCover"])
        self.ylim = graph_cfg["ylim"]
        self.line_width = float(line_width)
        self.colors = [pg.mkColor(c) for c in graph_cfg.get("Colors", [])]
        self.dark = theme == "dark"

        self.channels = {}
        self.meta_by_key = {}
        self.axis = None   # {"Fs": float, "start": int, "n": int}
        self.head = 0
        self.x = None      # time axis, constant once the sample rate is known
        self.dirty = set()
        self.frames = 0
        self.fps_t0 = time.perf_counter()

        self.setWindowTitle("CP-SENS LifePlot")
        self.resize(1200, 700)

        self.plot_widget = pg.PlotWidget()
        self.plot = self.plot_widget.getPlotItem()
        grid = str(graph_cfg.get("Grid", GRID_DEFAULT)).lower()
        self.plot.showGrid(x="x" in grid, y="y" in grid, alpha=0.3)
        self.plot.setLabel("bottom", "Time", units="s")
        self.plot.setXRange(0.0, self.time_to_cover, padding=0)
        self.plot.setYRange(self.ylim[0], self.ylim[1], padding=0)
        self.plot.disableAutoRange()
        self.legend = self.plot.addLegend(offset=(10, 10))

        self.pause_button = QtWidgets.QPushButton("Pause")
        self.pause_button.setCheckable(True)
        self.autoscale_box = QtWidgets.QCheckBox("Auto Y")
        self.autoscale_box.toggled.connect(self._on_autoscale_toggled)
        self.dark_box = QtWidgets.QCheckBox("Dark")
        self.dark_box.setChecked(self.dark)
        self.dark_box.toggled.connect(self._on_theme_toggled)
        # Neither mouse zoom nor the context menu can restore the configured
        # window, and the menu's per-axis "Auto" leaves auto-range running.
        self.reset_view_button = QtWidgets.QPushButton("Reset view")
        self.reset_view_button.clicked.connect(self._reset_view)
        self.status_label = QtWidgets.QLabel("--")

        controls = QtWidgets.QHBoxLayout()
        controls.addWidget(self.pause_button)
        controls.addWidget(self.autoscale_box)
        controls.addWidget(self.dark_box)
        controls.addWidget(self.reset_view_button)
        controls.addStretch(1)
        controls.addWidget(self.status_label)

        layout = QtWidgets.QVBoxLayout()
        layout.addLayout(controls)
        layout.addWidget(self.plot_widget)
        central = QtWidgets.QWidget()
        central.setLayout(layout)
        self.setCentralWidget(central)

        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(self._tick)
        self.timer.start(int(graph_cfg.get("UpdateIntervalMs", UPDATE_INTERVAL_MS_DEFAULT)))

        self._apply_theme()

    def _tick(self):
        self._drain()
        if self.dirty and not self.pause_button.isChecked():
            self._redraw()
        self._update_status()

    def _drain(self):
        # Keep ingesting even while paused, otherwise the queue grows unbounded.
        while True:
            try:
                item = incoming.get_nowait()
            except queue.Empty:
                return
            if item[0] == "meta":
                self.meta_by_key[item[1]] = item[2]
            elif not self._ingest(item[1], item[2], item[3]):
                print("Long break in the data detected. Resetting the plot", file=sys.stderr)
                self._reset()
                return

    def _ingest(self, key, first_sample, data):
        meta = self.meta_by_key.get(key)
        if meta is None:
            return True

        if self.axis is None:
            fs = float(meta["SampleRate"])
            n = int(round(self.time_to_cover * fs))
            self.axis = {"Fs": fs, "start": int(first_sample), "n": n}
            self.x = np.arange(n, dtype=np.float64) / fs
            self.head = 0

        channel = self.channels.get(key)
        if channel is None:
            channel = self._add_channel(key, meta)

        n = self.axis["n"]
        pos = int(first_sample) - self.axis["start"]
        if pos < 0:
            return False
        if pos + len(data) > n:
            if not self._scroll(pos + len(data) - n):
                return False
            pos = int(first_sample) - self.axis["start"]

        start = self.head + pos
        channel["buf"][start:start + len(data)] = data
        self.dirty.add(key)
        return True

    def _add_channel(self, key, meta):
        n = self.axis["n"]
        buf = np.zeros(2 * n, dtype=np.float32)
        pen = pg.mkPen(self._curve_color(len(self.channels)), width=self.line_width)
        # PlotCurveItem rather than PlotDataItem: the latter's per-update
        # bookkeeping roughly halves the achievable frame rate here.
        curve = pg.PlotCurveItem(pen=pen, connect="all", skipFiniteCheck=True)
        self.plot.addItem(curve)
        self.legend.addItem(curve, _channel_label(key))
        self.plot.setLabel("left", meta["PhysicalQuantity"], units=meta["Units"])
        channel = {"buf": buf}
        channel["curve"] = curve
        self.channels[key] = channel
        print(f"Key added: {key}.")
        return channel

    def _scroll(self, shift):
        """Slide the common window right. False means the gap is too big to bridge."""
        n = self.axis["n"]
        if shift >= n:
            return False
        if self.head + shift + n > 2 * n:
            self._rebase()
        for channel in self.channels.values():
            channel["buf"][self.head + n:self.head + n + shift] = 0.0
        self.head += shift
        self.axis["start"] += shift
        self.dirty.update(self.channels)
        return True

    def _rebase(self):
        """Move the live window back to the start of the double-length buffers."""
        n = self.axis["n"]
        for channel in self.channels.values():
            channel["buf"][0:n] = channel["buf"][self.head:self.head + n].copy()
        self.head = 0

    def _redraw(self):
        window = slice(self.head, self.head + self.axis["n"])
        for key in self.dirty:
            channel = self.channels.get(key)
            if channel is not None:
                channel["curve"].setData(x=self.x, y=channel["buf"][window],
                                         skipFiniteCheck=True)
        self.dirty.clear()
        self.frames += 1

    def _reset(self):
        for channel in self.channels.values():
            self.plot.removeItem(channel["curve"])
        self.legend.clear()
        self.channels.clear()
        self.dirty.clear()
        self.axis = None
        self.head = 0
        self.x = None

    def _on_autoscale_toggled(self, checked):
        if checked:
            self.plot.enableAutoRange(axis="y")
        else:
            self.plot.disableAutoRange(axis="y")
            self.plot.setYRange(self.ylim[0], self.ylim[1], padding=0)

    def _curve_color(self, index):
        if self.colors:
            return self.colors[index % len(self.colors)]
        # Full-brightness hues need a dark background; dim them for the light theme.
        return (pg.intColor(index, hues=16) if self.dark
                else pg.intColor(index, hues=16, maxValue=170))

    def _on_theme_toggled(self, checked):
        self.dark = checked
        self._apply_theme()

    def _apply_theme(self):
        foreground = "d" if self.dark else "k"
        self.plot_widget.setBackground("k" if self.dark else "w")
        for name in ("left", "bottom", "top", "right"):
            axis = self.plot.getAxis(name)
            axis.setPen(foreground)
            axis.setTextPen(foreground)
        self.legend.setLabelTextColor(foreground)
        for index, channel in enumerate(self.channels.values()):
            channel["curve"].setPen(pg.mkPen(self._curve_color(index), width=self.line_width))

    def _reset_view(self):
        self.plot.disableAutoRange()
        self.plot.setXRange(0.0, self.time_to_cover, padding=0)
        self._on_autoscale_toggled(self.autoscale_box.isChecked())

    def _update_status(self):
        now = time.perf_counter()
        elapsed = now - self.fps_t0
        if elapsed < 0.5:
            return
        self.status_label.setText(
            f"{self.frames / elapsed:5.1f} fps | {len(self.channels)} ch | queue {incoming.qsize()}")
        self.frames = 0
        self.fps_t0 = now


def on_connect(mqttc_in, userdata, flags, rc, properties=None):
    global json_config_public
    print("MQTT_IN: Connected with response code %s" % rc)
    for topic in json_config_public["MQTT_IN"]["TopicsToSubscribe"]:
        print(f"MQTT_IN: Subscribing to the topic {topic}...")
        mqttc_in.subscribe(topic, qos=json_config_public["MQTT_IN"]["QoS"])

def on_subscribe(self, mqttc, userdata, msg, granted_qos):
    print("Subscribed. Message: " + str(msg))

def on_message(client, userdata, msg):    
    topic = msg.topic
    substrings = topic.split('/')
    if substrings[-1] == "data":
        bIsMetadata = False
    elif substrings[-1] == "metadata":
        bIsMetadata = True
    else:
        raise Exception("Unknown topic: " + substrings[-1])

    # Create a tuple made of the topic string without the last element (data/metadata)
    myKey = tuple(substrings[:-1])

    if bIsMetadata:
        if myKey not in channel_meta:
            meta = _decode_metadata(msg.payload)
            if meta is not None:
                channel_meta[myKey] = meta
                incoming.put(("meta", myKey, meta))
        return

    meta = channel_meta.get(myKey)
    if meta is None:
        print("Waiting for the metadata...")
        return

    nSamplesFromDAQStart, data = _decode_data(meta, msg.payload)
    incoming.put(("data", myKey, nSamplesFromDAQStart, data))


def main():
    global json_config_private, json_config_public
    # Parse command line parameters
    # Create the parser
    parser = argparse.ArgumentParser(description="This Python script reads the time data from MQTT and outputs it on a life graph.")
    parser.add_argument('--config_private', type=str, help='Specify the JSON configuration file for PRIVATE data. Defaults to ' + PRIVATE_CONFIG_FILE_DEFAULT, default=PRIVATE_CONFIG_FILE_DEFAULT)
    parser.add_argument('--config_public', type=str, help='Specify the JSON configuration file for PUBLIC data. Defaults to ' + PUBLIC_CONFIG_FILE_DEFAULT, default=PUBLIC_CONFIG_FILE_DEFAULT)
    parser.add_argument('--simulate', type=int, default=0, metavar='N', help='Plot N synthetic channels instead of connecting to MQTT. For checking the rendering path.')
    parser.add_argument('--line_width', type=float, default=LINE_WIDTH_DEFAULT, metavar='PX', help='Curve thickness in pixels. 1 uses Qt\'s fast pen path, wider pens are much slower to rasterise. Defaults to ' + str(LINE_WIDTH_DEFAULT) + '.')
    parser.add_argument('--theme', choices=['dark', 'light'], default=THEME_DEFAULT, help='Plot background. The default curve palette is much easier to tell apart on dark. Defaults to ' + THEME_DEFAULT + '.')

    # Parse the arguments
    args = parser.parse_args()

    # Parsing the configuration files..
    # Name of the private configuration file
    strConfigFile = args.config_private
    # Read the configuration file
    print(f"Reading private configuration from {strConfigFile}...")
    if os.path.exists(strConfigFile):
        try:
            # Open and read the JSON file
            with open(strConfigFile, 'r') as file:
                json_config_private = json.load(file)
        except json.JSONDecodeError:
            print(f"Error: The file {strConfigFile} exists but could not be parsed as JSON.", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Error: The file {strConfigFile} does not exist.", file=sys.stderr)    
        sys.exit(1)

    # Name of the public configuration file
    strConfigFile = args.config_public
    # Read the configuration file
    print(f"Reading public configuration from {strConfigFile}...")
    if os.path.exists(strConfigFile):
        try:
            # Open and read the JSON file
            with open(strConfigFile, 'r') as file:
                json_config_public = json.load(file)
        except json.JSONDecodeError:
            print(f"Error: The file {strConfigFile} exists but could not be parsed as JSON.", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Error: The file {strConfigFile} does not exist.", file=sys.stderr)    
        sys.exit(1)

    # MQTT_IN stuff
    mqttc_in = MQTTClient(callback_api_version=CallbackAPIVersion.VERSION2, protocol=MQTTv311)

    # Set username and password
    if json_config_private["MQTT_IN"]["userId"] != "":
        mqttc_in.username_pw_set(json_config_private["MQTT_IN"]["userId"], json_config_private["MQTT_IN"]["password"])

    # TLS configuration (ONLY if enabled)
    if json_config_private["MQTT_IN"].get("useTLS", False):
        mqttc_in.tls_set(
            ca_certs=json_config_private["MQTT_IN"]["caFile"],
            certfile=None,
            keyfile=None,
            tls_version=ssl.PROTOCOL_TLS_CLIENT
        )

        # Optional but recommended
        mqttc_in.tls_insecure_set(False)

    mqttc_in.on_connect = on_connect
    mqttc_in.on_message = on_message
    mqttc_in.on_subscribe = on_subscribe
    if not args.simulate:
        mqttc_in.connect(json_config_private["MQTT_IN"]["host"], json_config_private["MQTT_IN"]["port"], 60) # we subscribe to the topics in on_connect callback
        mqttc_in.loop_start()
    # MQTT_IN done

    # plot params, from public configuration
    graph_cfg = json_config_public["Graph"]

    dark = args.theme == 'dark'
    pg.setConfigOption("background", "k" if dark else "w")
    pg.setConfigOption("foreground", "d" if dark else "k")
    # Antialiasing is the most expensive option under WSLg, and OpenGL there
    # usually falls back to llvmpipe, which is slower than the raster path.
    pg.setConfigOptions(
        antialias=bool(graph_cfg.get("Antialias", ANTIALIAS_DEFAULT)),
        useOpenGL=bool(graph_cfg.get("UseOpenGL", USE_OPENGL_DEFAULT)),
    )

    app = QtWidgets.QApplication(sys.argv)
    window = LivePlotWindow(graph_cfg, line_width=args.line_width, theme=args.theme)
    window.show()

    if args.simulate:
        window.simulator = _start_simulator(window, args.simulate)
    else:
        app.aboutToQuit.connect(mqttc_in.loop_stop)

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
# end of file                                                

