#!/usr/bin/env python3
"""
MQTT Fortune Bot for Meshtastic Networks

A simple bot that responds to direct messages with random fortunes from fortunes.txt
"""

#### Imports
try:
    from meshtastic.protobuf import mesh_pb2, mqtt_pb2, portnums_pb2, telemetry_pb2
    from meshtastic import BROADCAST_NUM
except ImportError:
    from meshtastic import mesh_pb2, mqtt_pb2, portnums_pb2, telemetry_pb2, BROADCAST_NUM

import random
import threading
import sqlite3
import time
import ssl
import string
import sys
import configparser
from datetime import datetime
from time import mktime
from typing import Optional
import base64
import json
import re
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import paho.mqtt.client as mqtt

from models import Node

# Node-Topic tracking
node_topic_map = {}  # Track which topic each node was last seen on

def update_node_topic(node_id, topic):
    """Update which topic a node was last seen on."""
    node_topic_map[node_id] = topic
    if debug:
        print(f"Updated node {node_id} last seen topic to: {topic}")
        
def get_node_topic_for_direct_message(destination_id):
    """Get the topic where a node was last seen, formatted for sending direct messages."""
    base_topic = node_topic_map.get(destination_id, None)
    
    if base_topic:
        # Extract root topic and reconstruct with OUR node ID in recipient's region
        # base_topic format: "msh/US/VA/RVA/2/e/LongFast/!somenode"
        topic_parts = base_topic.split('/')
        if len(topic_parts) >= 6:
            root_topic_part = '/'.join(topic_parts[:5]) + '/'
            our_node_hex = '!' + hex(node_number)[2:]  # Use OUR node ID, not destination
            direct_topic = root_topic_part + channel + "/" + our_node_hex
            if debug:
                print(f"Converted base topic {base_topic} to direct topic {direct_topic} for node {destination_id}")
            return direct_topic
    
    return None

def get_node_topic(node_id):
    """Get the topic where a node was last seen."""
    topic = node_topic_map.get(node_id, None)
    if debug:
        print(f"Node {node_id} last seen on topic: {topic}")
    return topic

def load_config():
    """Load configuration from config.ini file."""
    global mqtt_broker, mqtt_port, mqtt_username, mqtt_password, root_topic, root_topics, channel, key
    global node_number, client_long_name, client_short_name, lat, lon, alt
    global debug, auto_reconnect, auto_reconnect_delay
    global print_service_envelope, print_message_packet, print_text_message, print_node_info
    global print_telemetry, print_failed_encryption_packet, print_position_report, color_text
    global display_encrypted_emoji, display_dm_emoji, display_lookup_button, display_private_dms
    global record_locations, node_info_interval_minutes
    
    config = configparser.ConfigParser()
    
    try:
        config.read('config.ini')
        
        # MQTT Connection Settings
        mqtt_broker = config.get('DEFAULT', 'mqtt_broker', fallback='mqtt.meshtastic.org')
        mqtt_port = config.getint('DEFAULT', 'mqtt_port', fallback=1883)
        mqtt_username = config.get('DEFAULT', 'mqtt_username', fallback='meshdev')
        mqtt_password = config.get('DEFAULT', 'mqtt_password', fallback='large4cats')
        root_topic_config = config.get('DEFAULT', 'root_topic', fallback='msh/US/2/e/')
        
        # Parse comma-separated root topics
        root_topics = [topic.strip() for topic in root_topic_config.split(',') if topic.strip()]
        root_topic = root_topics[0] if root_topics else 'msh/US/2/e/'  # Use first topic as primary
        
        channel = config.get('DEFAULT', 'channel', fallback='LongFast')
        key = config.get('DEFAULT', 'key', fallback='AQ==')
        
        # Node Settings
        node_number = config.getint('DEFAULT', 'node_number', fallback=2882380807)
        client_long_name = config.get('DEFAULT', 'long_name', fallback='FortuneBot')
        client_short_name = config.get('DEFAULT', 'short_name', fallback='FB')
        
        # Position Settings
        lat = config.get('DEFAULT', 'lat', fallback='')
        lon = config.get('DEFAULT', 'lon', fallback='')
        alt = config.get('DEFAULT', 'alt', fallback='')
        
        # Debug Settings
        debug = config.getboolean('DEFAULT', 'debug', fallback=True)
        auto_reconnect = config.getboolean('DEFAULT', 'auto_reconnect', fallback=False)
        auto_reconnect_delay = config.getfloat('DEFAULT', 'auto_reconnect_delay', fallback=1.0)
        
        print_service_envelope = config.getboolean('DEFAULT', 'print_service_envelope', fallback=False)
        print_message_packet = config.getboolean('DEFAULT', 'print_message_packet', fallback=False)
        print_text_message = config.getboolean('DEFAULT', 'print_text_message', fallback=False)
        print_node_info = config.getboolean('DEFAULT', 'print_node_info', fallback=False)
        print_telemetry = config.getboolean('DEFAULT', 'print_telemetry', fallback=False)
        print_failed_encryption_packet = config.getboolean('DEFAULT', 'print_failed_encryption_packet', fallback=False)
        print_position_report = config.getboolean('DEFAULT', 'print_position_report', fallback=False)
        color_text = config.getboolean('DEFAULT', 'color_text', fallback=False)
        display_encrypted_emoji = config.getboolean('DEFAULT', 'display_encrypted_emoji', fallback=True)
        display_dm_emoji = config.getboolean('DEFAULT', 'display_dm_emoji', fallback=True)
        display_lookup_button = config.getboolean('DEFAULT', 'display_lookup_button', fallback=False)
        display_private_dms = config.getboolean('DEFAULT', 'display_private_dms', fallback=False)
        record_locations = config.getboolean('DEFAULT', 'record_locations', fallback=False)
        
        # Node Info Settings
        node_info_interval_minutes = config.getint('DEFAULT', 'node_info_interval_minutes', fallback=15)
        
        if debug:
            print("Configuration loaded from config.ini")
            
    except Exception as e:
        print(f"Error loading config.ini: {str(e)}")
        print("Using default configuration values")
        
        # Set defaults if config file fails to load
        mqtt_broker = "mqtt.meshtastic.org"
        mqtt_port = 1883
        mqtt_username = "meshdev"
        mqtt_password = "large4cats"
        root_topics = ["msh/US/2/e/"]
        root_topic = "msh/US/2/e/"
        channel = "LongFast"
        key = "AQ=="
        node_number = 2882380807
        client_long_name = "FortuneBot"
        client_short_name = "FB"
        lat = ""
        lon = ""
        alt = ""
        debug = True
        auto_reconnect = False
        auto_reconnect_delay = 1.0
        print_service_envelope = False
        print_message_packet = False
        print_text_message = False
        print_node_info = False
        print_telemetry = False
        print_failed_encryption_packet = False
        print_position_report = False
        color_text = False
        display_encrypted_emoji = True
        display_dm_emoji = True
        display_lookup_button = False
        display_private_dms = False
        record_locations = False
        node_info_interval_minutes = 15

# Program variables
default_key = "1PG7OiApB1nwvP+rz05pAQ==" # AKA AQ==
db_file_path = "fortune.db"
reserved_ids = [1,2,3,4,4294967295]

# Load configuration from config.ini
load_config()

# Additional variables that depend on config
max_msg_len = mesh_pb2.Constants.DATA_PAYLOAD_LEN
key_emoji = "\U0001F511"
encrypted_emoji = "\U0001F512"
dm_emoji = "\u2192"

client_hw_model = 255

def is_valid_hex(test_value: str, minchars: Optional[int], maxchars: int) -> bool:
    """Check if the provided string is valid hex."""
    if test_value.startswith('!'):
        test_value = test_value[1:]
    valid_hex_return: bool = all(c in string.hexdigits for c in test_value)

    decimal_value = int(test_value, 16)
    if decimal_value in reserved_ids:
        return False
    
    if minchars is not None:
        valid_hex_return = valid_hex_return and (minchars <= len(test_value))
    if maxchars is not None:
        valid_hex_return = valid_hex_return and (len(test_value) <= maxchars)

    return valid_hex_return

def set_topic():
    """Set the MQTT topics for subscribing and publishing."""
    if debug:
        print("set_topic")
    global subscribe_topics, publish_topic, node_number, node_name, root_topics
    node_name = '!' + hex(node_number)[2:]
    
    # Create subscription topics for all root topics
    subscribe_topics = [topic + channel + "/#" for topic in root_topics]
    
    # Use the first root topic for publishing
    publish_topic = root_topics[0] + channel + "/" + node_name
    
    if debug:
        print(f"Subscribe topics: {subscribe_topics}")
        print(f"Publish topic: {publish_topic}")

def current_time() -> str:
    """Return the current time as a string."""
    return str(int(time.time()))

def format_time(time_str: str) -> str:
    """Convert the time string back to a datetime object."""
    timestamp: int = int(time_str)
    time_dt: datetime = datetime.fromtimestamp(timestamp)
    now = datetime.now()

    if time_dt.date() == now.date():
        time_formatted = time_dt.strftime("%I:%M %p")
    else:
        time_formatted = time_dt.strftime("%d/%m/%y %H:%M:%S")

    return time_formatted

def xor_hash(data: bytes) -> int:
    """Return XOR hash of all bytes in the provided string."""
    result = 0
    for char in data:
        result ^= char
    return result

def generate_hash(name: str, key: str) -> int:
    """Generate hash for channel."""
    replaced_key = key.replace('-', '+').replace('_', '/')
    key_bytes = base64.b64decode(replaced_key.encode('utf-8'))
    h_name = xor_hash(bytes(name, 'utf-8'))
    h_key = xor_hash(key_bytes)
    result: int = h_name ^ h_key
    return result

def get_name_by_id(name_type: str, user_id: str) -> str:
    """Get name for the given user_id."""
    hex_user_id: str = '!%08x' % user_id

    try:
        table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_nodeinfo"
        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()

            if name_type == "long":
                result = db_cursor.execute(f'SELECT long_name FROM {table_name} WHERE user_id=?', (hex_user_id,)).fetchone()
            if name_type == "short":
                result = db_cursor.execute(f'SELECT short_name FROM {table_name} WHERE user_id=?', (hex_user_id,)).fetchone()

            if result:
                if debug:
                    print("found user in db: " + str(hex_user_id))
                return result[0]
            else:
                if user_id != BROADCAST_NUM:
                    if debug:
                        print("didn't find user in db: " + str(hex_user_id))
                    send_node_info(user_id, want_response=True)
                return f"Unknown User ({hex_user_id})"

    except sqlite3.Error as e:
        print(f"SQLite error in get_name_by_id: {e}")

    finally:
        db_connection.close()

def sanitize_string(input_str: str) -> str:
    """Sanitize string for database table names."""
    if not re.match(r'^[a-zA-Z_]', input_str):
        input_str = '_' + input_str
    sanitized_str: str = re.sub(r'[^a-zA-Z0-9_]', '_', input_str)
    return sanitized_str

def on_message(client, userdata, msg):
    """Callback function that accepts a meshtastic message from mqtt."""
    message_topic = msg.topic
    
    se = mqtt_pb2.ServiceEnvelope()
    is_encrypted: bool = False
    try:
        se.ParseFromString(msg.payload)
        if print_service_envelope:
            print("Service Envelope:")
            print(se)
        mp = se.packet

    except Exception as e:
        if debug:
            print(f"*** ServiceEnvelope: {str(e)}")
        return

    if len(msg.payload) > max_msg_len:
        if debug:
            print('Message too long: ' + str(len(msg.payload)) + ' bytes long, skipping.')
        return

    if mp.HasField("encrypted") and not mp.HasField("decoded"):
        decode_encrypted(mp)
        is_encrypted=True
    
    if print_message_packet:
        print("Message Packet:")
        print(mp)

    # Track which topic this node was seen on
    from_node = getattr(mp, "from")
    update_node_topic(from_node, message_topic)
    
    if mp.decoded.portnum == portnums_pb2.TEXT_MESSAGE_APP:
        try:
            text_payload = mp.decoded.payload.decode("utf-8")
            process_message(mp, text_payload, is_encrypted)
        except Exception as e:
            print(f"*** TEXT_MESSAGE_APP: {str(e)}")

    elif mp.decoded.portnum == portnums_pb2.NODEINFO_APP:
        info = mesh_pb2.User()
        try:
            info.ParseFromString(mp.decoded.payload)
            maybe_store_nodeinfo_in_db(info)
            if print_node_info:
                print("NodeInfo:")
                print(info)
        except Exception as e:
            print(f"*** NODEINFO_APP: {str(e)}")

def decode_encrypted(mp):
    """Decrypt a meshtastic message."""
    try:
        key_bytes = base64.b64decode(key.encode('ascii'))
        nonce_packet_id = getattr(mp, "id").to_bytes(8, "little")
        nonce_from_node = getattr(mp, "from").to_bytes(8, "little")
        nonce = nonce_packet_id + nonce_from_node

        cipher = Cipher(algorithms.AES(key_bytes), modes.CTR(nonce), backend=default_backend())
        decryptor = cipher.decryptor()
        decrypted_bytes = decryptor.update(getattr(mp, "encrypted")) + decryptor.finalize()

        data = mesh_pb2.Data()
        data.ParseFromString(decrypted_bytes)
        mp.decoded.CopyFrom(data)

    except Exception as e:
        if print_message_packet:
            print(f"failed to decrypt: \n{mp}")
        if debug:
            print(f"*** Decryption failed: {str(e)}")

def process_message(mp, text_payload, is_encrypted):
    """Process a single meshtastic text message."""
    if debug:
        print("process_message")
    
    if not message_exists(mp):
        from_node = getattr(mp, "from")
        to_node = getattr(mp, "to")

        # Ignore messages from our own node
        if from_node == node_number:
            if debug:
                print("Ignoring message from our own node")
            return

        message_id = getattr(mp, "id")
        want_ack: bool = getattr(mp, "want_ack")

        sender_short_name = get_name_by_id("short", from_node)
        receiver_short_name = get_name_by_id("short", to_node)
        display_str = ""
        private_dm = False

        if debug:
            print(f"Message from {from_node} to {to_node}, my node number is {node_number}")
            print(f"Message content: {text_payload}")

        if to_node == node_number:
            if debug:
                print("This is a direct message to me!")
                print(f"Processing DM from {from_node}: '{text_payload}'")
            display_str = f"{format_time(current_time())} DM from {sender_short_name}: {text_payload}"
            if display_dm_emoji:
                display_str = display_str[:9] + dm_emoji + display_str[9:]
            if want_ack is True:
                send_ack(from_node, message_id)
            
            # Send fortune response to any direct message
            if debug:
                print(f"Sending fortune response to {from_node}")
            # Send fortune in a separate thread with a delay
            def delayed_fortune_response():
                time.sleep(6.0)  # Wait 6 seconds for processing
                send_fortune(from_node)
            
            response_thread = threading.Thread(target=delayed_fortune_response, daemon=True)
            response_thread.start()

        elif from_node == node_number and to_node != BROADCAST_NUM:
            display_str = f"{format_time(current_time())} DM to {receiver_short_name}: {text_payload}"

        elif from_node != node_number and to_node != BROADCAST_NUM:
            if display_private_dms:
                display_str = f"{format_time(current_time())} DM from {sender_short_name} to {receiver_short_name}: {text_payload}"
                if display_dm_emoji:
                    display_str = display_str[:9] + dm_emoji + display_str[9:]
            else:
                if debug:
                    print("Private DM Ignored")
                private_dm = True

        else:
            display_str = f"{format_time(current_time())} {sender_short_name}: {text_payload}"

        if is_encrypted and not private_dm:
            color="encrypted"
            if display_encrypted_emoji:
                display_str = display_str[:9] + encrypted_emoji + display_str[9:]
        else:
            color="unencrypted"
        if not private_dm:
            update_console(display_str, tag=color)
        
        m_id = getattr(mp, "id")
        insert_message_to_db(current_time(), sender_short_name, text_payload, m_id, is_encrypted)

        if print_text_message:
            text = {
                "message": text_payload,
                "from": getattr(mp, "from"),
                "id": getattr(mp, "id"),
                "to": getattr(mp, "to")
            }
            rssi = getattr(mp, "rx_rssi")
            if rssi:
                text["RSSI"] = rssi
            print(text)
    else:
        if debug:
            print("duplicate message ignored")

def send_fortune(target_id):
    """Send a random fortune from fortunes.txt."""
    if debug:
        print(f"Sending fortune to {target_id}")

    if not client.is_connected():
        if debug:
            print("Not connected to MQTT broker, skipping fortune")
        return

    try:
        # Read fortunes from file
        with open('fortunes.txt', 'r', encoding='utf-8') as f:
            fortunes = [line.strip() for line in f if line.strip()]
        
        if not fortunes:
            fortune_text = "No fortunes available at this time."
            if debug:
                print("No fortunes found in file")
        else:
            # Pick a random fortune
            fortune_text = random.choice(fortunes)
            if debug:
                print(f"Selected fortune: {fortune_text}")
        
        encoded_message = mesh_pb2.Data()
        encoded_message.portnum = portnums_pb2.TEXT_MESSAGE_APP
        encoded_message.payload = fortune_text.encode("utf-8")
        encoded_message.bitfield = 1
        
        if debug:
            print(f"Sending fortune to {target_id}: {fortune_text}")
        
        # Add delay for processing before sending
        time.sleep(2.0)
        
        generate_mesh_packet(target_id, encoded_message)
        
        if debug:
            print(f"Fortune sent to {target_id}")
            
    except FileNotFoundError:
        error_msg = "Fortune file not found."
        print(error_msg)
        if debug:
            print("=== FORTUNE ERROR (FILE NOT FOUND) ===")
    except Exception as e:
        print(f"Error sending fortune: {str(e)}")

def message_exists(mp) -> bool:
    """Check for message id in db, ignore duplicates."""
    if debug:
        print("message_exists")
    try:
        table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_messages"

        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()
            existing_record = db_cursor.execute(f'SELECT * FROM {table_name} WHERE message_id=?', (str(getattr(mp, "id")),)).fetchone()
            return existing_record is not None

    except sqlite3.Error as e:
        print(f"SQLite error in message_exists: {e}")
    finally:
        db_connection.close()

def send_node_info(destination_id, want_response):
    """Send my node information to the specified destination."""
    global node_number

    if debug:
        print("send_node_info")

    if not client.is_connected():
        update_console(f"{format_time(current_time())} >>> Connect to a broker before sending nodeinfo", tag="info")
    else:
        if not move_text_up():
            return
        
        if destination_id == BROADCAST_NUM:
            update_console(f"{format_time(current_time())} >>> Broadcast NodeInfo Packet", tag="info")
        else:
            if debug:
                print(f"Sending NodeInfo Packet to {str(destination_id)}")

        decoded_client_id = bytes(node_name, "utf-8")
        decoded_client_long = bytes(client_long_name, "utf-8")
        decoded_client_short = bytes(client_short_name, "utf-8")
        decoded_client_hw_model = client_hw_model

        user_payload = mesh_pb2.User()
        setattr(user_payload, "id", decoded_client_id)
        setattr(user_payload, "long_name", decoded_client_long)
        setattr(user_payload, "short_name", decoded_client_short)
        setattr(user_payload, "hw_model", decoded_client_hw_model)

        user_payload = user_payload.SerializeToString()

        encoded_message = mesh_pb2.Data()
        encoded_message.portnum = portnums_pb2.NODEINFO_APP
        encoded_message.payload = user_payload
        encoded_message.want_response = want_response
        encoded_message.bitfield = 1

        generate_mesh_packet(destination_id, encoded_message)

def generate_mesh_packet(destination_id, encoded_message):
    """Send a packet out over the mesh."""
    global global_message_id
    mesh_packet = mesh_pb2.MeshPacket()

    mesh_packet.id = global_message_id
    global_message_id += 1

    setattr(mesh_packet, "from", node_number)
    mesh_packet.to = destination_id
    mesh_packet.want_ack = True
    mesh_packet.channel = generate_hash(channel, key)
    
    if destination_id != BROADCAST_NUM:
        mesh_packet.hop_limit = 3
        mesh_packet.hop_start = 3
    else:
        mesh_packet.hop_limit = 3
        mesh_packet.hop_start = 3

    if debug:
        print(f"Generating mesh packet: from={node_number}, to={destination_id}, id={mesh_packet.id}, hops={mesh_packet.hop_limit}")

    if key == "":
        mesh_packet.decoded.CopyFrom(encoded_message)
        if debug:
            print("key is none")
    else:
        mesh_packet.encrypted = encrypt_message(channel, key, mesh_packet, encoded_message)
        if debug:
            print("key present")

    service_envelope = mqtt_pb2.ServiceEnvelope()
    service_envelope.packet.CopyFrom(mesh_packet)
    service_envelope.channel_id = channel
    service_envelope.gateway_id = node_name

    payload = service_envelope.SerializeToString()
    set_topic()
    
    # For broadcast messages, publish to ALL topics
    if destination_id == BROADCAST_NUM:
        if debug:
            print(f"Broadcasting to all topics")
            print(f"Payload size: {len(payload)} bytes")
        
        for i, root_topic in enumerate(root_topics):
            broadcast_topic = root_topic + channel + "/" + node_name
            if debug:
                print(f"Publishing to topic {i+1}/{len(root_topics)}: {broadcast_topic}")
            
            result = client.publish(broadcast_topic, payload)
            
            if debug:
                print(f"MQTT publish result for {broadcast_topic}: {result.rc}")
                if result.rc == 0:
                    print(f"MQTT publish successful to {broadcast_topic}")
                else:
                    print(f"MQTT publish failed to {broadcast_topic} with code: {result.rc}")
    else:
        # For direct messages, try to send to recipient's last known region from our node
        recipient_topic = get_node_topic_for_direct_message(destination_id)
        
        if recipient_topic:
            # Send from our node in the specific region where recipient was last seen
            if debug:
                print(f"Sending direct message from our node in recipient's region: {recipient_topic}")
                print(f"Payload size: {len(payload)} bytes")
            
            result = client.publish(recipient_topic, payload)
            
            if debug:
                print(f"MQTT publish result for {recipient_topic}: {result.rc}")
                if result.rc == 0:
                    print(f"Direct message published successfully to {recipient_topic}")
                else:
                    print(f"Direct message failed to {recipient_topic} with code: {result.rc}")
        else:
            # Fallback: broadcast from our node to all regions
            if debug:
                print(f"No known topic for recipient {destination_id}, broadcasting from our node to all regions")
                print(f"Payload size: {len(payload)} bytes")
            
            # Publish from our node to all root topics
            for i, root_topic in enumerate(root_topics):
                broadcast_topic = root_topic + channel + "/" + node_name
                if debug:
                    print(f"Publishing direct message from our node to topic {i+1}/{len(root_topics)}: {broadcast_topic}")
                
                result = client.publish(broadcast_topic, payload)
                
                if debug:
                    print(f"MQTT publish result for {broadcast_topic}: {result.rc}")
                    if result.rc == 0:
                        print(f"Direct message published successfully to {broadcast_topic}")
                    else:
                        print(f"Direct message failed to {broadcast_topic} with code: {result.rc}")
                
                if i < len(root_topics) - 1:
                    time.sleep(0.1)

def encrypt_message(channel, key, mesh_packet, encoded_message):
    """Encrypt a message."""
    if debug:
        print("encrypt_message")

    if key == "AQ==":
        key = "1PG7OiApB1nwvP+rz05pAQ=="

    mesh_packet.channel = generate_hash(channel, key)
    key_bytes = base64.b64decode(key.encode('ascii'))

    nonce_packet_id = mesh_packet.id.to_bytes(8, "little")
    nonce_from_node = node_number.to_bytes(8, "little")
    nonce = nonce_packet_id + nonce_from_node

    cipher = Cipher(algorithms.AES(key_bytes), modes.CTR(nonce), backend=default_backend())
    encryptor = cipher.encryptor()
    encrypted_bytes = encryptor.update(encoded_message.SerializeToString()) + encryptor.finalize()

    return encrypted_bytes

def send_ack(destination_id, message_id):
    """Return a meshtastic acknowledgement."""
    if debug:
        print("Sending ACK")

    encoded_message = mesh_pb2.Data()
    encoded_message.portnum = portnums_pb2.ROUTING_APP
    encoded_message.request_id = message_id
    encoded_message.payload = b"\030\000"

    generate_mesh_packet(destination_id, encoded_message)

def setup_db():
    """Setup database tables."""
    try:
        table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_messages"
        nodeinfo_table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_nodeinfo"
        
        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()
            
            # Create messages table
            db_cursor.execute(f'''CREATE TABLE IF NOT EXISTS {table_name}
                                (time TEXT, sender_short_name TEXT, text_payload TEXT, message_id TEXT, is_encrypted INTEGER)''')
            
            # Create nodeinfo table
            db_cursor.execute(f'''CREATE TABLE IF NOT EXISTS {nodeinfo_table_name}
                                (user_id TEXT PRIMARY KEY, long_name TEXT, short_name TEXT, hw_model INTEGER)''')
            
            db_connection.commit()
            if debug:
                print("Database tables created/verified")
                
    except sqlite3.Error as e:
        print(f"SQLite error in setup_db: {e}")
    finally:
        db_connection.close()

def maybe_store_nodeinfo_in_db(info):
    """Save nodeinfo in sqlite unless that record is already there."""
    if debug:
        print("node info packet received: Checking for existing entry in DB")

    table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_nodeinfo"

    try:
        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()

            existing_record = db_cursor.execute(f'SELECT * FROM {table_name} WHERE user_id=?', (info.id,)).fetchone()

            if existing_record is None:
                if debug:
                    print("no record found, adding node to db")
                db_cursor.execute(f'''
                    INSERT INTO {table_name} (user_id, long_name, short_name)
                    VALUES (?, ?, ?)
                ''', (info.id, info.long_name, info.short_name))
                db_connection.commit()

                new_record = db_cursor.execute(f'SELECT user_id, short_name, long_name FROM {table_name} WHERE user_id=?', (info.id,)).fetchone()
                new_node = Node(*new_record)

                if debug:
                    print(f"New node added: {new_node.node_list_disp}")
                
            else:
                if existing_record[1] != info.long_name or existing_record[2] != info.short_name:
                    if debug:
                        print("updating existing record in db")
                    db_cursor.execute(f'''
                        UPDATE {table_name}
                        SET long_name=?, short_name=?
                        WHERE user_id=?
                    ''', (info.long_name, info.short_name, info.id))
                    db_connection.commit()

                    updated_record = db_cursor.execute(f'SELECT * FROM {table_name} WHERE user_id=?', (info.id,)).fetchone()

                    if debug:
                        print(f"Node updated: {updated_record[0]}, {updated_record[1]}, {updated_record[2]}")

    except sqlite3.Error as e:
        print(f"SQLite error in maybe_store_nodeinfo_in_db: {e}")
    finally:
        db_connection.close()

def insert_message_to_db(time, sender_short_name, text_payload, message_id, is_encrypted):
    """Save a meshtastic message to sqlite storage."""
    if debug:
        print("insert_message_to_db")

    table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_messages"

    try:
        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()
            formatted_message = text_payload.strip()
            db_cursor.execute(f'INSERT INTO {table_name} (time, sender_short_name, text_payload, message_id, is_encrypted) VALUES (?,?,?,?,?)',
                              (time, sender_short_name, formatted_message, message_id, is_encrypted))
            db_connection.commit()

    except sqlite3.Error as e:
        print(f"SQLite error in insert_message_to_db: {e}")
    finally:
        db_connection.close()

def connect_mqtt():
    """Connect to the MQTT server."""
    if debug:
        print("connect_mqtt")
    global mqtt_broker, mqtt_port, mqtt_username, mqtt_password, root_topic, root_topics, channel, node_number, db_file_path, key
    if not client.is_connected():
        try:
            if ':' in mqtt_broker:
                mqtt_broker,mqtt_port = mqtt_broker.split(':')
                mqtt_port = int(mqtt_port)

            if key == "AQ==":
                if debug:
                    print("key is default, expanding to AES128")
                key = "1PG7OiApB1nwvP+rz05pAQ=="

            if not move_text_up():
                return

            padded_key = key.ljust(len(key) + ((4 - (len(key) % 4)) % 4), '=')
            replaced_key = padded_key.replace('-', '+').replace('_', '/')
            key = replaced_key

            if debug:
                print (f"padded & replaced key = {key}")

            setup_db()

            client.username_pw_set(mqtt_username, mqtt_password)
            if mqtt_port == 8883:
                client.tls_set(ca_certs="cacert.pem", tls_version=ssl.PROTOCOL_TLSv1_2)
                client.tls_insecure_set(False)
            client.connect(mqtt_broker, mqtt_port, 60)
            update_console(f"{format_time(current_time())} >>> Connecting to MQTT broker at {mqtt_broker}...", tag="info")

        except Exception as e:
            update_console(f"{format_time(current_time())} >>> Failed to connect to MQTT broker: {str(e)}", tag="info")

    else:
        update_console(f"{format_time(current_time())} >>> Already connected to {mqtt_broker}", tag="info")

def disconnect_mqtt():
    """Disconnect from the MQTT server."""
    if debug:
        print("disconnect_mqtt")
    if client.is_connected():
        client.disconnect()
        update_console(f"{format_time(current_time())} >>> Disconnected from MQTT broker", tag="info")
        if debug:
            print("Disconnected")
    else:
        update_console("Already disconnected", tag="info")

def on_connect(client, userdata, flags, reason_code, properties):
    """Callback when MQTT client connects."""
    set_topic()

    if debug:
        print("on_connect")
        if client.is_connected():
            print("client is connected")

    if reason_code == 0:
        load_message_history_from_db()
        if debug:
            print(f"Subscribe Topics are: {subscribe_topics}")
        
        # Subscribe to all topics
        for topic in subscribe_topics:
            client.subscribe(topic)
            if debug:
                print(f"Subscribed to: {topic}")
        
        topic_list = ", ".join([topic.replace(f"{channel}/#", f"{channel}") for topic in subscribe_topics])
        message = f"{format_time(current_time())} >>> Connected to {mqtt_broker} on topics {topic_list} as {'!' + hex(node_number)[2:]}"
        update_console(message, tag="info")
        send_node_info(BROADCAST_NUM, want_response=False)

    else:
        message = f"{format_time(current_time())} >>> Failed to connect to MQTT broker with result code {str(reason_code)}"
        update_console(message, tag="info")

def on_disconnect(client, userdata, flags, reason_code, properties):
    """Callback when MQTT client disconnects."""
    if debug:
        print("on_disconnect")
    if reason_code != 0:
        message = f"{format_time(current_time())} >>> Disconnected from MQTT broker with result code {str(reason_code)}"
        update_console(message, tag="info")
        if auto_reconnect is True:
            print("attempting to reconnect in " + str(auto_reconnect_delay) + " second(s)")
            time.sleep(auto_reconnect_delay)
            connect_mqtt()

def load_message_history_from_db():
    """Load previously stored messages from sqlite - console version."""
    if debug:
        print("load_message_history_from_db")

    table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_messages"

    try:
        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()
            messages = db_cursor.execute(f'SELECT time, sender_short_name, text_payload, is_encrypted FROM {table_name} ORDER BY time DESC LIMIT 10').fetchall()

            if debug and messages:
                print("Recent message history:")
                for message in reversed(messages):
                    timestamp = format_time(message[0])
                    emoji = encrypted_emoji if message[3] == 1 else ""
                    print(f"  {timestamp} {emoji}{message[1]}: {message[2]}")

    except sqlite3.Error as e:
        print(f"SQLite error in load_message_history_from_db: {e}")
    finally:
        db_connection.close()

def move_text_up():
    """Validate node ID."""
    node_id_str = '!' + hex(node_number)[2:]
    if not is_valid_hex(node_id_str, 8, 8):
        print("Not valid Hex")
        return False
    else:
        return True

def mqtt_thread():
    """Function to run the MQTT client loop in a separate thread."""
    if debug:
        print("MQTT Thread")
        if client.is_connected():
            print("client connected")
        else:
            print("client not connected")
    while True:
        client.loop()

def send_node_info_periodically() -> None:
    """Function to broadcast NodeInfo in a separate thread."""
    while True:
        if client.is_connected():
            send_node_info(BROADCAST_NUM, want_response=False)

        time.sleep(node_info_interval_minutes * 60)

def on_exit():
    """Function to be called when the application is closed."""
    if client.is_connected():
        client.disconnect()
        print("client disconnected")
    
    client.loop_stop()

def update_console(text_payload, tag=None):
    """Print message to console."""
    if debug:
        print(f"[{tag if tag else 'INFO'}] {text_payload}")
    else:
        print(text_payload)

# Global initialization
node_name = '!' + hex(node_number)[2:]
if not is_valid_hex(node_name, 8, 8):
    print('Invalid node name from config: ' + str(node_name))
    sys.exit(1)

global_message_id = random.getrandbits(32)

if __name__ == "__main__":
    print("Meshtastic Fortune Bot")
    print("=====================")
    print(f"Node: {node_name} ({client_short_name})")
    print(f"MQTT Broker: {mqtt_broker}")
    print(f"Channel: {channel}")
    print(f"Root Topics: {', '.join(root_topics)}")
    print("Starting Fortune Bot...")

    # Initialize MQTT client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="", clean_session=True, userdata=None)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    # Start background threads
    mqtt_thread = threading.Thread(target=mqtt_thread, daemon=True)
    mqtt_thread.start()

    node_info_timer = threading.Thread(target=send_node_info_periodically, daemon=True)
    node_info_timer.start()

    # Auto-connect to MQTT
    connect_mqtt()

    # Keep the application running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        on_exit() 