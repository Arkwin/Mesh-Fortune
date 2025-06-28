## Mesh Fortune - A Fortune Cookie Bot for Meshtastic Networks!

Many thanks to and inspiration from:<br>
 https://github.com/arankwende/meshtastic-mqtt-client & https://github.com/joshpirihi/meshtastic-mqtt & https://github.com/pdxlocations/connect<br>
<b>Powered by Meshtastic.org</b>

### Installation

Create a virtual environment and install Mesh Fortune:
```bash
python3 -m venv mesh-fortune
cd mesh-fortune && source bin/activate
pip3 install meshtastic paho-mqtt cryptography
git clone https://github.com/Arkwin/Mesh-Fortune.git
cd Mesh-Fortune
```

### Running the Fortune Bot
```bash
python mqtt-connect.py
```

### How It Works

## Mesh Fortune System

This system provides a fun fortune cookie bot for Meshtastic users across multiple regions. When someone sends a direct message to your fortune bot node, it automatically replies with a random fortune from the `fortunes.txt` file.

### Getting Your Fortune

**Getting a Fortune:**
Simply send any direct message to the fortune bot node and you'll receive a random fortune in response!

### Fortune Database

The bot reads from `fortunes.txt` which contains:
- Inspirational quotes
- Funny sayings  
- Motivational messages
- Random wisdom
- Silly observations

You can customize the fortunes by editing the `fortunes.txt` file - just put one fortune per line.

### Multi-Region Support

The fortune bot automatically works across different Meshtastic regions:
- **Supported Regions:** Configurable via `config.ini`
- **Cross-Region Delivery:** Fortunes sent across different mesh regions
- **Smart Routing:** System remembers where users were last seen for efficient delivery

### Configuration

Edit `config.ini` to customize:
- **MQTT Settings:** Broker, credentials, topics
- **Node Information:** Your node number, names, location  
- **Regional Topics:** Add multiple root topics for cross-region support
- **Debug Options:** Detailed logging and message tracking

### Database Storage

- Node information is cached for faster lookups
- Message history is maintained for debugging
- Automatic database setup on first run
- Position tracking (optional)

### Features

- 🔮 **Random Fortunes** - Picks from 90+ different fortunes
- 🌐 **Cross-Region Delivery** - Works across multiple Meshtastic regions  
- ⚡ **Instant Response** - Replies immediately to any direct message
- 🎯 **Smart Routing** - Automatic routing to correct regional topics
- 📝 **Customizable** - Edit fortunes.txt to add your own messages
- 🔧 **Multi-Topic Support** - Configurable for different mesh regions
- 📊 **Database Tracking** - Logs interactions and node information

### Example Fortunes

Here are some examples of what the bot might send:
- "Nothing is impossible. Except Monday mornings."
- "A comfort zone is a magical place where nothing ever grows."
- "Go confidently in the direction of your dreams."
- "Curiosity kills boredom. Nothing can kill curiosity."
- "Better ask twice than lose yourself once."

### Adding Your Own Fortunes

To add custom fortunes:
1. Open `fortunes.txt` in a text editor
2. Add one fortune per line
3. Save the file
4. Restart the bot to load new fortunes

The bot will randomly select from all available fortunes each time someone messages it!
