# EVE Caravanserai

![EVE Caravanserai](https://github.com/user-attachments/assets/23f0f9e6-8340-4bc4-bbfb-c82d1aff1701)
_(don't worry, it was a test key)_

A market comparison tool for EVE Online. Compare prices and supply between any two markets — NPC trade hubs, freeport player structures, or any structure your character can access — to find import and export opportunities.

Runs entirely on your own machine. No accounts, no cloud, no subscriptions.

---

## Requirements

- A PC running Windows or Linux
- Python 3.10 or newer
- An internet connection
- An EVE Online account

---

## Step 1 — Install Python

### Windows

1. Go to **https://www.python.org/downloads/** and click the big yellow **Download Python** button
2. Run the installer
3. **Important:** on the first screen, tick the box that says **"Add Python to PATH"** before clicking Install
4. Click **Install Now** and wait for it to finish
5. Open the **Start Menu**, search for **Command Prompt**, and open it
6. Type `python --version` and press Enter — you should see something like `Python 3.12.x`

### Linux

Python is likely already installed. Open a terminal and type:

```bash
python3 --version
```

If you see `Python 3.x.x` you're good. If not, install it with your package manager:

```bash
# Ubuntu / Debian
sudo apt install python3 python3-pip

# Fedora
sudo dnf install python3
```

---

## Step 2 — Set up the tool

Note: If you know what you're doing and are fine with system wide dependency installs, you can skip the venv virtual environment and pip install -r Requirements.txt just run the script as is.

### Windows

1. Unzip the **EVE Caravanserai** folder somewhere easy to find (e.g. your Desktop or Documents)
2. Open the folder
3. In the address bar at the top of the window, click once so it turns blue, type `cmd`, and press Enter — this opens a Command Prompt inside that folder
4. Type the following commands one at a time, pressing Enter after each:

```
python -m venv venv
venv\Scripts\activate
pip install -r Requirements.txt
```

You should see `(venv)` appear at the start of your prompt. That means the virtual environment is active.

### Linux

1. Unzip or copy the **EVE Caravanserai** folder somewhere convenient
2. Open a terminal and navigate into the folder:

```bash
cd ~/path/to/EVE\ Caravanserai
python3 -m venv venv
source venv/bin/activate
pip install -r Requirements.txt
```

You should see `(venv)` appear at the start of your prompt.

---

## Step 3 — Create an EVE Developer Application

EVE Caravanserai needs permission to read market data from your EVE character. You grant this by registering a free application on CCP's developer portal. This takes about two minutes.

1. Go to **https://developers.eveonline.com/** and log in with your EVE account
2. Click **Create New Application**
3. Fill in the form:
   - **Name:** anything you like, e.g. `Caravanserai`
   - **Description:** anything, e.g. `Personal market tool`
   - **Connection Type:** select **Authentication & API Access**
   - **Callback URL:** enter exactly: `http://127.0.0.1:8182/callback`
   - **Scopes:** add all of the following by searching for each one and clicking it:
     - `publicData`
     - `esi-universe.read_structures.v1`
     - `esi-search.search_structures.v1`
     - `esi-markets.structure_markets.v1`
     - `esi-ui.open_window.v1`
     - `esi-markets.read_character_orders.v1`
     - `esi-markets.read_corporation_orders.v1`
4. Click **Create Application**
5. On the next page, copy your **Client ID** — a long string of letters and numbers. Keep it handy for Step 5.

---

## Step 4 — Run the tool

Make sure your virtual environment is still active (`(venv)` shows in your prompt). If you opened a new terminal, re-run the activate command from Step 2 first.

### Windows
```
python app.py
```

If it fails, try adding python to your system path variables (google it or install Linux instead 🤡).

### Linux
```bash
python3 app.py
```

You'll see some startup messages. Open your browser and go to:

**http://127.0.0.1:8182**

The tool will spend the first minute downloading universe data in the background — you'll see a spinner in the Universe panel that turns into a green checkmark when ready. You can continue with Steps 5 and 6 while it downloads.

---

## Step 5 — Enter your Client ID

In the left sidebar, find the **ESI Client ID** panel. Paste the Client ID you copied in Step 3 into the input field and click **SAVE**. A green confirmation will appear.

---

## Step 6 — Log in with your EVE character

Once your Client ID is saved, a **⬡ LOGIN** button will appear in the **EVE Character** panel. Click it. A browser window will open taking you to the EVE Online login page. Log in with your EVE account and authorise the application when prompted.

The window will close automatically and your character name will appear in the sidebar.

---

## Step 7 — Compare markets

Once the universe data has loaded and your character is logged in, the market panels will become active.

1. **Pick a Source market** — choose from the NPC Hubs tab (Jita, Amarr, Dodixie, etc.), the Freeports tab, or search for a player structure in the Exclusive Outposts tab
2. **Pick a Destination market** — same process on the other panel
3. **Click FETCH** on each panel to download today's market data for that location
4. **Click COMPARE MARKETS** to run the comparison

Note: If you already fetched data for any of the specific structures, the data persists for that day through runs of the script, no need to worry about re-fetching.
Note2: You can swap source and destination on the fly (left panel) to instantly swap import/export values based on your needs.

The table shows price, supply, demand, spread, and import/export margins for every item traded at either location. Use the filters at the top to narrow by name, category, or group. The **⇄** button between the panels swaps source and destination and instantly recalculates margins without re-fetching.

To export what you see to a spreadsheet, click **⬇ CSV**.

---

## Everyday use

Your Client ID and character login are remembered between sessions. Next time:

1. Run `python app.py` (Windows: `python app.py`, Linux: `python3 app.py`)
2. Open **http://127.0.0.1:8182** in your browser of choice.
3. Click FETCH on each market for fresh data, then COMPARE

Market data is cached **DAILY** — fetching the same market twice in one day skips the download and confirms the existing data.

---

## Stopping the tool

Press **Ctrl+C** in the terminal window where `app.py` is running.

---

## Troubleshooting

**The LOGIN button doesn't appear**
Save your Client ID first. The LOGIN button only shows once a Client ID is present.

**"Not authenticated" error when fetching a structure**
Your session may have expired. Click LOGOUT and log in again.

**A freeport shows "No orders found"**
Some freeports go offline or change ownership over time. Try a different one or search for a structure in that system manually.

**Can't find a player structure in a system**
Your character needs docking access to that structure for it to appear. The owning corporation may have restricted access.

**Login popup closes but character doesn't appear**
The Callback URL in your developer application must be exactly `http://127.0.0.1:8182/callback` — not `localhost`, not with a trailing slash.

**I can't connect to the app, but everything seems fine**
It could be a port issue. Make sure you don't have any other apps using port 8182. I should probably add a routine to do this automatically at some point, but I'm lazy so I'll only do this if anyone actually has this issue. Contact Jah'bastah II ingame if you have any issues not found in the README.

---

## Privacy

Everything runs locally. Your Client ID, character tokens, and all market data are stored only in `caravanserai.db` on your own computer. Nothing is sent anywhere except CCP's official ESI API (`esi.evetech.net`) and your calls to the Fuzzworks market data service (`market.fuzzwork.co.uk`).

## Upcoming Features

- Data legend redesign.

# Known Issues

- Remnant/exotic skin items not classified under the normal skin item group still appearing
