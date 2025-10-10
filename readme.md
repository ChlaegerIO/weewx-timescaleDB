# weewx-timescaleDB (tsdb)

This Weewx driver plugin synchronizes data from the Weewx database to a postgres / TimescaleDB database locally on the device. Every new record is added to the tsdb, as well as not yet synchronized older records. In the initialization of the tsdb class one can change the disired columns that should be synchronized.

## Pre-Requisites 

Install the [TimescaleDB](https://docs.tigerdata.com/self-hosted/latest/install/installation-linux/) extension for postgres
```bash
sudo apt install gnupg postgresql-common apt-transport-https lsb-release wget
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
echo "deb https://packagecloud.io/timescale/timescaledb/debian/ $(lsb_release -c -s) main" | sudo tee /etc/apt/sources.list.d/timescaledb.list
wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/timescaledb.gpg
sudo apt update
sudo apt install timescaledb-2-postgresql-17 postgresql-client-17
```

Optional: Install Grafana for Visualization


## Installation

0) Install pre-requisites

    a) install [weewx](http://weewx.com/docs)
      
    b) install [timescaleDB](https://docs.tigerdata.com/self-hosted/latest/install/installation-linux/)

1) Install the tsdb driver
    ```bash
    weectl extension install https://github.com/ChlaegerIO/weewx-timescaleDB/archive/master.zip
    ```

2) Configure the driver
    ```bash
    weectl station reconfigure --driver=user.tsdb --no-prompt
    ```

3) Run the driver directly to identify the packets you want to capture

    ```bash
    export WEEWX_BINDIR=/usr/share/weewx
    export WEEWX_USRDIR=/etc/weewx/bin/user
    PYTHONPATH=$WEEWX_BINDIR python3 $WEEWX_USRDIR/tsdb.py
    ```

4) Add or modify the [TimescaleDBSync] section in weewx.conf

    ```bash
    [TimescaleDBSync]
        
        # Configuration
        host = localhost
        port = 5432
        database = weather_data
        user = postgres
        password = password
        enable_daily_sync = true
    ```

5) Start weewx
    ```bash
    sudo systemctl start weewx
    ```

## Author
Copyright 2025 Timo Kleger

Distributed under terms of the GPLv3
