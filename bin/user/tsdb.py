#!/usr/bin/env python
# Copyright 2025 Timo Kleger
# Distributed under the terms of the GNU Public License (GPLv3)

import weewx
from weewx.engine import StdService
import sqlite3
import psycopg2
import logging
import time

VERSION = "0.1"
WEEWX_VERSION = "4"

if weewx.__version__ < WEEWX_VERSION:
    raise weewx.UnsupportedFeature("weewx %s or greater is required, found %s"
                                   % (WEEWX_VERSION, weewx.__version__))

import weeutil.logger
log = logging.getLogger(__name__)

class TimescaleDBSync(StdService):
    """A WeeWX service that syncs archive records to a TimescaleDB database."""

    def __init__(self, engine, config_dict):
        # Store daily archive tables as an instance variable
        self.daily_archive_tables = [
            "archive_day_altimeter", "archive_day_appTemp1", "archive_day_barometer",
            "archive_day_cloudBase", "archive_day_dewpoint", "archive_day_ET", "archive_day_heatindex",
            "archive_day_humidex", "archive_day_inDewpoint", "archive_day_inHumidity", "archive_day_inTemp",
            "archive_day_outHumidity", "archive_day_outTemp", "archive_day_pressure", "archive_day_rain",
            "archive_day_rainRate", "archive_day_windChill", "archive_day_windDir", "archive_day_windGust",
            "archive_day_windGustDir", "archive_day_windRun", "archive_day_windSpeed"
        ]
        # Ensure 'synced' column exists in archive and daily tables
        try:
            db = config_dict['DataBindings']['wx_binding']['database']
            db_path = f"/var/lib/weewx/{config_dict['Databases'][db]['database_name']}"
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(archive)")
            columns = [row[1] for row in cursor.fetchall()]
            if 'synced' not in columns:
                cursor.execute("ALTER TABLE archive ADD COLUMN synced INTEGER DEFAULT 0")
                conn.commit()
            # Add 'synced' column to daily tables if missing
            for table in self.daily_archive_tables:
                cursor.execute(f"PRAGMA table_info({table})")
                columns = [row[1] for row in cursor.fetchall()]
                if 'synced' not in columns:
                    cursor.execute(f"ALTER TABLE {table} ADD COLUMN synced INTEGER DEFAULT 0")
                    conn.commit()
            conn.close()
        except Exception as e:
            log.error(f"Error ensuring 'synced' column exists: {e}")
            print(f"Error ensuring 'synced' column exists: {e}")
        super().__init__(engine, config_dict)
        
        # Get configuration
        try:
            self.host = config_dict['TimescaleDBSync'].get('host', 'localhost')
            self.port = config_dict['TimescaleDBSync'].get('port', 5432)
            self.database = config_dict['TimescaleDBSync'].get('database', 'weather_data_test')
            self.user = config_dict['TimescaleDBSync'].get('user', 'postgres')
            self.password = config_dict['TimescaleDBSync'].get('password', '')
            self.enable_daily_sync = config_dict['TimescaleDBSync'].get('enable_daily_sync', False)
            self.tsdb_config = {
                'host': self.host,
                'port': self.port,
                'database': self.database,
                'user': self.user,
                'password': self.password
            }
        except KeyError as e:
            log.info(f"Missing TimescaleDB configuration: %s", e)
            print(f"Missing TimescaleDB configuration: {e}")
        else:
            self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)
            log.info("TimescaleDB database: %s", self.database)
            print(f"TimescaleDB database: {self.database}")

    def new_archive_record(self, event):
        """Gets called on a new archive record event."""
        record = event.record
        
        # Synchronize to TimescaleDB
        try:
            self.tsdb_conn = psycopg2.connect(**self.tsdb_config)
            self._insert_tsdb("archive", record)
        except Exception as e:
            log.error(f"Error connecting and synchronizing to TimescaleDB: %s", e)
            print(f"Error connecting and synchronizing to TimescaleDB: {e}")
        finally:
            if hasattr(self, 'tsdb_conn'):
                self.tsdb_conn.close()

        # Check for older records in the wewx database that haven't been synchronized yet
        try:
            db = self.config_dict['DataBindings']['wx_binding']['database']
            db_path = f"/var/lib/weewx/{self.config_dict['Databases'][db]['database_name']}"
            self.weewx_conn = sqlite3.connect(db_path)            
            cursor = self.weewx_conn.cursor()
            cursor.execute("SELECT * FROM archive WHERE synced IS NULL OR synced = 0 ORDER BY dateTime ASC")
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            for row in rows:
                old_record = dict(zip(columns, row))
                self.tsdb_conn = psycopg2.connect(**self.tsdb_config)
                self._insert_tsdb("archive", old_record)
                # Mark as synced
                cursor.execute("UPDATE archive SET synced = 1 WHERE dateTime = ?", (old_record['dateTime'],))
                self.weewx_conn.commit()
        except Exception as e:
            log.error(f"Error synchronizing old records: %s", e)
            print(f"Error synchronizing old records: {e}")
        finally:
            if hasattr(self, 'weewx_conn'):
                self.weewx_conn.close()
            if hasattr(self, 'tsdb_conn'):
                self.tsdb_conn.close()

        # Check daily archives if enabled
        if self.enable_daily_sync:
            self._sync_daily_archives()


    def _insert_tsdb(self, table, record):
        cur = self.tsdb_conn.cursor()
        try:
            # Create table if it does not exist
            col_defs = ', '.join([f'{col} DOUBLE PRECISION' if col != 'dateTime' else 'dateTime INTEGER PRIMARY KEY' for col in record.keys()])
            create_sql = f"CREATE TABLE IF NOT EXISTS {table} ({col_defs})"
            cur.execute(create_sql)

            columns = ','.join(record.keys())
            placeholders = ','.join(['%s'] * len(record))
            values = tuple(record.values())
            cur.execute(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", values)
        except Exception as e:
            log.error("Error inserting record into TimescaleDB: %s", e)
            print(f"Error inserting record into TimescaleDB: {e}")
        finally:
            cur.close()

    def _sync_daily_archives(self):
        """Synchronize daily archives from WeeWX to TimescaleDB."""
        for measurements in self.daily_archive_tables:
            try:
                self.weewx_conn = sqlite3.connect(self.engine.db_binder.db_name)
                cursor = self.weewx_conn.cursor()
                cursor.execute(f"SELECT * FROM {measurements} WHERE synced IS NULL OR synced = 0 ORDER BY dateTime ASC")
                rows = cursor.fetchall()
                columns = [description[0] for description in cursor.description]
                for row in rows:
                    daily_record = dict(zip(columns, row))
                    self.tsdb_conn = psycopg2.connect(**self.tsdb_config)
                    self._insert_tsdb(measurements, daily_record)
                    # Mark as synced
                    cursor.execute(f"UPDATE {measurements} SET synced = 1 WHERE dateTime = ?", (daily_record['dateTime'],))
                    self.weewx_conn.commit()
            except Exception as e:
                log.error(f"Error synchronizing daily archives from {measurements}: {e}")
            finally:
                if hasattr(self, 'weewx_conn'):
                    self.weewx_conn.close()
                if hasattr(self, 'tsdb_conn'):
                    self.tsdb_conn.close()

if __name__ == "__main__":
    """This section is used to test tsdb.py."""
    from optparse import OptionParser
    import weecfg
    import weeutil.logger

    usage = """Usage: python tsdb.py --help
       python tsdb.py [CONFIG_FILE] [--config=CONFIG_FILE]
       
    Arguments:
       CONFIG_FILE   Path to weewx.conf. Default is /etc/weewx/weewx.conf"""

    epilog = """You must be sure the WeeWX modules are in your PYTHONPATH. 
    For example:

    PYTHONPATH=/home/weewx/bin python alarm.py --help"""

    # force debug
    weewx.debug = 1

    # Create a command line parser:
    parser = OptionParser(usage=usage, epilog=epilog)
    parser.add_option("--config", dest="config_path", metavar="CONFIG_FILE",
                      help="Use configuration file CONFIG_FILE.")
    # Parse the arguments and options
    (options, args) = parser.parse_args()

    try:
        config_path, config_dict = weecfg.read_config(options.config_path, args)
    except IOError as e:
        exit("Unable to open configuration file: %s" % e)

    print("Using configuration file %s" % config_path)

    # Set logging configuration:
    weeutil.logger.setup('timescaledb', config_dict)

    if 'TimescaleDBSync' not in config_dict:
        exit("No [TimescaleDBSync] section in the configuration file %s" % config_path)

    # This is a fake record that we'll use
    rec = {'extraTempTest': 40.1,
           'dateTime': int(time.time())}

    # We need the main WeeWX engine in order to bind to the event, 
    # but we don't need for it to completely start up. So get rid of all
    # services:
    config_dict['Engine']['Services'] = {}
    # Now we can instantiate our slim engine, using the DummyEngine class...
    engine = weewx.engine.DummyEngine(config_dict)
    # ... and set the alarm using it.
    sync = TimescaleDBSync(engine, config_dict)

    # Create a NEW_ARCHIVE_RECORD event
    event = weewx.Event(weewx.NEW_ARCHIVE_RECORD, record=rec)

    # Use it to trigger the sync:
    sync.new_archive_record(event)
