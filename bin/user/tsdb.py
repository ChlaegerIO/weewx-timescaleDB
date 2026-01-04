#!/usr/bin/env python
# Copyright 2025 Timo Kleger
# Distributed under the terms of the GNU Public License (GPLv3)

import weewx
from weewx.engine import StdService
import sqlite3
import psycopg2
import logging
import time
import os

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
        """Initialize the TimescaleDB service."""
        super().__init__(engine, config_dict)

        # daily archive tables we want to sync
        self.daily_archive_tables = [
            "archive_day_altimeter", "archive_day_appTemp1", "archive_day_barometer",
            "archive_day_cloudBase", "archive_day_dewpoint", "archive_day_ET", "archive_day_heatindex",
            "archive_day_humidex", "archive_day_inDewpoint", "archive_day_inHumidity", "archive_day_inTemp",
            "archive_day_outHumidity", "archive_day_outTemp", "archive_day_pressure", "archive_day_rain",
            "archive_day_rainRate", "archive_day_windChill", "archive_day_windDir", "archive_day_windGust",
            "archive_day_windGustDir", "archive_day_windRun", "archive_day_windSpeed"
        ]
        # Columns in the archive table we want to sync, adjustable before postgres tsdb initialization
        self.archive_columns = [
            "dateTime", "usUnits", "interval", "altimeter", "appTemp", "appTemp1", "barometer", 
            "batteryStatus1", "batteryStatus2", "cloudBase", "co", "co2", "consBatteryVoltage", "dewpoint", 
            "ET", "extraHumid1", "extraHumid2", "extraTemp1", "extraTemp2", "forecast", "hail", "hailRate", 
            "heatindex", "heatIndex1", "heatingTemp", "heatingVoltage", "humIndex", "humIndex1", "inDewpoint", 
            "inHumidity", "inTemp", "leafTemp1", "leafTemp2", "leafWet1", "leafWet2", "lightningDistance", 
            "lightningDisturberCount", "lightningEnergy", "lightningNoiseCount", "lightningStrikeCount", 
            "luminosity", "maxSolarRad", "nh3", "no2", "noise", "o3", "outHumidity", "outTemp", "pb", "pm10", 
            "pm1", "pm2_5", "pressure", "radiation", "rain", "rainRate", "rxCheckPercent", "snow", "snowDepth", 
            "snowMoisture", "snowRate", "so2", "soilMoist1", "soilMoist2", "soilTemp1", "soilTemp2", 
            "txBatteryStatus", "uv", "windChill", "windDir", "windGust", "windGustDir", "windRun", "windSpeed",
            "highOutTemp", "lowOutTemp", "forecastRule", "windSpeed10", "dayRain", "monthRain", "yearRain", 
            "stormRain", "dayET", "monthET", "yearET", "forecastIcon", "sunrise", "sunset", "wind_samples"
        ]
        
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
            
        # Initialize paths and dataset
        try:
            db = config_dict['DataBindings']['wx_binding']['database']
            db_path = f"/var/lib/weewx/{config_dict['Databases'][db]['database_name']}"
            self.weewx_db_path = db_path
            self.sync_db_path = f"/var/lib/weewx/syncTsdb.sdb"
            # Initialize synchronization DB and postgres ts database
            self._init_sync_db()
        except Exception as e:
            log.error(f"Error initialization: {e}")
        else:
            self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)

    def new_archive_record(self, event):
        """Gets called on a new archive record event."""
        record = event.record
        
        # Synchronize to TimescaleDB
        try:
            self.tsdb_conn = psycopg2.connect(**self.tsdb_config)
            self._insert_tsdb("archive", record)
            # Mark this new record as synced in the sync DB
            sconn = sqlite3.connect(self.sync_db_path)
            scur = sconn.cursor()
            scur.execute("INSERT OR REPLACE INTO synced_archive (dateTime, synced) VALUES (?, 1)", (record.get('dateTime'),))
            sconn.commit()
            scur.close()
            sconn.close()
        except Exception as e:
            log.error(f"Error synchronizing TimescaleDB archive: %s", e)
        finally:
            if hasattr(self, 'tsdb_conn'):
                self.tsdb_conn.close()

        # Check for older records in the weewx database that haven't been synchronized yet
        try:
            self.weewx_conn = sqlite3.connect(self.weewx_db_path)
            cursor = self.weewx_conn.cursor()

            # Attach sync database to allow cross-database queries
            cursor.execute(f"ATTACH DATABASE '{self.sync_db_path}' AS sync_db")

            # Select only records that are NOT in the synced_archive table
            # This avoids loading the entire archive table into memory
            cursor.execute("""
                SELECT * FROM archive 
                WHERE dateTime NOT IN (SELECT dateTime FROM sync_db.synced_archive WHERE synced = 1)
                ORDER BY dateTime ASC
            """)
            rows = cursor.fetchall()

            if rows:
                columns = [description[0] for description in cursor.description]
                # filter columns that we want to insert
                filtered_columns = [col for col in columns if col in self.archive_columns]

                # Open connections once for the batch
                try:
                    self.tsdb_conn = psycopg2.connect(**self.tsdb_config)
                    sconn = sqlite3.connect(self.sync_db_path)
                    scur = sconn.cursor()
                    for row in rows:
                        filtered_row = [row[columns.index(col)] for col in filtered_columns]
                        old_record = dict(zip(filtered_columns, filtered_row))
                        dt = old_record['dateTime']

                        try:
                            self._insert_tsdb("archive", old_record)
                            # Mark as synced in sync DB
                            scur.execute("INSERT OR REPLACE INTO synced_archive (dateTime, synced) VALUES (?, 1)", (dt,))
                        except Exception as e:
                            log.error(f"Error syncing older record {dt} to TimescaleDB: %s", e)

                    sconn.commit()
                    scur.close()
                    sconn.close()
                except Exception as e:
                    log.error(f"Error during batch sync to TimescaleDB: %s", e)
                finally:
                    if hasattr(self, 'tsdb_conn'):
                        self.tsdb_conn.close()

        except Exception as e:
            log.error(f"Error synchronizing old records: %s", e)
        finally:
            if hasattr(self, 'weewx_conn'):
                self.weewx_conn.close()

        # Check daily archives if enabled
        if self.enable_daily_sync:
            log.info(f"Daily archive sync is {'enabled' if self.enable_daily_sync else 'disabled'}.")
            self._sync_daily_archives()


    def _insert_tsdb(self, table, record):
        cur = self.tsdb_conn.cursor()
        try:
            # Log the keys we're receiving
            # log.info(f"Record keys for table {table}: {list(record.keys())}")
            
            # Create table if it does not exist
            col_defs = ', '.join([f'{col} DOUBLE PRECISION' if col != 'dateTime' else 'dateTime INTEGER PRIMARY KEY' for col in record.keys()])
            create_sql = f"CREATE TABLE IF NOT EXISTS {table} ({col_defs})"
            cur.execute(create_sql)
            self.tsdb_conn.commit()

            filtered_columns = [col for col in record.keys() if col in self.archive_columns or col in ['dateTime', 'usUnits', 'interval']]
            filtered_record = {col: record[col] for col in filtered_columns}
            columns = ','.join(filtered_record.keys())
            placeholders = ','.join(['%s'] * len(filtered_record))
            values = tuple(filtered_record.values())
            insert_sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
            cur.execute(insert_sql, values)
            self.tsdb_conn.commit()
            log.info(f"Inserted data into TimescaleDB {table} at time {record.get('dateTime')}")
        except Exception as e:
            log.error("Error inserting record into TimescaleDB %s: %s", table, e)
        finally:
            cur.close()

    def _sync_daily_archives(self):
        """Synchronize daily archives from WeeWX to TimescaleDB."""
        for measurements in self.daily_archive_tables:
            try:
                self.weewx_conn = sqlite3.connect(self.weewx_db_path)
                cursor = self.weewx_conn.cursor()

                # Attach sync database
                cursor.execute(f"ATTACH DATABASE '{self.sync_db_path}' AS sync_db")

                # Select records not in synced_archive_day
                cursor.execute(f"""
                    SELECT * FROM {measurements} 
                    WHERE dateTime NOT IN (SELECT dateTime FROM sync_db.synced_archive_day WHERE synced = 1)
                    ORDER BY dateTime ASC
                """)
                rows = cursor.fetchall()

                if rows:
                    columns = [description[0] for description in cursor.description]

                    try:
                        self.tsdb_conn = psycopg2.connect(**self.tsdb_config)
                        sconn = sqlite3.connect(self.sync_db_path)
                        scur = sconn.cursor()
                        for row in rows:
                            daily_record = dict(zip(columns, row))
                            dt = daily_record['dateTime']
                            
                            try:
                                self._insert_tsdb(measurements, daily_record)
                                # Mark day as synced
                                scur.execute("INSERT OR REPLACE INTO synced_archive_day (dateTime, synced) VALUES (?, 1)", (dt,))
                            except Exception as e:
                                log.error(f"Error syncing daily record {dt} from {measurements}: {e}")
                        
                        sconn.commit()
                        scur.close()
                        sconn.close()
                    except Exception as e:
                        log.error(f"Error during batch sync for {measurements}: {e}")
                    finally:
                        if hasattr(self, 'tsdb_conn'):
                            self.tsdb_conn.close()

            except Exception as e:
                log.error(f"Error synchronizing daily archives from {measurements}: {e}")
            finally:
                if hasattr(self, 'weewx_conn'):
                    self.weewx_conn.close()

    def _init_sync_db(self):
        """Create the sync DB file and required tables if they do not exist."""
        try:
            # Create sync database if it does not exist
            db_dir = os.path.dirname(self.sync_db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
            conn = sqlite3.connect(self.sync_db_path)
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS synced_archive (
                    dateTime INTEGER PRIMARY KEY,
                    synced INTEGER DEFAULT 0
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS synced_archive_day (
                    dateTime INTEGER PRIMARY KEY,
                    synced INTEGER DEFAULT 0
                )
            """)
            conn.commit()
            cur.close()
            conn.close()
            log.info("Synchronization database at %s", self.sync_db_path)
        except Exception as e:
            log.error("Failed to ensure sync DB/tables: %s", e)
        
        # Create postgres TimescaleDB database with archive and daily_archive_xxx if it does not exist
        try:
            tsdb_conn = psycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.password)
            tsdb_conn.autocommit = True
            tsdb_cur = tsdb_conn.cursor()
            
            # Check if database exists
            tsdb_cur.execute(f"SELECT 1 FROM pg_database WHERE datname = %s", (self.database,))
            db_exists = tsdb_cur.fetchone()
            if not db_exists:
                tsdb_cur.execute(f"CREATE DATABASE {self.database}")
                log.info(f"Created TimescaleDB database {self.database}")
            
            # Close connection to postgres db and connect to our database
            tsdb_cur.close()
            tsdb_conn.close()
            
            # Connect to our specific database
            tsdb_conn = psycopg2.connect(host=self.host, port=self.port, user=self.user, 
                                       password=self.password, database=self.database)
            tsdb_conn.autocommit = True
            tsdb_cur = tsdb_conn.cursor()
            
            # Enable TimescaleDB extension in our database
            tsdb_cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
            log.info("Enabled TimescaleDB extension")
            
            # Check if archive table exists
            tsdb_cur.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'archive')")
            table_exists = tsdb_cur.fetchone()[0]
            
            if not table_exists:
                # Create base archive table with essential columns first
                tsdb_cur.execute("""
                    CREATE TABLE IF NOT EXISTS archive (
                        datetime INTEGER,
                        usunits INTEGER,
                        interval INTEGER
                    )
                """)
                tsdb_conn.commit()
                
                # Convert to hypertable tables
                try:
                    tsdb_cur.execute("SELECT create_hypertable('archive', 'datetime', if_not_exists => TRUE, chunk_time_interval => 86400)")
                    tsdb_conn.commit()
                    log.info(f"Created hypertable archive")
                except Exception as e:
                    log.error(f"Error converting archive table to hypertable: {e}")

                # Add remaining columns from self.archive_columns
                for column in self.archive_columns:
                    if column in ['datetime', 'usunits', 'interval']:
                        continue  # Skip columns we already created
                    try:
                        tsdb_cur.execute(f"ALTER TABLE archive ADD COLUMN IF NOT EXISTS {column} DOUBLE PRECISION")
                        tsdb_conn.commit()
                    except Exception as e:
                        log.error(f"Error adding column {column} to archive table: {e}")

                # Create and convert daily summary tables
                for table in self.daily_archive_tables:
                    try:
                        # Create table
                        tsdb_cur.execute(f"""
                            CREATE TABLE IF NOT EXISTS {table} (
                                datetime INTEGER,
                                min DOUBLE PRECISION,
                                mintime INTEGER,
                                max DOUBLE PRECISION,
                                maxtime INTEGER,
                                sum DOUBLE PRECISION,
                                count INTEGER,
                                wsum DOUBLE PRECISION,
                                sumtime INTEGER
                            )
                        """)

                        # Convert to hypertable
                        tsdb_cur.execute(f"SELECT create_hypertable('{table}', 'datetime', if_not_exists => TRUE, chunk_time_interval => 86400)")
                        log.info(f"Created hypertable {table}")
                    except Exception as e:
                        log.error(f"Error creating hypertable {table} failed: {e}")

            tsdb_cur.close()
            tsdb_conn.close()
            log.info("Completed TimescaleDB initialization")
        except Exception as e:
            log.error(f"Failed to initialize TimescaleDB database: {e}")

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

    log.info("Using configuration file %s", config_path)

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
