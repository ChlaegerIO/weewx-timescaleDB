#!/usr/bin/env python
# Copyright 2025 Timo Kleger
# Distributed under the terms of the GNU Public License (GPLv3)

import weewx
from weewx.engine import StdService
import sqlite3
import psycopg2
import logging

VERSION = "0.1"
WEEWX_VERSION = "4"

if weewx.__version__ < WEEWX_VERSION:
    raise weewx.UnsupportedFeature("weewx %s or greater is required, found %s"
                                   % (WEEWX_VERSION, weewx.__version__))

import weeutil.logger
log = logging.getLogger(__name__)
def logdbg(msg): log.debug(msg)
def loginf(msg): log.info(msg)
def logerr(msg): log.error(msg)

class TimescaleDBSync(StdService):
    def __init__(self, engine, config_dict):
        super(TimescaleDBSync, self).__init__(engine, config_dict)
        self.tsdb_config = config_dict.get('TimescaleDB', {})
        self.sqlite_path = config_dict.get('DataBindings', {}).get('wx_binding', {}).get('database', 'weewx.sdb')
        self.tsdb_conn = self._connect_tsdb()
        self.sqlite_conn = sqlite3.connect(self.sqlite_path)
        self.archive_manager = weewx.manager.Manager(config_dict['DataBindings']['wx_binding'], self.sqlite_conn)
        self.schema = self.archive_manager.getSqlColumns()
        self.last_tsdb_ts = self._get_last_tsdb_timestamp()
        self._bulk_sync_if_needed()
        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)

    def _connect_tsdb(self):
        return psycopg2.connect(
            dbname=self.tsdb_config.get('database', 'weather_data_test'),
            user=self.tsdb_config.get('user', 'postgres'),
            password=self.tsdb_config.get('password', ''),
            host=self.tsdb_config.get('host', 'localhost'),
            port=self.tsdb_config.get('port', 5432)
        )

    def _get_last_tsdb_timestamp(self):
        cur = self.tsdb_conn.cursor()
        try:
            cur.execute("SELECT MAX(dateTime) FROM weewx_archive")
            result = cur.fetchone()
            return result[0] if result and result[0] else 0
        except Exception as e:
            logerr(f"Error fetching last timestamp from TimescaleDB: {e}")
            return 0
        finally:
            cur.close()

    def _bulk_sync_if_needed(self):
        cur = self.sqlite_conn.cursor()
        try:
            columns = ','.join(self.schema)
            cur.execute(f"SELECT {columns} FROM archive WHERE dateTime > ?", (self.last_tsdb_ts,))
            rows = cur.fetchall()
            loginf(f"Bulk sync: {len(rows)} rows to insert")
            for row in rows:
                record = dict(zip(self.schema, row))
                self._insert_tsdb(record)
            self.tsdb_conn.commit()
        except Exception as e:
            logerr(f"Error during bulk sync: {e}")
        finally:
            cur.close()

    def new_archive_record(self, event):
        record = event.record
        try:
            self._insert_tsdb(record)
            self.tsdb_conn.commit()
        except Exception as e:
            logerr(f"Error inserting new archive record: {e}")

    def _insert_tsdb(self, record):
        cur = self.tsdb_conn.cursor()
        try:
            columns = ','.join(record.keys())
            placeholders = ','.join(['%s'] * len(record))
            values = tuple(record.values())
            cur.execute(f"INSERT INTO weewx_archive ({columns}) VALUES ({placeholders})", values)
        except Exception as e:
            logerr(f"Error inserting record into TimescaleDB: {e}")
        finally:
            cur.close()

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

    if 'TimescaleDB' not in config_dict:
        exit("No [TimescaleDB] section in the configuration file %s" % config_path)

    # This is a fake record that we'll use
    rec = {'extraTemp1': 1.0,
           'outTemp': 38.2,
           'dateTime': int(time.time())}

    # We need the main WeeWX engine in order to bind to the event, 
    # but we don't need for it to completely start up. So get rid of all
    # services:
    config_dict['Engine']['Services'] = {}
    # Now we can instantiate our slim engine, using the DummyEngine class...
    engine = weewx.engine.DummyEngine(config_dict)
    # ... and set the alarm using it.
    alarm = MyAlarm(engine, config_dict)

    # Create a NEW_ARCHIVE_RECORD event
    event = weewx.Event(weewx.NEW_ARCHIVE_RECORD, record=rec)

    # Use it to trigger the alarm:
    alarm.new_archive_record(event)


    parser = optparse.OptionParser(usage=usage)
    parser.add_option('--host', default='localhost',
                      help="TimescaleDB host. Default is 'localhost'",
                      metavar="HOST")
    parser.add_option('--port', default=5432, type="int",
                      help="TimescaleDB port. Default is 5432",
                      metavar="PORT")
    parser.add_option('--user', default='weewx',
                      help="TimescaleDB user. Default is 'weewx'",
                      metavar="USER")
    parser.add_option('--password', default='',
                      help="TimescaleDB password. Default is ''",
                      metavar="PASSWORD")
    parser.add_option('--database', default='weewx',
                      help="TimescaleDB database name. Default is 'weewx'",
                      metavar="DBNAME")
    (options, args) = parser.parse_args()

    config_dict = {
        'DataBindings': {
            'wx_binding': {
                'database': 'weewx.sdb'
            }
        },
        'TimescaleDB': {
            'host': options.host,
            'port': options.port,
            'user': options.user,
            'password': options.password,
            'database': options.database
        }
    }

    # Simulate a new archive record event
    class DummyEvent:
        def __init__(self, record):
            self.record = record

    # Example record, adjust fields as needed for your schema
    test_record = {
        'dateTime': int(time.time()),
        'usUnits': 1,
        'interval': 300,
        'outTemp': 22.5,
        'inTemp': 21.0,
        'outHumidity': 55
    }

    print("Testing TimescaleDBSync plugin with record:", test_record)
    plugin = TimescaleDBSync(None, config_dict)
    plugin.new_archive_record(DummyEvent(test_record))
    print("Record inserted.")

"""
[TimescaleDB]
    host = localhost
    port = 5432
    user = weewx
    password = yourpassword
    database = weewx
    table = weewx_archive
    # Add any other custom options here
"""
