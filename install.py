# installer for the weewx-sdr driver
# Copyright 2016-2024 Matthew Wall
# Distributed under the terms of the GNU Public License (GPLv3)

from weecfg.extension import ExtensionInstaller

def loader():
    return TimescaleDBInstaller()

class TimescaleDBInstaller(ExtensionInstaller):
    def __init__(self):
        super(TimescaleDBInstaller, self).__init__(
            version="0.1",
            name='tsdb',
            description='Capture data from rtl_433',
            author="Timo Kleger",
            author_email="",
            files=[('bin/user', ['bin/user/tsdb.py'])]
            )
