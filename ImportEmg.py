# -*- coding: utf8 -*-
import sys, os
import time
import datetime
import logging, logging.handlers
import psycopg2
import psycopg2.extras
import msvcrt 

class CSIO_in :
    def __init__(self, ApexDB_cursor, logger, showsql, capexdb, savesql) :
        self.ApexCursor = ApexDB_cursor
        self.logger = logger
        self.showsql = showsql
        self.ApexDB = capexdb
        self.savesql = savesql
        
    def make_data(self):
        self.logger.info("Make Emg Over")
        
    def run(self) :
        self.logger.info("Start CSIO import")
        self.make_data()