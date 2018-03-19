# -*- coding: utf8 -*-
import sys, os
import time
import datetime
import logging, logging.handlers
import psycopg2
import psycopg2.extras
import msvcrt 

class T80_in:
    def __init__(self, house_id_list, ub_cursor_, ApexDB_cursor, logger, showsql, C_ub_DB, capexdb, savesql) :
        self.house_id_list = house_id_list
        self.ub_cursor_ = ub_cursor_
        self.ApexCursor = ApexDB_cursor
        self.logger = logger
        self.showsql = showsql
        self.C_ub_DB = C_ub_DB
        self.ApexDB = capexdb
        self.savesql = savesql
        
    def make_one_bhno(self):
        sqlout = "select dfh.sf_import_t80_start();" 
        itemcount = 0
        sql = "select stock from public.tb_t80"
        if self.showsql:
            print(sql)
        if self.C_ub_DB:
            self.ub_cursor_.execute(sql)
            rowall = self.ub_cursor_.fetchall()
            for r in rowall:
                sqlout = sqlout + "insert into dfh.tb_T80_temp(stock, stype) values ('"+r[0]+"', '0');"
                itemcount = itemcount + 1
        sqlout = sqlout + "select dfh.sf_import_t80_end(" + str(itemcount) + ");" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()            

        self.logger.info("Make T80 size=" + str(itemcount))
        
    def run(self) :
        self.logger.info("Start T80 import")
        self.make_one_bhno()