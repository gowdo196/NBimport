# -*- coding: utf8 -*-
import sys, os
import time
import datetime
import logging, logging.handlers
import psycopg2
import psycopg2.extras
import msvcrt

class cntl_in :
    def __init__(self, bhnos, ub_cursor_, ApexDB_cursor, logger, showsql, C_ub_DB, capexdb, savesql) :
        self.bhnos = bhnos
        self.ub_cursor_ = ub_cursor_
        self.ApexCursor = ApexDB_cursor
        self.logger = logger
        self.showsql = showsql
        self.C_ub_DB = C_ub_DB
        self.ApexDB = capexdb
        self.savesql = savesql
        
    def make_one_bhno(self, bhno):
        self.logger.info("Make Cntl at " + bhno)
        sqlout = "select dfh.sf_import_cntl_start('" + bhno + "');" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()            
        
        itemcount = 0
        sql = "select cseq_mcsbs, ckno_mcsbs, stock_mcsbs, to_char(now() at time zone 'CCT','YYYYMMDD') as tdate, cnqty_mcsbs from public.yy_trans_sacsbs where area_mcsbs = '" + bhno + "'"
        if self.showsql:
            print(sql)
        if self.C_ub_DB:
            self.ub_cursor_.execute(sql)
            rowall = self.ub_cursor_.fetchall()
            self.logger.info("After Loading Data") 
            for r in rowall:
                sqlout = "select dfh.sf_import_cntl("
                sqlout = sqlout + "'" + bhno + "',"
                sqlout = sqlout + "'" + r[0] + "',"
                sqlout = sqlout + "'" + r[1] + "',"
                sqlout = sqlout + "'" + r[2] + "',"
                sqlout = sqlout + "'" + r[3] + "',"
                sqlout = sqlout + str(int(r[4])) + ","
                sqlout = sqlout + "0,0,0,0,0);"
                if self.showsql:
                    print(sqlout)
                if self.ApexDB:
                    self.ApexCursor.execute(sqlout)
                if self.savesql == 1:
                    fp = open("sqldata.txt", 'a')
                    fp.write(sqlout + "\n")
                    fp.close()            
                itemcount = itemcount + 1
        sqlout = "select dfh.sf_import_cntl_end('" + bhno + "'," + str(itemcount) + ");" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()            

        self.logger.info("Make Cntl at " + bhno + " size=" + str(itemcount))

    def run(self) :
        self.logger.info("Start Cntl import")
        for b in self.bhnos:
            self.make_one_bhno(b)