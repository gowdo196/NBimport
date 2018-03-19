# -*- coding: utf8 -*-
import sys, os
import time
import datetime
import logging, logging.handlers
import psycopg2
import psycopg2.extras
import msvcrt 

class CSIO_in :
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
        sqlout = "select dfh.sf_import_append_cntl_start('" + bhno + "');" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()            
        
        itemcount = 0
        """
        2   交易股數	9(08)	
        3   作業別	X(03)	1-進庫類 2-出庫類

                        0     1     2     3  
        sql = "select cseq, stock, qty, code from dbo.sacsio where bhno = '" + bhno + "'"
        """
        sql = "select cseq_mcsio, ckno_mcsio, stock_mcsio, qty_mcsio, code_mcsio from public.yy_trans_sacsio where bhno_mcsio = '" + bhno + "'"
        if self.showsql:
            print(sql)
        if self.C_ub_DB:
            self.ub_cursor_.execute(sql)
            rowall = self.ub_cursor_.fetchall()
            nowdate = time.strftime("%Y%m%d", time.localtime())
            for r in rowall:
                sqlout = "select dfh.sf_import_append_cntl("
                sqlout = sqlout + "'" + bhno + "',"
                sqlout = sqlout + "'" + r[0] + "',"
                sqlout = sqlout + "'" + r[1] + "',"
                sqlout = sqlout + "'" + r[2] + "',"
                sqlout = sqlout + "'" + nowdate + "',"
                if r[4][0] == '1':
                    sqlout = sqlout + str(int(r[3])) + ","
                else:
                    sqlout = sqlout + str(int(r[3])*(-1)) + ","
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
        sqlout = "select dfh.sf_import_append_cntl_end('" + bhno + "'," + str(itemcount) + ");" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()            
            
        self.logger.info("Make CSIO at " + bhno + " size=" + str(itemcount))
        
    def run(self) :
        self.logger.info("Start CSIO import")
        for b in self.bhnos:
            self.make_one_bhno(b)