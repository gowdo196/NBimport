# -*- coding: utf8 -*-
import sys, os
import time
import datetime
import logging, logging.handlers
import psycopg2
import psycopg2.extras
import math
import msvcrt 

class mrgn_in :
    def __init__(self, bhnos, ub_cursor_, ApexDB_cursor, logger, showsql, C_ub_DB, capexdb, savesql) :
        self.bhnos = bhnos
        self.ub_cursor_ = ub_cursor_
        self.ApexCursor = ApexDB_cursor
        self.logger = logger
        self.showsql = showsql
        self.C_ub_DB = C_ub_DB
        self.ApexDB = capexdb
        self.savesql = savesql
        
    def convert_to_float(self, sdata, intlen, dellen):
        v1 = float(sdata[0:intlen]) + float(sdata[intlen:]) / math.pow(10, dellen)
        return v1
        
    def make_one_bhno(self, bhno):
        sqlout = "select dfh.sf_import_mrgnbuy_start('" + bhno + "');" 
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
        融資

        3  股票代號
        4  起息日期	YYYYMMDD
        5  成交日期	YYYYMMDD
        6  原始單序號	TERM：櫃號       長度：1
                        DSEQ：序號       長度：4
                        DNO：分單號碼    長度：1
        7  交易價格	小數位數6位，無小數點
        8  留存融資股數
        9  留存融資金額	小數位數6位，無小數點
        10 留存融資擔保品	小數位數6位，無小數點
        """
        #                0   1    2     3    4      5     6     7   8     9   
        #sql = "select cseq,ckno,stock,cdate,tdate,sheet,price,qty,cramt,crgd from dbo.cr_rate_day where area = '" + bhno + "'"
        sql = "select cseq_mcrud, ckno_mcrud, stock_mcrud, cdate_mcrud, tdate_mcrud, term_mcrud||dseq_mcrud||dno_mcrud::int as team, price_mcrud, qty_mcrud::int, cramt_mcrud::int, crgd_mcrud \
                from public.yy_trans_sacrud where area_mcrud = '" + bhno + "';"
        if self.showsql:
            print(sql)
        if self.C_ub_DB:
            self.ub_cursor_.execute(sql)
            rowall = self.ub_cursor_.fetchall()
            for r in rowall:
                sqlout = "select dfh.sf_import_mrgnbuy("
                sqlout = sqlout + "'" + bhno + "',"
                sqlout = sqlout + "'" + r[0] + "',"
                sqlout = sqlout + "'" + r[1] + "',"
                sqlout = sqlout + "'" + r[2] + "',"
                sqlout = sqlout + "'" + r[3] + "',"
                sqlout = sqlout + "'" + r[4] + "',"
                sqlout = sqlout + "'" + r[5][0:6] + "',"
                sqlout = sqlout + "'',"
                sqlout = sqlout + str(r[6]) + ","
                sqlout = sqlout + "0,0,0,"
                sqlout = sqlout + str(int(r[7])) + ","
                sqlout = sqlout + str(r[8]) + ","
                sqlout = sqlout + str(int(r[9])) + ","
                sqlout = sqlout + "'','');"
                if self.showsql:
                    print(sqlout)
                if self.ApexDB:
                    self.ApexCursor.execute(sqlout)
                if self.savesql == 1:
                    fp = open("sqldata.txt", 'a')
                    fp.write(sqlout + "\n")
                    fp.close()            
                itemcount = itemcount + 1
        sqlout = "select dfh.sf_import_mrgnbuy_end('" + bhno + "'," + str(itemcount) + ");" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()            
            
        self.logger.info("Make Mrgnbuy at " + bhno + " size=" + str(itemcount))
        
    def run(self) :
        self.logger.info("Start Mrgnbuy import")
        for b in self.bhnos:
            self.make_one_bhno(b)