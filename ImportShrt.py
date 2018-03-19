# -*- coding: utf8 -*-
import sys, os
import time
import datetime
import logging, logging.handlers
import psycopg2
import psycopg2.extras
import msvcrt 
import math

class shrt_in :
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
        sqlout = "select dfh.sf_import_shrtsale_start('" + bhno + "');" 
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
        融券
        
        2   股票代號
        3   起息日期	YYYYMMDD
        4   成交日期	YYYYMMDD
        5   原始單序號	TERM：櫃號       長度：1
                        DSEQ：序號       長度：4
                        DNO：分單號碼    長度：1
        6   交易價格	小數位數6位，無小數點
        7   留存融券借券費	小數位數6位，無小數點
        8   留存融券股數
        9   留存融券保證品	小數位數6位，無小數點
        10  留存融券保證金	小數位數6位，無小數點
            保證金金額 - 已償還保證金金額

                        0   1   2       3     4     5       6   7   8   9       10
        sql = "select cseq,ckno,stock,cdate,tdate,sheet,price,dlfee,qty,gdamt,gtamt from dbo.db_rate_day where area = '" + bhno + "'"
        """
        sql = "select cseq_mdbud, ckno_mdbud, stock_mdbud, cdate_mdbud, tdate_mdbud, term_mdbud||dseq_mdbud||dno_mdbud::int, price_mdbud, dlfee_mdbud, qty_mdbud, gdamt_mdbud, gtamt_mdbud \
                from public.yy_trans_sadbud where area_mdbud = '" + bhno + "'"
        if self.showsql:
            print(sql)
        if self.C_ub_DB:
            self.ub_cursor_.execute(sql)
            rowall = self.ub_cursor_.fetchall()
            for r in rowall:
                sqlout = "select dfh.sf_import_shrtsale("
                sqlout = sqlout + "'" + bhno + "',"
                sqlout = sqlout + "'" + r[0] + "',"
                sqlout = sqlout + "'" + r[1] + "',"
                sqlout = sqlout + "'" + r[2] + "',"
                sqlout = sqlout + "'" + r[3] + "',"
                sqlout = sqlout + "'" + r[4] + "',"
                sqlout = sqlout + "'" + r[5][0:6] + "',"
                sqlout = sqlout + "'',"
                sqlout = sqlout + str(r[6]) + ","
                sqlout = sqlout + "0,0,0,0,0,0,"
                sqlout = sqlout + str(int(r[7])) + ","
                sqlout = sqlout + str(int(r[8])) + ","
                sqlout = sqlout + str(int(r[9])) + ","
                sqlout = sqlout + str(int(r[10])) + ","
                sqlout = sqlout + str(int(r[10])) + ","
                sqlout = sqlout + str(int(r[9])) + ","
                sqlout = sqlout + "'','');"                
                #if self.showsql:
                print(sqlout)
                if self.ApexDB:
                    self.ApexCursor.execute(sqlout)
                if self.savesql == 1:
                    fp = open("sqldata.txt", 'a')
                    fp.write(sqlout + "\n")
                    fp.close()            
                itemcount = itemcount + 1
        sqlout = "select dfh.sf_import_shrtsale_end('" + bhno + "'," + str(itemcount) + ");" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()            
            
        self.logger.info("Make Shrtsale at " + bhno + " size=" + str(itemcount))
        
    def run(self) :
        self.logger.info("Start Shrtsale import")
        for b in self.bhnos:
            self.make_one_bhno(b)