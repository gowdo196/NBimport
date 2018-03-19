# -*- coding: utf8 -*-
import sys, os
import time
import datetime
import logging, logging.handlers
import psycopg2
import psycopg2.extras
import msvcrt 

class rate_in :
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
        sqlout = "select dfh.sf_import_custrate_start('" + bhno + "');" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()            
        
        itemcount = 0
        """                長度
        1 客戶帳號          7
        2 身份證號          10
        3 轉檔日期          8
        4 集中整戶維持率	7	1234.56小數兩位
        """
        #               1         2     3          4
        #sql = "select B.cseq, A.idno, A.trdate, A.ratio from dbo.sacscd A "
        #sql = sql + " left join dbo.sacust B on B.idno=A.idno and B.area=A.bhno where A.bhno = '" + bhno + "'"

        sql = "select cseq_mcumb, ckno_mcumb, idno_mcumb, to_char(now() at time zone 'CCT','YYYYMMDD') as trdate, rate_mcumb from public.yy_trans_sacseq where bkno_mcumb = '"+ bhno +"' ORDER BY cseq_mcumb;"
        if self.showsql:
            print(sql)
        if self.C_ub_DB:
            self.ub_cursor_.execute(sql)
            rowall = self.ub_cursor_.fetchall()
            for r in rowall:
                if type(r[0]) == type(None):
                    continue
                if float(r[4]) == 0:
                    continue
                sqlout = "select dfh.sf_import_custrate("
                sqlout = sqlout + "'" + bhno + "',"
                sqlout = sqlout + "'" + r[0] + "',"
                sqlout = sqlout + "'" + r[1] + "',"
                sqlout = sqlout + "'" + r[3] + "',"
                sqlout = sqlout + str(float(r[4])) + ","
                sqlout = sqlout + "'N','');"
                if self.showsql:
                    print(sqlout)
                if self.ApexDB:
                    self.ApexCursor.execute(sqlout)
                if self.savesql == 1:
                    fp = open("sqldata.txt", 'a')
                    fp.write(sqlout + "\n")
                    fp.close()            
                itemcount = itemcount + 1
        sqlout = "select dfh.sf_import_custrate_end('" + bhno + "'," + str(itemcount) + ");" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()

        self.logger.info("Make Custrate at " + bhno + " size=" + str(itemcount))

    def run(self) :
        self.logger.info("Start Custrate import")
        for b in self.bhnos:
            self.make_one_bhno(b)