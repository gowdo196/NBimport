# -*- coding: utf8 -*-
import sys, os
import time
import datetime
import logging, logging.handlers
import psycopg2
import psycopg2.extras
import msvcrt

class customer_in :
    def __init__(self, bhnos, ub_cursor_, ApexDB_cursor, logger, showsql, C_ub_DB, capexdb, savesql) :
        self.bhnos = bhnos
        self.ub_cursor_ = ub_cursor_
        self.ApexCursor = ApexDB_cursor
        self.logger = logger
        self.showsql = showsql
        self.C_ub_DB = C_ub_DB
        self.ApexDB = capexdb
        self.savesql = savesql
        
    def make_cust(self, bhno):
        self.logger.info("Make Customer at " + bhno) 
        """
        3 信用開戶 ??? 原本是日期
        4 投資上限
        5 電子額度
        6 融資額度
        7 融券額度
        8 電話(一)
        9 現沖註記	N:非現沖戶 Y:先買後賣戶 X:買賣現沖戶
        10 ID
        """
        #                                       1     2             3          4          5        6      7     8      9     10
        #sql = "select cseq,                  sname, tsale,      date_cscd,   dlimit, lamt_njcu, cramt, dbamt, TEL1, sflag, idno from dbo.sacust where area = '" + bhno + "'"
        sql = "select cseq_mcumb, ckno_mcumb, sname_mcumb,  sale_mcumb, dtrade_mcumb, dlimit_mcumb, '0', cramt_mcumb, dbamt_mcumb, mtel_mcumb, dflag_mcumb, idno_mcumb \
                from public.yy_trans_sacseq where bkno_mcumb = '" + bhno + "' ORDER BY cseq_mcumb;"
        if self.showsql:
            print(sql)
        if self.C_ub_DB :
            self.ub_cursor_.execute(sql)
            rowall = self.ub_cursor_.fetchall()
        
        ########################################################################################
        self.logger.info("After Loading Data") 
        sqlout = "select dfh.sf_import_customerinfo_start('" + bhno + "');" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:       
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()  

        nowdate = time.strftime("%Y%m%d", time.localtime())
        itemcount = 0
        if self.C_ub_DB :
            for r in rowall:
                sqlout = "select dfh.sf_import_customerinfo("
                sqlout = sqlout + "'" + r[0] + "',"
                sqlout = sqlout + "'" + r[1] + "',"
                #print(str(r[0])+","+str(r[2].decode('utf8').encode('big5', 'ignore')))#hkscs
                sqlout = sqlout + "'" + str(r[2].decode('utf8').encode('cp950', 'ignore')) + "',"
                sqlout = sqlout + "'" + str(r[3]).ljust(3,'Z') + "',"
                sqlout = sqlout + "'" + bhno + "',"
                sqlout = sqlout + "'0',"
                sqlout = sqlout + "'" + str(r[4]) + "',"
                if(str(r[10])) == "N" :
                    sqlout = sqlout + "'N',"
                else:
                    sqlout = sqlout + "'Y',"
                sqlout = sqlout + str(int(r[5])) + ","
                sqlout = sqlout + str(int(r[6])) + ","
                sqlout = sqlout + str(int(r[7])) + ","
                sqlout = sqlout + str(int(r[8])) + ","
                strtel = str(r[9])#.encode('latin-1').decode('big5', 'ignore')
                sqlout = sqlout + "'" + strtel.replace("-", "") + "');"

                if self.showsql:
                    print(sqlout)
                if self.ApexDB:       
                    self.ApexCursor.execute(sqlout)
                if self.savesql == 1:
                    fp = open("sqldata.txt", 'a')
                    fp.write(sqlout + "\n")
                    fp.close()        
                itemcount = itemcount + 1
        sqlout = "select dfh.sf_import_customerinfo_end('" + bhno + "'," + str(itemcount) + ");" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:       
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()  
        self.logger.info("End Insert customer")         
            
        ########################################################################################
        self.logger.info("Start insert other Data") 
        sqlout = "select dfh.sf_import_day_trade_start('" + bhno + "');" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:       
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()            
            
        sqlout = "select dfh.sf_import_id_start('" + bhno + "');" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:       
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()                      
        
        nowdate = time.strftime("%Y%m%d", time.localtime())
        itemcount = 0
        if self.C_ub_DB :
            for r in rowall:
                sqlout = "select dfh.sf_import_day_trade("
                sqlout = sqlout + "'" + bhno + "',"
                sqlout = sqlout + "'" + r[0] + "',"
                sqlout = sqlout + "'" + r[1] + "',"
                if(str(r[10])) == "N" :
                    sqlout = sqlout + "'N');"
                else:
                    sqlout = sqlout + "'Y');"
                if self.showsql:
                    print(sqlout)
                if self.ApexDB:       
                    self.ApexCursor.execute(sqlout)
                if self.savesql == 1:
                    fp = open("sqldata.txt", 'a')
                    fp.write(sqlout + "\n")
                    fp.close() 

                sqlout = "select dfh.sf_import_id("
                sqlout = sqlout + "'" + bhno + "',"
                sqlout = sqlout + "'" + r[0] + "',"
                sqlout = sqlout + "'" + r[1] + "',"
                sqlout = sqlout + "'" + r[11] + "');"
                if self.showsql:
                    print(sqlout)
                if self.ApexDB:       
                    self.ApexCursor.execute(sqlout)
                if self.savesql == 1:
                    fp = open("sqldata.txt", 'a')
                    fp.write(sqlout + "\n")
                    fp.close()                                
                itemcount = itemcount + 1
        sqlout = "select dfh.sf_import_day_trade_end('" + bhno + "'," + str(itemcount) + ");" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:       
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()  
            
        sqlout = "select dfh.sf_import_id_end('" + bhno + "'," + str(itemcount) + ");" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:       
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()            
        self.logger.info("End Insert")         
            
        sqlout = "select dfh.sf_import_telphone_start('" + bhno + "');" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:       
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()            
        sqlout = "select dfh.sf_import_telphone_end('" + bhno + "'," + str(itemcount) + ");" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:       
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()                   
            
        self.logger.info("Make Customer at " + bhno + " size=" + str(itemcount))

    def run(self) :
        self.logger.info("Start Customer import")
        for b in self.bhnos:
            self.make_cust(b)