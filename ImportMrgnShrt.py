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
        
    def build_list_of_cmd(self):
        list_of_cmd = []
        #先從三個檔案 for三次把str組好
        for SLDT in read(self.ub_cursor_[0]):
            cmd_ = ""
            stock_temp = SLDT[0:6]
            #SLDT
            cmd_ = cmd_ + SLDT[6:10]#分公司代號(轉四碼)
            cmd_ = cmd_ + SLDT[0:6]#stock
            cmd_ = cmd_ + SLDT[11:19]#融資分配數量
                                    #已委託融資額數
            cmd_ = cmd_ + SLDT[19:27]#融卷分配數量
                                    #已委託融卷額數
            for CT in read(self.ub_cursor_[1]):
                #CT
                if CT[0:6] == stock_temp:
                    cmd_ = cmd_ + CT[0:6]#stock
                    cmd_ = cmd_ + CT[82:90]#融資調整數量
                    cmd_ = cmd_ + CT[90:98]#融卷調整數量
                    cmd_ = cmd_ + CT[98:102]#融資成數
                    cmd_ = cmd_ + CT[102:106]#融卷保證金成數

            for T30V in read(self.ub_cursor_[2]):
                #T30V
                if T30V[0:6] == stock_temp:
                    cmd_ = cmd_ + T30V[0:6]#stock
                    cmd_ = cmd_ + T30V[49:50]#可盤下放空

            list_of_cmd.append(cmd_)
        return list_of_cmd

    def make_one_bhno(self):
        """
        sqlout = "select dfh.sf_import_mrgn_shrt_start('" + bhno + "');" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()
        """
        itemcount = 0

        if self.C_ub_DB:
            """
            _cmd varchar(40)
            sf_import_mrgn_shrt(_mode varchar, _branch varchar, _cmd varchar)
            """

            list_of_cmd = build_list_of_cmd()
            for _cmd in list_of_cmd:
                sqlout = "select dfh.sf_import_mrgn_shrt("
                sqlout = sqlout + "'0',"
                sqlout = sqlout + "'" + bhno + "',"
                sqlout = sqlout + "'" + str(_cmd) + "');"
                if self.showsql:
                    print(sqlout)
                if self.ApexDB:
                    self.ApexCursor.execute(sqlout)
                if self.savesql == 1:
                    fp = open("sqldata.txt", 'a')
                    fp.write(sqlout + "\n")
                    fp.close()
                itemcount = itemcount + 1
        """
        sqlout = "select dfh.sf_import_mrgn_shrt_end('" + bhno + "'," + str(itemcount) + ");" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()            
        """
        self.logger.info("Make MrgnShrt size=" + str(itemcount))
        
    def run(self) :
        self.logger.info("Start MrgnShrt import")
        #for b in self.bhnos:
        self.make_one_bhno()