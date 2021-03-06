# -*- coding: utf8 -*-
import sys, os
import time
import datetime
import logging, logging.handlers
import psycopg2
import psycopg2.extras
import msvcrt 

class branch_in :
    def __init__(self, house_id_list, ub_cursor_, ApexDB_cursor, logger, showsql, C_ub_DB, capexdb, savesql) :
        self.house_id_list = house_id_list
        self.ub_cursor_ = ub_cursor_
        self.ApexCursor = ApexDB_cursor
        self.logger = logger
        self.showsql = showsql
        self.C_ub_DB = C_ub_DB
        self.ApexDB = capexdb
        self.savesql = savesql
        
    def make_one_bhno(self, bhno):
        sqlout = ""
        sql = "select house_code, house_name, house_id from public.ax_bs_house where house_id ='" + bhno + "'"
        if self.showsql:
            print(sql)
        if self.C_ub_DB:
            self.ub_cursor_.execute(sql)
            rowall = self.ub_cursor_.fetchall()
            for r in rowall:
                sqlout = sqlout + "select dfh.sf_import_branch('" + str(r[0]) + "','" + str(r[1].decode('utf8').encode('big5')) + "','1');"

        return sqlout
        
    def run(self) :
        self.logger.info("Start Branchs import")
        sqlout = "select dfh.sf_import_branch_start();"
        itemcount = 0
        for b in self.house_id_list:
            sqlout = sqlout + self.make_one_bhno(b)
            itemcount = itemcount + 1
        sqlout = sqlout + "select dfh.sf_import_branch_end(" + str(itemcount) + ");"
        if self.showsql:
            print(sqlout)
        if self.ApexDB:
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()