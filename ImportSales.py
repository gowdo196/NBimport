# -*- coding: utf8 -*-
import sys, os
import time
import datetime
import logging, logging.handlers
import psycopg2
import psycopg2.extras
import msvcrt 

class sales_in :
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
        sqlout = "select dfh.sf_import_salseinfo_start('" + bhno + "');" 
        itemcount = 0
        sql = "select b.broker_id,b.broker_code,house.house_code from public.ax_bs_broker b \
                left join public.ax_bs_house house on b.house_id = house.house_id where b.house_id ='" + bhno + "'"
        if self.showsql:
            print(sql)
        if self.C_ub_DB:
            self.ub_cursor_.execute(sql)
            rowall = self.ub_cursor_.fetchall()
            for r in rowall:
                sqlout = sqlout + "select dfh.sf_import_salseinfo('" + str(r[2])[1:4]+str(r[1]).ljust(3,'Z') + "','" + str(r[1]).ljust(3,'Z') + "','" + str(r[2]) + "','0','1','1');"
                itemcount = itemcount + 1
        sqlout = sqlout + "select dfh.sf_import_salseinfo_end('" + rowall[0][2] + "'," + str(itemcount) + ");" 
        if self.showsql:
            print(sqlout)
        if self.ApexDB:
            self.ApexCursor.execute(sqlout)
        if self.savesql == 1:
            fp = open("sqldata.txt", 'a')
            fp.write(sqlout + "\n")
            fp.close()            

        self.logger.info("Make Sales at " + bhno + " size=" + str(itemcount))
        
    def run(self) :
        self.logger.info("Start Sales import")
        for b in self.house_id_list:
            self.make_one_bhno(b)