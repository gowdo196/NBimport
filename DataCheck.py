# -*- coding: utf8 -*-
from __future__ import print_function
import sys, os
import ConfigParser
import time
import datetime
import logging, logging.handlers
from logging.handlers import RotatingFileHandler
import psycopg2
import psycopg2.extras
import pymssql
import msvcrt 
import math

def check_customer(Old_cursor, New_cursor):
    print("Check Customer")
    fp = open("check_resault.txt",'a')
    fp.write("Check customer\n")
    
    sql = "select count(cesq) from dfh.tb_customerinfo"
    Old_cursor.execute(sql)
    oldra = Old_cursor.fetchall()
    oldsize = int(oldra[0][0])
    
    New_cursor.execute(sql)
    newra = New_cursor.fetchall()
    newsize = int(newra[0][0])
    
    if oldsize == newsize:
        fp.write("customer size same\n")
    else :
        fp.write ("error=> customer size not same\n")
        
    sql = "select cesq, confirm, name, sale, branch from dfh.tb_customerinfo"
    Old_cursor.execute(sql)
    oldra = Old_cursor.fetchall()
    count = 0
    error = 0
    for r in oldra:
        sql = "select name, sale from dfh.tb_customerinfo where branch='" + r[4] + "' and cesq='" + r[0] + "' and confirm='" + r[1] + "'"
        New_cursor.execute(sql)
        newra = New_cursor.fetchall()
        count = count + 1
        if (count % 1000) == 0:
            print(".", end = '')
            
        if New_cursor.rownumber != 1:
            fp.write("error=> " + r[4] + "-" + r[0]+ r[1] + " not find\n")
            error = error + 1
            continue
        if(newra[0][0] != r[2]) or (newra[0][1] != r[3]):
            fp.write("error=> " + r[4] + "-" + r[0]+ r[1] + " name or sales not same:" + r[2] + "," + newra[0][0] + ":" +  r[3] + "," + newra[0][1] + " \n")
            error = error + 1
            continue
            
    print(".")        
    fp.write("Customer data same=" + str(count) + ", error=" + str(error) + "\n")
    fp.close()
    print("Check Customer over")
    
def check_rate(Old_cursor, New_cursor):
    print("Check Rate")
    fp = open("check_resault.txt",'a')
    fp.write("Check Rate\n")
        
    sql = "select branch, cesq, confirm from dfh.tb_customerinfo"
    Old_cursor.execute(sql)
    oldra = Old_cursor.fetchall()
    count = 0
    error = 0
    workcount = 0
    for r in oldra:
        workcount = workcount + 1
        if (workcount % 1000) == 0:
            print(".", end = '')
        sql = "select * from dfh.sf_request_keer_rate_inside('" + r[0] + "','" + r[1] + "','');"
        Old_cursor.execute(sql)
        olddata = Old_cursor.fetchall()
        for rr in olddata:
            if float(rr[2]) > 0:
                count = count + 1
                sql = "select * from dfh.sf_request_keer_rate_inside('" + r[0] + "','" + r[1] + "','');"
                New_cursor.execute(sql)
                newra = New_cursor.fetchall()
                if New_cursor.rownumber != 1:
                    fp.write("error=> " + r[0] + "-" + r[1]+ r[2] + " not find\n")
                    error = error + 1
                    continue
                if(newra[0][2] != rr[2]) or (newra[0][3] != rr[3]):
                    fp.write("error=> " + r[0] + "-" + r[1]+ r[2] + " rate not same:" + str(rr[2]) + "," + str(newra[0][2]) + ":" + str(rr[3]) + "," + str(newra[0][3]) + " \n")
                    error = error + 1
                    continue
    print(".")        
    fp.write("Rate data same=" + str(count) + ", error=" + str(error) + "\n")
    print("Check Rate over")    
    
    fp.close()
    
def check_cntl(Old_cursor, New_cursor):
    print("Check Cntl")
    fp = open("check_resault.txt",'a')
    fp.write("Check Cntl\n")
        
    sql = "select branch, cesq, confirm from dfh.tb_customerinfo"
    Old_cursor.execute(sql)
    oldra = Old_cursor.fetchall()
    count = 0
    error = 0
    workcount = 0
    for r in oldra:
        workcount = workcount + 1
        if (workcount % 1000) == 0:
            print(".", end = '')
        sql = "select * from dfh.sf_request_last_store_inside('" + r[0] + "','" + r[1] + "','');"
        Old_cursor.execute(sql)
        olddata = Old_cursor.fetchall()
        for rr in olddata:
            if (int(rr[1]) > 0) or (int(rr[2]) > 0) or (int(rr[3]) > 0):
                count = count + 1
                sql = "select * from dfh.sf_request_last_store_inside('" + r[0] + "','" + r[1] + "','" + rr[0] + "');"
                New_cursor.execute(sql)
                newra = New_cursor.fetchall()
                if New_cursor.rownumber != 1:
                    fp.write("error=> " + r[0] + "-" + r[1]+ r[2] + ":" + rr[0] + " not find\n")
                    error = error + 1
                    continue
                if (newra[0][1] != rr[1]) or  (newra[0][2] != rr[2]) or (newra[0][3] != rr[3]):
                    fp.write("error=> " + r[0] + "-" + r[1]+ r[2] + ":" + rr[0] + " not same:" + str(rr[1]) + "," + str(newra[0][1]) + ":" + str(rr[2]) + "," + str(newra[0][2]) + ":" + str(rr[3]) + "," + str(newra[0][3]) + " \n")
                    error = error + 1
                    continue
    print(".")        
    fp.write("Cntl data same=" + str(count) + ", error=" + str(error) + "\n")
    print("Check Cntl over")    
    
    
    fp.close()    

def convert_to_float(sdata, intlen, dellen):
    v1 = float(sdata[0:intlen]) + float(sdata[intlen:]) / math.pow(10, dellen)
    return v1

if __name__ == "__main__":
    ####################################################################################    
    ApexDB_IP_ = "192.168.24.218"
    ApexDB_Port_ = "5432"
    ApexDB_DB_ = "cs"
    ApexDB_User_ = "postgres"
    ApexDB_Pwd_ = "0000"

    ApexDB_str_ = "host="+ApexDB_IP_+" port="+ApexDB_Port_+ " user="+ApexDB_User_ + " dbname="+ApexDB_DB_ + " password="+ApexDB_Pwd_
    print(ApexDB_str_)
    try:
        ApexDB_conn_ = psycopg2.connect(ApexDB_str_)
    except:
        print ("Can't connect to Working DB")
        sys.exit(0)
      
    ApexDB_conn_.autocommit = True  
    ApexDB_cursor_ = ApexDB_conn_.cursor()
    ####################################################################################    
    NewDB_IP_ = "192.168.254.216"
    NewDB_Port_ = "5432"
    NewDB_DB_ = "cs"
    NewDB_User_ = "postgres"
    NewDB_Pwd_ = "0000"
    NewDB_str_ = "host="+NewDB_IP_+" port="+NewDB_Port_+ " user="+NewDB_User_ + " dbname="+NewDB_DB_ + " password="+NewDB_Pwd_
    print(NewDB_str_)
    try:
        NewDB_conn_ = psycopg2.connect(NewDB_str_)
    except:
        print ("Can't connect to Old DB")
        sys.exit(0)
    
    NewDB_conn_.autocommit = True  
    NewDB_cursor_ = NewDB_conn_.cursor()
    ####################################################################################    
    fp = open("check_resault.txt",'w')
    fp.write("NEW to OLD\n")
    fp.close()
    check_customer(ApexDB_cursor_, NewDB_cursor_)
    check_rate(ApexDB_cursor_, NewDB_cursor_)
    check_cntl(ApexDB_cursor_, NewDB_cursor_)
    fp = open("check_resault.txt",'a')
    fp.write("==============================================================\n")
    fp.write("Reverse Check\n")
    fp.close()
    check_customer(NewDB_cursor_, ApexDB_cursor_)
    check_rate(NewDB_cursor_, ApexDB_cursor_)
    check_cntl(NewDB_cursor_, ApexDB_cursor_)
    ####################################################################################    
    ApexDB_cursor_.close()
    ApexDB_conn_.close()  
        
    #NewDB_cursor_.close()
    #NewDB_conn_.close()  
    

        
    
    
 