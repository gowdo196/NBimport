# -*- coding: utf8 -*-
import sys, os
import ConfigParser
import time
import datetime
import logging, logging.handlers
from logging.handlers import RotatingFileHandler
import psycopg2
import psycopg2.extras
import msvcrt 
import math
import ImportBranch
import ImportSales
import ImportCustomer
import ImportRate
import ImportMrgn
import ImportCntl
import ImportShrt
import ImportCSIO
import ImportEmg
import ImportT80
# 取出 ini 中的設定     
def get_ini_int(section, key):
    config_ = ConfigParser.ConfigParser()
    config_.read('DataImport.ini')
    #config_.read('C:\\Python34\\mysite\\NBimport\\DataImport.ini')
    return config_.getint(section, key)
   
# 取出 ini 中的設定     
def get_ini_str(section, key):
    config_ = ConfigParser.ConfigParser()
    config_.read('DataImport.ini')
    #config_.read('C:\\Python34\\mysite\\NBimport\\DataImport.ini')
    return config_.get(section, key)  

def convert_to_float(sdata, intlen, dellen):
    v1 = float(sdata[0:intlen]) + float(sdata[intlen:]) / math.pow(10, dellen)
    return v1

if __name__ == "__main__":
    ####################################################################################\
    showsql = 0
    C_ub_DB = 1
    CApexDB = 1
    savesql = 0
    
    runnight = 1
    if len(sys.argv) < 2:
        runnight = 1
    else:
        if(sys.argv[1] == "day") :
            runnight = 0
        elif(sys.argv[1] == "emg") :
            runnight = 2
    
    if savesql == 1:
        fp = open("sqldata.txt", 'w')
        fp.close()

    ####################################################################################    
    #初始化APEX DB
    ApexDB_IP_ = get_ini_str("Apex", "DBIP")
    ApexDB_Port_ = get_ini_str("Apex", "DBPort")
    ApexDB_DB_ = get_ini_str("Apex", "DBName")
    ApexDB_User_ = get_ini_str("Apex", "UID")
    ApexDB_Pwd_ = get_ini_str("Apex", "PWD")
    if CApexDB:
        ApexDB_str_ = "host="+ApexDB_IP_+" port="+ApexDB_Port_+ " user="+ApexDB_User_ + " dbname="+ApexDB_DB_ + " password="+ApexDB_Pwd_
        try:
            ApexDB_conn_ = psycopg2.connect(ApexDB_str_)
        except:
            print "Can't connect to Working DB"
            sys.exit(0)
        
        ApexDB_conn_.autocommit = True  
        ApexDB_conn_.set_client_encoding('BIG5')
        ApexDB_cursor_ = ApexDB_conn_.cursor()
    else:
        ApexDB_cursor_ = None
    
    ####################################################################################    
    #初始化資料DB
    ub_DB_IP_ = get_ini_str("main", "DBIP")
    ub_DB_Port_ = get_ini_str("main", "DBPort")
    ub_DB_DB_ = get_ini_str("main", "DBName")
    ub_DB_User_ = get_ini_str("main", "UID")
    ub_DB_Pwd_ = get_ini_str("main", "PWD")
    if C_ub_DB:
        ub_DB_str_ = "host="+ub_DB_IP_+" port="+ub_DB_Port_+ " user="+ub_DB_User_ + " dbname="+ub_DB_DB_ + " password="+ub_DB_Pwd_
        ub_conn_ = psycopg2.connect(ub_DB_str_)
        ub_cursor_ = ub_conn_.cursor()
    else:
        ub_cursor_ = None
    

    ####################################################################################    
    #初始化Log
    #取得log handle
    logger = logging.getLogger()
    #設定大小滾動(10M,5個)
    Rthandler = RotatingFileHandler('myapp.log', maxBytes=10*1024*1024,backupCount=5)
    #設定輸出格式
    formatter = logging.Formatter('%(asctime)s--%(levelname)-8s [%(module)-s line:%(lineno)d] : %(message)s', '%Y-%m-%d %H:%M:%S')
    Rthandler.setFormatter(formatter)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(Rthandler)
    #螢幕輸出
    console = logging.StreamHandler();
    console.setLevel(logging.DEBUG);
    console.setFormatter(formatter);
    logger.addHandler(console);
    
    nowdate = time.strftime("%Y%m%d", time.localtime())
    sql = "select holiday from dfh.tb_trade_holiday where holiday='" + nowdate + "'"
    if CApexDB:
        ApexDB_cursor_.execute(sql)
        rowall = ApexDB_cursor_.fetchall()
        if(len(rowall)) <> 0:
            logger.info("Not Trade day")
            sys.exit()

    ####################################################################################    
    #讀取分公司
    bhnosize = get_ini_int("Data", "BhnoSize")
    bhnos = list()
    for i in range(0, bhnosize):
        bhnotag = "bhno{:02d}".format(i)
        gbhno = get_ini_str("Data", bhnotag)
        bhnos.append(gbhno)

    #讀取分公司id
    house_id_size = get_ini_int("house_id", "Size")
    house_id_list = list()
    for i in range(0, house_id_size):
        id_tag = "bhno{:02d}".format(i)
        id = get_ini_str("house_id", id_tag)
        house_id_list.append(id)
    ####################################################################################    
    #dump_ray_db()
    if runnight == 1:
        logger.info("Start night working")
        
        houseobj = ImportBranch.branch_in(house_id_list, ub_cursor_, ApexDB_cursor_, logger, showsql, C_ub_DB, CApexDB, savesql)
        houseobj.run()

        salesobj = ImportSales.sales_in(house_id_list, ub_cursor_, ApexDB_cursor_, logger, showsql, C_ub_DB, CApexDB, savesql)
        salesobj.run()
        
        customerobj = ImportCustomer.customer_in(bhnos, ub_cursor_, ApexDB_cursor_, logger, showsql, C_ub_DB, CApexDB, savesql)
        customerobj.run()
        
        rateobj = ImportRate.rate_in(bhnos, ub_cursor_, ApexDB_cursor_, logger, showsql, C_ub_DB, CApexDB, savesql)
        rateobj.run()

        cntlobj = ImportCntl.cntl_in(bhnos, ub_cursor_, ApexDB_cursor_, logger, showsql, C_ub_DB, CApexDB, savesql)
        cntlobj.run()

        mrgnobj = ImportMrgn.mrgn_in(bhnos, ub_cursor_, ApexDB_cursor_, logger, showsql, C_ub_DB, CApexDB, savesql)
        mrgnobj.run()

        shrtobj = ImportShrt.shrt_in(bhnos, ub_cursor_, ApexDB_cursor_, logger, showsql, C_ub_DB, CApexDB, savesql)
        shrtobj.run()
        
        tbt80obj = ImportT80.T80_in(bhnos, ub_cursor_, ApexDB_cursor_, logger, showsql, C_ub_DB, CApexDB, savesql)
        tbt80obj.run()
    elif runnight == 0 :
        logger.info("Start day working")
        CSIOobj = ImportCSIO.CSIO_in(bhnos, ub_cursor_, ApexDB_cursor_, logger, showsql, C_ub_DB, CApexDB, savesql)
        CSIOobj.run() 
    else:
        logger.info("Start Emg working")
        Emgobj = ImportEmg.Emg_in(ApexDB_cursor_, logger, showsql, CApexDB, savesql)
        Emgobj.run()

    ####################################################################################    
    if CApexDB:
        ApexDB_cursor_.close()
        ApexDB_conn_.close()

    if C_ub_DB:
        ub_cursor_.close()
        ub_conn_.close()