#!/usr/bin/env python
# -*- coding: utf-8 -*- 


import os
import time
import logging
from optparse import OptionParser
from crawler import Crawler


#命令行参数解析#
'''
    参数说明：
    -u 指定爬虫开始地址 指定爬虫开始地址 指定爬虫开始地址 指定爬虫开始地址
    -d 指定爬虫深度 指定爬虫深度 指定爬虫深度
    -- thread thread thread thread 指定线程池大小，多爬取页面可选参数默认 指定线程池大小，多爬取页面可选参数默认 指定线程池大小，多爬取页面可选参数默认 指定线程池大小，多爬取页面可选参数默认 指定线程池大小，多爬取页面可选参数默认 指定线程池大小，多爬取页面可选参数默认 指定线程池大小，多爬取页面可选参数默认 指定线程池大小，多爬取页面可选参数默认 指定线程池大小，多爬取页面可选参数默认 指定线程池大小，多爬取页面可选参数默认 指定线程池大小，多爬取页面可选参数默认 指定线程池大小，多爬取页面可选参数默认 10
    -- dbfile dbfile dbfile 存放结果数据到指定的库（ 存放结果数据到指定的库（ 存放结果数据到指定的库（ 存放结果数据到指定的库（ 存放结果数据到指定的库（ 存放结果数据到指定的库（ 存放结果数据到指定的库（ sqlitesqlite sqlite sqlite）文件中 ）文件中 ）文件中
    -- key key key 页面 内的关键词，获取满足该网可选参数默认为所有页面 内的关键词，获取满足该网可选参数默认为所有页面 内的关键词，获取满足该网可选参数默认为所有页面 内的关键词，获取满足该网可选参数默认为所有页面 内的关键词，获取满足该网可选参数默认为所有页面 内的关键词，获取满足该网可选参数默认为所有页面 内的关键词，获取满足该网可选参数默认为所有页面 内的关键词，获取满足该网可选参数默认为所有页面 内的关键词，获取满足该网可选参数默认为所有页面 内的关键词，获取满足该网可选参数默认为所有页面 内的关键词，获取满足该网可选参数默认为所有页面 内的关键词，获取满足该网可选参数默认为所有页面 内的关键词，获取满足该网可选参数默认为所有页面 内的关键词，获取满足该网可选参数默认为所有页面 内的关键词，获取满足该网可选参数默认为所有页面
    内的关键词，获取满足该网可选参数默认为所有-- downloadfile downloadfile downloadfile downloadfile downloadfile downloadfile 需要下载的文件后缀名，默认为全部。 需要下载的文件后缀名，默认为全部。 需要下载的文件后缀名，默认为全部。 需要下载的文件后缀名，默认为全部。 需要下载的文件后缀名，默认为全部。 需要下载的文件后缀名，默认为全部。 需要下载的文件后缀名，默认为全部。 需要下载的文件后缀名，默认为全部。 需要下载的文件后缀名，默认为全部。
    -l 日志记录文件详细程度，数字越大可选参默认 日志记录文件详细程度，数字越大可选参默认 日志记录文件详细程度，数字越大可选参默认 日志记录文件详细程度，数字越大可选参默认 日志记录文件详细程度，数字越大可选参默认 日志记录文件详细程度，数字越大可选参默认 日志记录文件详细程度，数字越大可选参默认 日志记录文件详细程度，数字越大可选参默认 日志记录文件详细程度，数字越大可选参默认 日志记录文件详细程度，数字越大可选参默认 日志记录文件详细程度，数字越大可选参默认 日志记录文件详细程度，数字越大可选参默认 日志记录文件详细程度，数字越大可选参默认 日志记录文件详细程度，数字越大可选参默认 日志记录文件详细程度，数字越大可选参默认 spider.logspider.log spider.log spider.log spider.logspider.log
    -- testself testself testself testself testself 程序自测，可选参数 程序自测，可选参数 程序自测，可选参数 程序自测，可选参数 程
'''
def  parseoption():
        
        helpInfo = '%%prog %s' % "-u url -d deep -f logfile -l loglevel(1-5) -testself --thread number --dbfile filepath --key kerword --downloadfile "
        optParser = OptionParser(usage = helpInfo)
        optParser.add_option("-u",dest="url",default="",type="string",help="The url for crawl,it can't be empty.")
        optParser.add_option("-d",dest="depth",default=1,type="int",help="The depth to crawl,Default: 1")
        optParser.add_option("-f",dest="logfile",default="spider.log",type="string",help="The log file path, Default: spider.log")
        optParser.add_option("-l",dest="loglevel",default="5",type="int",help="The level(1-5) of logging details. Default: 5")
        optParser.add_option("--thread",dest="thread",default="10",type="int",help="The number of threadpool. Default: 10")
        optParser.add_option("--dbfile",dest="dbfile",default="spider.db",type="string",help="The SQLite file path. Default:data.sql")
        optParser.add_option("--key",metavar="key",default="",type="string",help="The keyword for crawling,use , to split. Default: ''")
        optParser.add_option("--downloadfile",dest="downloadfile",default="",type="string",help="The extension of filename which need to be downloaded,use , to split. Default:ALL")
        (options,args) = optParser.parse_args()
        ##要爬取的url不可为空##
        if  options.url == "":
                print "url can't be empty!,please see the help infomation.\r\n"
                print optParser.print_help()
                return False
        if options.loglevel > 5 or options.loglevel < 1:
                print "unknow log level! please see the help infomation.\r\n"
                print optParser.print_help()
                return False
        return options

##设置日志的格式##
def logConfig(options):
	
	LEVELS = {
		1:logging.CRITICAL,
		2:logging.ERROR,
		3:logging.WARNING,
		4:logging.INFO,
		5:logging.DEBUG
	}
        ##追加模式##
        logging.basicConfig(filename =  os.path.join(os.getcwd(),  options.logfile),level = LEVELS[options.loglevel],filemode = 'a',format = '%(asctime)s  %(levelname)s: %(message)s')

##处理一下url,加上http://##        
def  sanitizeUrl(url):
        if not url.startswith(("http://","https://")): 
                return "http://%s" % url  
        
        return url
 
if __name__=='__main__':
        
    ##设置默认编码UTF8##
    
    ##解析命令行参数##
    options = parseoption()
    if not options:
        exit()
         
    ##规整url##
    options.url = sanitizeUrl(options.url)
        
    ##根据命令行参数设置日志##
    logConfig(options)
    logger =  logging.getLogger()
        
    ##启动爬虫##
    crawler = Crawler(options)
    crawler.start()
        
    print "INFO:thead number of threadpool:%s,fetched url:%s,unfeched url:%s,depth:%s" % crawler.getInfo()
    ##10s显示一次状态##
    while crawler.isAlive():
        for x in xrange(10):
            if crawler.isAlive():
                time.sleep(1)
                continue
            else:
                break
        if crawler.isAlive():
            print "INFO:thead number of threadpool:%s,fetched url:%s,unfeched url:%s,depth:%s" % crawler.getInfo()
        
    print "INFO:thead number of threadpool:%s,fetched url:%s,unfeched url:%s,depth:%s" % crawler.getInfo()
    print "over"
       
