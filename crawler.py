#encoding=utf-8
#!/usr/bin/env python

import os
import sys
import re
import time
import locale
import logging
import urllib
import urllib2
import socket
from threading import Thread 
from database import  DataBase
from  threadpool import ThreadPool 
from Queue import Queue
from bs4 import BeautifulSoup


logger =  logging.getLogger()

def  get_Data(url):
         
        socket.setdefaulttimeout(10)
        logger.info("fetch url:%s",url)
        
        try:
                
                url = url.split("?",1)
                if len(url) > 1:
                        url[1] = urllib.quote(url[1])
                url = "?".join(url)
                sock = urllib2.urlopen(url)
                documentType = sock.info().getheader("Content-Type","text/html").split(";")[0]
                htmlSource = sock.read()
                sock.close()
        except Exception,e:
                #print url,e
                logger.warn("url:%s,%s" % (url, str(e)))
                return ("","")
        
        return (htmlSource,documentType)

'''
负责将数据保存到数据库
'''        
class SaveData(Thread): 

        def __init__(self,dbfile):
		
                Thread.__init__(self)
                self.setDaemon(True)
                
                ##存储读取的html页面##
		self.DataQueue = Queue()
        
                self.state = True
                
                self.dbfile = dbfile
                
                self.start()
        '''
    将DataQueue保存到数据库
    '''
        def save(self):
                try:
                        count = self.DataQueue.qsize()
                        logger.debug("save data,count:%s",count)
                        
                        data = []
                        for x in xrange(count):
                                data.append(self.DataQueue.get())
                                self.DataQueue.task_done()
                        self.dataBase.save(data)
                except Exception,e:
                        #print "insert into databse fail:",str(e)
                        logger.error("insert into databse fail:%s" % e)
        
        def run(self):
                self.dataBase = DataBase(self.dbfile)
                while  self.state:
                        
                        self.save()
                        time.sleep(2)
                self.save()
                self.dataBase.close()
                
        def stop(self):
               
                self.state = False
'''
爬虫类，启动一个线程
'''

class Crawler(Thread):
        
	def __init__(self,options):
		
                Thread.__init__(self)
                #reload(sys)
                #sys.setdefaultencoding('utf-8')
                self.setDaemon(True)
                
                #用来存储需要被爬取的url，格式#
                #[url,depth],depth 代表url的深度#
		self.urlQueue = Queue()
        
                ##存储已经爬取过的url##
		self.fecthedUrl = []
		self.fetchedQueue = Queue()
		##线程数##
		self.nums_threads = options.thread
		
                ##页面需要包含的关键字##
                self.key = options.key.decode(locale.getdefaultlocale()[1]).replace(",","|")
                
                ##保存的文件扩展名##
                self.downloadext = []
                if options.downloadfile != "":
                        self.downloadext = options.downloadfile.split(",")
                
                ##爬取的深度##
		self.depth = int(options.depth)
        
		##单独启动一个线程保存数据##
                self.dbfile = options.dbfile
                self.saveData = SaveData(options.dbfile)
                self.DataQueue = self.saveData.DataQueue
		##创建线程池##
		self.threadPool = ThreadPool(self.nums_threads)
		
                ##初始化线程队列##
                
		self.urlQueue.put([options.url.decode(locale.getdefaultlocale()[1]),1])
			
		#当前爬行深度
		self.currentDepth = 1
        
		#当前程序运行状态
		self.state = False

	
        '''
    线程池中的线程运行该函数，获得url的html内容
    depth 代表出入url的深度
    '''
	def Worker(self,url,depth):
		
                depth = int(depth)+1
                try:
			htmlSource,documentType = get_Data(url)
                        
                        if htmlSource == "" and documentType == "":
                                self.fetchedQueue.put(url)
                                return 
		except Exception,e:
                        self.fetchedQueue.put(url)
			logger.warn(e)
			return 
		'''
        判断文档类型,仅解析html文档,其他文件一概存到本地
        '''
                
                if documentType not in ["text/html"]:
                        self.saveFile(url,htmlSource)
                        self.fetchedQueue.put(url)
                        return
                try:       
                        soup = BeautifulSoup(htmlSource)
                except Exception,e:
                        logger.error("parse html fail.url:%s",url)
                        logger.warn(str(e))
                        self.fetchedQueue.put(url)
                        return 
                try:
                        AbsolutePath = "%s//%s" % (url.split("//")[0],url.split("//",1)[1].split("/",1)[0])
                        RelativePath = "%s//%s" % (url.split("//")[0],url.split("//",1)[1].rsplit("/",1)[0])
                        
                        if not RelativePath.endswith("/"):
                                RelativePath = RelativePath + "/"
                        '''
            获得当前页面所有的链接,线程池中的线程仅将链接放到队列，
            不判断链接是否需要爬取
            '''
                        def href(tag):
                                return tag.has_attr("href")
                        def src(tag):
                                return tag.has_attr("src")

                        Tag =  [each["href"] for each in soup.find_all(href)]
                        Tag = Tag + [each["src"] for each in soup.find_all(src)]
                
                        for each in Tag:
                                ##绝对路径##
                                if each.startswith('/'):
                                        each = "%s%s" % (AbsolutePath,each)
                                            
                                elif each.startswith(("http://","https://")):
                                        pass
                                ##相对路径##
                                else:
                                        each= "%s%s" % (RelativePath,each)
                                self.urlQueue.put([each,depth])
                
                        ##保存当前页面，仅添加到队列，并不保存到数据库##
                        
                        if self.key == "" or  soup.find_all(text = re.compile(self.key)):
                                logger.debug("put url:%s data in dataqueue" % url)
                                
                                self.DataQueue.put((url,(",").join(self.key),htmlSource))
                         
                except Exception,e:
                         logger.error("parse url :%s,%s" % (url,str(e)))
                         self.fetchedQueue.put(url)
                         return 
                self.fetchedQueue.put(url)
        ##相关文件保存在本地,用网站域名做文件夹，网页最后一个路径做文件名##       
	def saveFile(self,url,htmlSource):
                logger.info("save file,url:%s" % url)
                
                root_url = url.split("//",1)[1].split("/",1)[0]
                if not os.path.isdir(root_url):
                        os.makedirs(root_url)
                '''
        url最后一个/后面的作为文件名。为了防止test.js?0.2222
        形式，去掉最后一个?
        ext 为扩展名
        '''
                filename = url.rsplit("/",1)[1].rsplit("?",1)[0]
               
                ext  = filename.rsplit(".",1)[1]
                
                if self.downloadext and ext not in self.downloadext:
                        return 
                try:
                        f = open("%s/%s" % (root_url,filename),"wb")
                        f.write(htmlSource)
                        f.close()
                except Exception,e:
                        logger.warn("save file fail,url:%s,except:%s" % (url,e))
 
        
        
        def run(self):
		self.state = True
		print '     Start Crawling   \n'
                logger.info("   Start Crawling  ")
		while self.state:
                        
                        '''
            队列为空，判断是否要结束.
            判断标准:
                线程池任务队列全部完成
                url队列为空
            '''
                        if self.urlQueue.empty():
                                logger.debug("urlQueue is empty")
                                self.threadPool.wait()
                                logger.debug("threadPool's taskQueue is empty")
                                if self.urlQueue.empty():
                                        ##任务已结束，将未保存的DataQueue内容保存起来##
                                        logger.debug("task is over")
                                        self.saveData.stop()
                                        self.saveData.join()
                                        self.stop()
                                        print '\n Crawling Over.........\n'
                                        logger.info("  Crawling Over  ")
                                        break
                        
                        task = self.urlQueue.get()
                        ##深度满足要求，且urk未被获取则添加到待处理列表##
                        if int(task[1]) <= self.depth and task[0] not in self.fecthedUrl:
                                
                                logger.debug("add a task url:%s,depth:%s" % (task[0],task[1]))
                                self.threadPool.addTask(self.Worker,task[0],task[1])
                                self.fecthedUrl.append(task[0])
                                self.currentDepth = task[1]
                        self.urlQueue.task_done()
                                
        '''
    返回当前状态，包括现在线程池线程个数
    当前任务队列数量
    '''
        def getInfo(self):
                return  (self.threadPool.num_workers,self.fetchedQueue.qsize(),self.threadPool.TaskQueue.qsize() + self.urlQueue.qsize(),self.currentDepth)
                
	def stop(self):
		self.state = False
		self.threadPool.stop()
        
        