# -*- coding: utf-8 -*- 


import time
from Queue import Queue
from threading import Thread  

 
##线程池类##
class ThreadPool(object):
    
    def __init__(self,num_workers):
        
        ##任务队列##
        self.TaskQueue = Queue()
        
        #线程池
        self.threadPool = []

        #线程数目
        self.num_workers = 0
        
        self.createWorkers(num_workers)
    
    ##创建工作线程,num_workers为个数##
    def createWorkers(self,num_workers):
        for x in xrange(num_workers):
            self.threadPool.append(WorkerThread(self.TaskQueue))
        self.num_workers += num_workers
            
    ##销毁工作线程,num_workers为个数##
    def destroyWorkers(self,num_workers):

        for  x in xrange(min(num_workers,self.num_workers)):        
            t = self.threadPool[x]
            t.kill()
                    
        self.num_workers -= min(num_workers,self.num_workers)
    
    ##添加任务##
    def addTask(self,func,*args,**kargs):
            
        self.TaskQueue.put((func,args,kargs),False)
    
    ##停止线程池中所有线程##
    def stop(self):
            
        self.destroyWorkers(self.num_workers)
    
    ##等待队列为空结束##
    def wait(self):
            
        ##没有工作线程##
        if self.num_workers == 0 :
            return 
            
        self.TaskQueue.join()
                

##工作线程##                
class WorkerThread(Thread):

    def __init__(self,TaskQueue=[]):
        Thread.__init__(self)
        self.TaskQueue = TaskQueue
        self.setDaemon(True)
        self.state = True  
        self.start()

    def run(self):

        while self.state:
            ##队列为空，则等待##
            if self.TaskQueue.empty():
                time.sleep(0.1)
                continue
            ##从队列取一个任务并执行##
            try:
                func,args,kargs = self.TaskQueue.get()
            except: 
                continue
            try:
                func(*args,**kargs)
                self.TaskQueue.task_done()
            except Exception,e:
                print str(e)
                self.TaskQueue.task_done()
                continue
                                
	#结束线程
    def kill(self):
        self.state =  False

