import requests
from lxml import etree
from time import sleep
from dbPipeline import mysqlPipe
import re
from queue import Queue
import threading
import time
domain = 'http://ask.39.net'
base_url = "http://ask.39.net/news/"
regex_space = re.compile("[\n\s\t]")
regex_docNum = re.compile("http[s]*://my.39.net/(.+)")
regex_age = re.compile("(\d+)岁")
headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:64.0) Gecko/20100101 Firefox/64.0'}
lock = threading.Lock()
sql_obj = mysqlPipe(name='abc')
max_timeout = 3 #最大超时设置
max_attempts = 3 #最大重连次数
# 待爬取的问答页链接
inqueryUrl = Queue()
# 下载好的问答页
inqueryHtml = Queue()
# 待爬取的医生信息链接
doctorUrl = Queue()
# 查重
doctorUrlSeen = set()
# 下载好的医生信息页面:
doctorHtml = Queue()
# 待写入的问诊信息:
inqueryParsed = Queue()
# 待写入的医生信息:
doctorParsed = Queue()


def downloader(source, target):
    print(threading.current_thread())
    while True:
        url = source.get()
        i = 0
        while i < max_attempts:
            try:
                req_obj = requests.get(url=url, headers=headers, timeout=max_timeout)
                req_obj.encoding = 'utf8'
                print("url: %s 下载成功" % url)
            except:
                # 计数器
                i += 1
            else:
                target.put({
                    'url': url,
                    'html': req_obj.text
                })
                break
        if i == max_attempts:
            print("url: %s 已超过最大重试次数" % url)

def get_detail_page(html):
    tree = etree.HTML(html)
    urls = tree.xpath('//ul/li/span/p/a/@href')
    for index in range(len(urls)):
        urls[index]=domain+urls[index]
    for url in urls:
        inqueryUrl.put(url)

def get_inquery():
    while True:
        page = inqueryHtml.get()
        html = page['html']
        tree = etree.HTML(html)
        ans = ""
        #获取
        try:
            q_list = tree.xpath("//p[@class='txt_ms']/text()")[0]
        except:
            continue
        patient_info = tree.xpath("//p[@class='mation']/span/text()")
        doc_urls = tree.xpath("//div[@class='doc_img']/a/@href")
        answers = tree.xpath("//p[@class='sele_txt']/text()")
        #预处理
        question = re.sub(regex_space, '', string='患者:'+q_list)
        try:
            sex = re.sub(regex_space, '', string=patient_info[0])
        except:
            sex = ""
        try:
            age = re.sub(regex_space, '', string=patient_info[1])
            age = int(regex_age.search(age).group(1))
        except:
            age = 0
        for i in range(len(answers)):
            ans+='医生:'+answers[i]+'\n'
        for url in doc_urls:
            if url not in doctorUrlSeen:
                doctorUrlSeen.add(url)
                doctorUrl.put(url)
        ids = ""
        for url in doc_urls:
            try:
                id = regex_docNum.search(url).group(1)
            except:
                continue
            ids+=(id+'\n')
        #返回
        inqueryParsed.put({
            "content":question+'\n'+ans,
            "sex":sex,
            "age":age,
            "docID":ids
        })

def get_docinfo():
    while True:
        page = doctorHtml.get()
        html = page['html']
        tree = etree.HTML(html)
        #获取
        try:
            job = tree.xpath("//span[@class='job']/text()")[0]
        except:
            job = ""
        try:
            goodat = tree.xpath("//span[@class='J_article_content content']/text()")[0]
        except:
            goodat = ""
        try:
            hospital = tree.xpath("//span[@class='hospital']/text()")[0]
        except:
            hospital = ""
        try:
            clinic = tree.xpath("//div[@class='doctor-msg-job']/span/text()")[1]
        except:
            clinic = ""
        #预处理
        try:
            id = regex_docNum.search(page['url']).group(1)
        except:
            continue
        job = re.sub(regex_space, '', string=job)
        #返回:
        #print(id,job,hospital,clinic,goodat)
        doctorParsed.put({
            "id":id,
            "clinic":clinic,
            "job":job,
            "hospital":hospital,
            "goodat":goodat
        })


#写入数据库
def write_inquery():
    print(threading.current_thread())
    while True:
         inquery = inqueryParsed.get()
         #print(inquery)
         lock.acquire()
         sql_obj.insert_inquery_online(inquery['docID'],inquery['sex'],inquery['age'],inquery['content'])
         lock.release()

#写入数据库
def write_docInfo():
    print(threading.current_thread())
    while True:
         doc_info = doctorParsed.get()
         #print(doc_info)
         lock.acquire()
         sql_obj.insert_doctor_info(doc_info['id'], doc_info['clinic'], doc_info['job'], doc_info['hospital'],
                                doc_info['goodat'])
         lock.release()

def pin():
    while True:
        time.sleep(10)
        print("当前待抓取的链接数: inquery:%d doctor:%d" % (inqueryUrl.qsize(), doctorUrl.qsize()))
        print("当前已下载网页数量: inquery:%d dcotor:%d"%(inqueryHtml.qsize(),doctorHtml.qsize()))

def taskManager():
    tasks = []
    for line in open('tasks'):
        line = line.strip('\n')
        tasks.append(line)
    #上次抓取313时内存不足
    for task in tasks:
        for page in range(1, 1001):
            print("正在爬取科室 %s 第 %d 页....." % (task, page))
            page_url = base_url + task + '-' + str(page) + '.html'
            try:
                req_obj = requests.get(url=page_url, headers=headers, timeout=3)
            except:
                print("第 %d 页请求超时" % page)
                continue
            req_obj.encoding = 'utf8'
            html = req_obj.text
            try:
                get_detail_page(html)
            except:
                print("url获取失败")
    # inqueryUrl.put("http://ask.39.net/question/57684417.html")

if __name__ == '__main__':
    # 定义各线程
    threads = []
    # 探针
    threads.append(threading.Thread(target=pin, args=()))
    # 任务管理器
    threads.append(threading.Thread(target=taskManager, args=()))
    # 下载器
    # detail_url_old.put("https://www.haodf.com/wenda/fwliuhaibo_g_6798775932.htm")
    threads.append(threading.Thread(target=downloader, args=(inqueryUrl, inqueryHtml)))
    threads.append(threading.Thread(target=downloader, args=(inqueryUrl, inqueryHtml)))
    threads.append(threading.Thread(target=downloader, args=(inqueryUrl, inqueryHtml)))
    threads.append(threading.Thread(target=downloader, args=(inqueryUrl, inqueryHtml)))
    threads.append(threading.Thread(target=downloader, args=(inqueryUrl, inqueryHtml)))
    threads.append(threading.Thread(target=downloader, args=(doctorUrl, doctorHtml)))
    threads.append(threading.Thread(target=downloader, args=(doctorUrl, doctorHtml)))
    # 解析器
    threads.append(threading.Thread(target=get_docinfo, args=()))
    threads.append(threading.Thread(target=get_docinfo, args=()))
    threads.append(threading.Thread(target=get_inquery, args=()))
    threads.append(threading.Thread(target=get_inquery, args=()))
    # 数据库模型
    threads.append(threading.Thread(target=write_docInfo, args=()))
    threads.append(threading.Thread(target=write_inquery, args=()))
    # 启动各线程
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

