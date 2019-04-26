import requests
from lxml import etree
from dbPipeline import mysqlPipe
if __name__ == '__main__':
    sqlObj = mysqlPipe(name='39asknew')
    domain = "http://ask.39.net"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:64.0) Gecko/20100101 Firefox/64.0'}
    url = "http://ask.39.net/browse/all_321.html"
    req_obj = requests.get(url=url,headers=headers)
    tree = etree.HTML(req_obj.text)
    urls = tree.xpath("//ul[@class='tag-all-menu']/li/a/@href")
    for i in range(len(urls)):
        urls[i]=domain+urls[i]
    for url in urls:
        req_obj = requests.get(url=url,headers=headers)
        tree = etree.HTML(req_obj.text)
        departments = tree.xpath("//div[@class='tg-box']/dl")  # 所有科室项
        for department in departments:
            title = department.find("dt").text
            tags = department.findall(".//dd/a")
            for i in range(len(tags)):
                tags[i] = tags[i].text
            diseases = '\n'.join(tags)
            sqlObj.insert_department_info(title, diseases)
