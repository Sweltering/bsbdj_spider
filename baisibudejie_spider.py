import requests
from lxml import etree
from queue import Queue
import threading
import csv


# 生产者生产段子内容和url
class Producer(threading.Thread):
    def __init__(self, page_queue, paragraph_queue, *args, **kwargs):
        super(Producer, self).__init__(*args, **kwargs)
        self.page_queue = page_queue
        self.paragraph_queue = paragraph_queue

    def run(self):
        while True:
            if self.page_queue.empty():
                break
            url = self.page_queue.get()  # 从队列中取出url
            self.parse_page(url)

    # 爬取段子内容及对应的url
    def parse_page(self, url):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36',
        }
        base_url = 'http://www.budejie.com'
        urls = []  # 存放段子对应的url
        response = requests.get(url, headers=headers)
        html = response.text

        ehtml = etree.HTML(html)
        paragraph_contents = ehtml.xpath('//div[@class="j-r-c"]//div[@class="j-r-list-c-desc"]/a/text()')  # 爬取段子内容
        paragraph_urls = ehtml.xpath('//div[@class="j-r-c"]//div[@class="j-r-list-c-desc"]/a/@href')  # 爬取对应的url
        for paragraph_url in paragraph_urls:
            urls.append(base_url + paragraph_url)
        paragraphs = zip(paragraph_contents, urls)  # 段子的内容和url
        for paragraph in paragraphs:
            self.paragraph_queue.put(paragraph)  # 将段子内容和url放进队列中
        print("=====================第{}页下载完成=========================".format(url.split('/')[-1]))


# 消费者保存数据到本地
class Consumer(threading.Thread):
    def __init__(self, paragraph_queue, gLock, writer, *args, **kwargs):
        super(Consumer, self).__init__(*args, **kwargs)
        self.paragraph_queue = paragraph_queue
        self.lock = gLock
        self.writer = writer

    def run(self):
        while True:
            try:
                content, url = self.paragraph_queue.get()
                self.lock.acquire()
                self.writer.writerow((content, url))
                self.lock.release()
                print("保存一条成功")
            except:
                break


# 获取每一页的url
def main():
    page_queue = Queue(10)
    paragraph_queue = Queue(500)
    gLock = threading.Lock()
    f = open('bsbdj.csv', 'a', newline="", encoding='utf-8')
    writer = csv.writer(f)
    writer.writerow(('content', 'url'))
    for x in range(1, 11):
        url = 'http://www.budejie.com/{}'.format(x)
        page_queue.put(url)

    # 5个生产者
    for x in range(5):
        t = Producer(page_queue, paragraph_queue)
        t.start()

    # 5个消费者
    for x in range(5):
        t = Consumer(paragraph_queue, gLock, writer)
        t.start()


if __name__ == '__main__':
    main()
