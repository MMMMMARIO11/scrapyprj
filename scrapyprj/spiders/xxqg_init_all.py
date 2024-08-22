import requests
import scrapy
import re
from scrapyprj.items import xuexiqiangguo_Item
from bs4 import BeautifulSoup


class XxqgInitAllSpider(scrapy.Spider):
    name = "xxqg_init_all"
    allowed_domains = ["www.xuexi.cn"]

    def start_requests(self):
        json_urls = [  # "https://www.xuexi.cn/lgdata/1jscb6pu1n2.json?_st=26095725", #重要新闻
            "https://www.xuexi.cn/lgdata/132gdqo7l73.json?_st=26095749"  # 重要讲话
            # "https://www.xuexi.cn/lgdata/1crqb964p71.json?_st=26095757", #头条新闻
            # "https://www.xuexi.cn/lgdata/u1ght1omn2.json?_st=26096137" #学习理论
        ]
        for i, json in enumerate(json_urls, start=1):
            response = requests.get(json)
            json_data_list_all = response.json()
            item_count = len(json_data_list_all)

            print(f"JSON 列表中的总项数为: {item_count}")
            json_data_list = json_data_list_all[201:251]  # 为了避免爬取风险，建议分段爬取，根据栏目文章数量手动设置每次需要爬取的文章数量
            for json_data in json_data_list:
                if json_data["producer"] == "采编系统":

                    url = json_data["url"]
                    id_param = url.split('id=')[1].split('&')[0]

                    # 构建 JSON 链接
                    json_url = f'https://boot-source.xuexi.cn/data/app/{id_param}.js'
                    response = requests.get(json_url)
                    json_string = response.text

                    # 使用正则表达式提取 normalized_content 中的文本内容
                    match = re.search(r'"normalized_content":"([^"]*)"', json_string)
                    if match:
                        normalized_content_init = match.group(1)
                        normalized_content = normalized_content_init.replace(" ", "\n")
                        # 打印提取的文本内容
                    else:
                        normalized_content = ''
                        print("No match found for normalized_content")

                elif json_data["producer"] == "旧PC站":
                    url = json_data["url"]
                    match = re.search(r"https://www.xuexi.cn/([^/]+)/([^/]+)\.html", url)

                    if match:
                        first_segment = match.group(1)
                        second_segment = match.group(2)

                        # 构建JSON链接
                        json_url = f"https://www.xuexi.cn/{first_segment}/data{second_segment}.js"

                        print(json_url)
                        response = requests.get(json_url)
                        json_text = response.text

                        pattern = r'"content":"(.*?)","'

                        # 使用正则表达式查找匹配项
                        match = re.search(pattern, json_text)

                        if match:
                            content_html = match.group(1)  # 获取 "content" 字段中的 HTML 内容

                            # 使用 BeautifulSoup 解析 HTML，提取所有 <p> 标签的文本内容
                            soup = BeautifulSoup(content_html, 'html.parser')
                            paragraphs = soup.find_all('p')  # 查找所有 <p> 标签

                            # 遍历所有 <p> 标签，获取其文本内容并组合成一个字符串
                            content_text = '\n'.join([p.get_text(separator='\n') for p in paragraphs])

                            normalized_content = content_text.strip()  # 打印或进一步处理纯文本内容
                            #normalized_content = normalized_content.strip()
                            #normalized_content = normalized_content.replace('\n', '')

                else:
                    url = json_data["url"]
                    normalized_content = ''
                    print("未找到对应producer，请查找原因")

                yield scrapy.Request(url=url, meta={'json_data': json_data, 'normalized_content': normalized_content, 'json_index': i},
                                     callback=self.parse)

    def parse(self, response):

        items = []
        json_data = response.meta['json_data']
        normalized_content = response.meta['normalized_content']
        json_index = response.meta['json_index']

        item_data = {
            'url': json_data.get('url', ''),
            'channelNames': json_data.get('channelNames', ''),
            'showSource': json_data.get('showSource', ''),
            'auditTime': json_data.get('auditTime', ''),
            'title': json_data.get('title', ''),
            'xxqg_text': normalized_content,
            'json_index': json_index,
        }

        # 创建一个Item对象，将抓取的数据传递给Item Pipeline
        item = xuexiqiangguo_Item(**item_data)
        yield item

