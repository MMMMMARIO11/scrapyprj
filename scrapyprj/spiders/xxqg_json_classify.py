import requests
import scrapy
import re
from scrapyprj.items import xuexiqiangguo_Item
import pymysql
from datetime import datetime

class XxqgJsonClassifySpider(scrapy.Spider):
    name = "xxqg_json_classify"
    allowed_domains = ["www.xuexi.cn"]

    def start_requests(self):
        # global last_item_count  # 声明全局变量

        # mysql数据库配置
        MYSQL_HOST = '192.168.1.148'
        MYSQL_PORT = 3306
        MYSQL_USER = 'root'
        MYSQL_PASSWORD = 'Fengjiaqi1'
        MYSQL_DATABASE = 'rag_data'

        # 连接到MySQL数据库
        connection = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        json_urls = ["https://www.xuexi.cn/lgdata/1jscb6pu1n2.json?_st=26095725", #重要新闻
            "https://www.xuexi.cn/lgdata/132gdqo7l73.json?_st=26095749",  # 重要讲话
            "https://www.xuexi.cn/lgdata/1crqb964p71.json?_st=26095757", #头条新闻
            "https://www.xuexi.cn/lgdata/u1ght1omn2.json?_st=26096137" #学习理论
        ]
        for i, json in enumerate(json_urls, start=1):
            response = requests.get(json)
            json_data_list_all = response.json()
            current_item_count = len(json_data_list_all)

            print(f"当前JSON_{i} 列表中的总项数为: {current_item_count}")
            try:
                with connection.cursor() as cursor:
                    # 查询 '上一次'重要讲话'json列表总项数' 列的数据
                    column_name = f"上一次json_{i}列表总项数"

                    sql_select_last_item_count = f"SELECT `{column_name}` FROM jsonlist_count"
                    cursor.execute(sql_select_last_item_count)

                    # 获取查询结果
                    result = cursor.fetchone()
                    if result:
                        last_item_count = int(result[column_name])
                        print(f"上一次json_{i}列表总项数: {last_item_count}")
                    else:
                        print(f"没有找到 {column_name} 的值")

                    articles_to_scrape = current_item_count - last_item_count

                    # 创建SQL更新语句
                    sql_update_last_item_count = f"UPDATE jsonlist_count SET `{column_name}` = %s"
                    cursor.execute(sql_update_last_item_count, (current_item_count,))

                    column_name_update = f"json_{i}列表更新项数"
                    sql_update_articles_to_scrape = f"UPDATE jsonlist_count SET `{column_name_update}` = %s"
                    cursor.execute(sql_update_articles_to_scrape, (articles_to_scrape,))

                    update_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                    column_name_update_time = f"json_{i}列表更新时间"
                    sql_update_update_time = f"UPDATE jsonlist_count SET `{column_name_update_time}` = %s"
                    cursor.execute(sql_update_update_time, (update_time,))

                # 提交事务
                connection.commit()

                if articles_to_scrape > 0:

                    print(f"需要爬取最新的 {articles_to_scrape} 篇文章。")
                    json_data_list = json_data_list_all[:articles_to_scrape]  # 获取最新的文章列表
                    # 更新 last_item_count
                    # last_item_count = current_item_count
                    # print(f"After function call: last_item_count = {last_item_count}")
                    # urls = [item["url"] for item in json_data_list]
                    for json_data in json_data_list:
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
                            #print(normalized_content)

                            # 返回 数据到parse
                            yield scrapy.Request(url=url, meta={'json_data': json_data,
                                                                'normalized_content': normalized_content, 'json_index': i},
                                                 callback=self.parse)

                            with connection.cursor() as cursor:
                                # 准备插入的数据
                                spider_status = '爬取成功'
                                crawl_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                                itemid = json_data["itemId"]
                                title = json_data["title"]

                                # 插入数据的 SQL 语句
                                sql_insert_log = "INSERT INTO spider_log (`学习强国网站爬取状态`, `文章id`, `文章标题`, `爬取时间`) VALUES (%s, %s, %s, %s)"
                                cursor.execute(sql_insert_log, (spider_status, itemid, title, crawl_time))

                                # 提交数据库事务
                            connection.commit()

                        else:
                            print("No match found for normalized_content")
                            with connection.cursor() as cursor:
                                # 准备插入的数据
                                spider_status = '爬取失败'
                                crawl_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

                                # 插入数据的 SQL 语句
                                sql_insert_log = "INSERT INTO spider_log (`学习强国网站爬取状态`, `爬取时间`) VALUES (%s, %s)"
                                cursor.execute(sql_insert_log, (spider_status, crawl_time))

                                # 提交数据库事务
                            connection.commit()

                else:
                    print("没有发现新增的文章，无需爬取。")


            except Exception as e:

                print(f"获取 MySQL 数据失败: {e}")


            #finally:

                #connection.close()  # 关闭数据库连接

    def parse(self, response):

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




