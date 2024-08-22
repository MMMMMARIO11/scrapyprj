import pymongo
from scrapy.exceptions import DropItem
import pymysql
from datetime import datetime

class mongodbpipelines_jsonclassify:
    def __init__(self, mongo_uri, mongo_db, mysql_params):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mysql_params = mysql_params

    @classmethod
    def from_crawler(cls, crawler):
        mongo_uri = crawler.settings.get('MONGO_URI', 'mongodb://localhost:27017/') #将setting.py中服务器地址传过来
        mongo_db = crawler.settings.get('MONGO_DATABASE', 'scrapy_data') #要传输给mongodb的数据库名称
        mysql_params = {
            'host': crawler.settings.get('MYSQL_HOST', 'localhost'),
            'port': crawler.settings.get('MYSQL_PORT', 3306),
            'user': crawler.settings.get('MYSQL_USER', 'root'),
            'password': crawler.settings.get('MYSQL_PASSWORD', ''),
            'database': crawler.settings.get('MYSQL_DATABASE', 'rag_data'),
            'charset': 'utf8mb4',
            #'cursorclass': pymysql.cursors.DictCursor,
        }
        return cls(
            mongo_uri=mongo_uri,
            mongo_db=mongo_db,
            mysql_params=mysql_params
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.conn = pymysql.connect(**self.mysql_params)
        self.cursor = self.conn.cursor()

    def close_spider(self, spider):
        self.client.close()
        self.conn.close()

    def process_item(self, item, spider):
        try:
            collection_name = self._get_collection_name(item)
            # 指定插入到 xxqg 集合中
            #print(item.get('xxqg_text'))
            self.db[collection_name].insert_one({
                'url': item.get('url', ''),
                'channelNames': ','.join(item.get('channelNames', [])),
                'auditTime': item.get('auditTime', ''),
                'showSource': item.get('showSource', ''),
                'title': item.get('title', ''),
                'xxqg_text': item.get('xxqg_text', '')
            })
            #inserted_doc = self.db[collection_name].find_one({'url': item.get('url', '')})
            #print('Inserted Document:', repr(inserted_doc))
            # 写入mysql日志
            spider_status = '写入成功'
            crawl_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            title = item.get('title', '')

            # 插入数据的 SQL 语句
            sql_insert_log = "INSERT INTO db_log (`写入mongodb数据库状态`, `文章标题`, `写入时间`) VALUES (%s, %s, %s)"

            self.cursor.execute(sql_insert_log, (spider_status, title, crawl_time))
            self.conn.commit()

            return item
        except Exception as e:
            #self.conn.rollback()  # 回滚事务

            spider_status = '写入失败'
            crawl_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

            # 插入数据的 SQL 语句
            sql_insert_log = "INSERT INTO db_log (`写入mongodb数据库状态`, `写入时间`) VALUES (%s, %s)"

            self.cursor.execute(sql_insert_log, (spider_status, crawl_time))
            self.conn.commit()

            raise DropItem(f"Error inserting item into MongoDB: {e}")

    def _get_collection_name(self, item):
        # 根据 spider 的名称或其他标识，确定存储的集合名称
        # 这里假设集合名称是根据 json_index 来确定的
        json_index = item.get('json_index')
        return f'xxqg_{json_index}'