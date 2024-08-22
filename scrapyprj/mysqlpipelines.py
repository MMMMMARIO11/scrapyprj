# pipelines.py

import pymysql
from scrapy.exceptions import DropItem
from scrapy.utils.project import get_project_settings

class MySQLPipeline:
    def __init__(self):
        settings = get_project_settings()
        self.host = settings.get('MYSQL_HOST')
        self.port = settings.get('MYSQL_PORT')
        self.user = settings.get('MYSQL_USER')
        self.password = settings.get('MYSQL_PASSWORD')
        self.database = settings.get('MYSQL_DATABASE')

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def open_spider(self, spider):
        self.conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8mb4'
        )
        self.cursor = self.conn.cursor()

    def close_spider(self, spider):
        self.conn.commit()
        self.conn.close()

    def process_item(self, item, spider):
        try:
            insert_query =   "INSERT INTO xxqg (url, channelNames, auditTime, showSource, title, xxqg_text) VALUES (%s, %s, %s, %s, %s, %s)"
            self.cursor.execute(insert_query, (
                item['url'],
                ','.join(item['channelNames']),
                item['auditTime'],
                item['showSource'],
                item['title'],
                item['xxqg_text']
            ))
        except Exception as e:
            self.conn.rollback()
            raise DropItem(f"Failed to insert item into MySQL: {e}")
        
        return item
