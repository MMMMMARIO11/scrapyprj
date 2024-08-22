import pymongo
from scrapy.exceptions import DropItem

class MongoDBPipeline_init:
    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        mongo_uri = crawler.settings.get('MONGO_URI', 'mongodb://localhost:27017/') #将setting.py中服务器地址穿过来
        mongo_db = crawler.settings.get('MONGO_DATABASE', 'scrapy_data') #要传输给mongodb的数据库名称

        return cls(
            mongo_uri=mongo_uri,
            mongo_db=mongo_db
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        try:
            # 指定插入到 xxqg 集合中
            self.db['xxqg_1'].insert_one({
                'url': item.get('url', ''),
                'channelNames': ','.join(item.get('channelNames', [])),
                'auditTime': item.get('auditTime', ''),
                'showSource': item.get('showSource', ''),
                'title': item.get('title', ''),
                'xxqg_text': item.get('xxqg_text', '')
            })
            return item
        except Exception as e:
            raise DropItem(f"Error inserting item into MongoDB: {e}")
