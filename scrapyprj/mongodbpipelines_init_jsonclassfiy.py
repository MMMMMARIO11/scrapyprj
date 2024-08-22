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
            collection_name = self._get_collection_name(item, spider)
            # 指定插入到 xxqg 集合中
            self.db[collection_name].insert_one({
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

    def _get_collection_name(self, item, spider):
        # 根据 spider 的名称或其他标识，确定存储的集合名称
        # 这里假设集合名称是根据 json_index 来确定的
        json_index = item.get('json_index')
        return f'xxqg_{json_index}'