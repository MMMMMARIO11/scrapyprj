# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import csv


class ScrapyprjPipeline:
    def __init__(self): 
        self.file = open('xxqg_output.csv', 'w', newline='', encoding='utf-8')
        self.csv_writer = csv.DictWriter(self.file, fieldnames=['url', 'channelNames','auditTime','showSource','title' , 'xxqg_text'])
        self.csv_writer.writeheader()




    def process_item(self, item, spider):
        self.csv_writer.writerow({
            'url': item.get('url', ''),
            'channelNames': ','.join(item.get('channelNames', [])),            
            'auditTime': item.get('auditTime', ''),            
            'showSource': item.get('showSource', ''),
            'title': item.get('title', ''),
            'xxqg_text': item.get('xxqg_text', '')
        })
        
        return item


    def close_spider(self, spider):
        self.file.close()
