# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class xuexiqiangguo_Item(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    url = scrapy.Field()
    channelNames = scrapy.Field()
    showSource = scrapy.Field()
    auditTime = scrapy.Field()
    title = scrapy.Field()
    xxqg_text = scrapy.Field()
    pass

class qiushi_Item(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    
    qiushi_text = scrapy.Field()
    pass