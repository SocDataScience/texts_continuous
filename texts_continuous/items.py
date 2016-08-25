# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

# Get the BlogTexts for the Blogtext table in the DB:
class BlogTextItem(scrapy.Item):
    posttime = scrapy.Field()
    postdate = scrapy.Field()
    permalink = scrapy.Field()
    posttitle = scrapy.Field()
    posttext = scrapy.Field()
    numbercomments = scrapy.Field()
    commenturl = scrapy.Field()
    blogger = scrapy.Field()
    blogurl = scrapy.Field()
    addedtodb = scrapy.Field()
    pagenumber = scrapy.Field()
