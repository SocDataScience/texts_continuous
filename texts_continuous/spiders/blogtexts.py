# -*- coding: utf-8 -*-
# Crawling all the blog post content!!

# This crawler should run on PYTHON 2!!!

# *********************** #
# Import all necessary modules:

from scrapy.spiders import Spider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.selector import HtmlXPathSelector
from scrapy import Request

import csv # reading and writing csv files properly

import re
import time
import random

from texts_continuous.items import BlogTextItem


import sqlite3 as lite

con = None

import platform
import sys # for using command line arguments
# *********************** #
# Define the BlogTextSpider class:

class BlogTextSpider(Spider):

    name = "texts_continuous"
    allowed_domains = ["blogger.ba"]

    # *********************** #
    global con
    if platform.system() == "Darwin":
        con = lite.connect('/Users/Annerose/Documents/15-16/Data/texts_continuous.db')
    if platform.system() == "Linux":
        con = lite.connect('/home/annerose/Python/texts_continuous.db')
    global cur
    cur = con.cursor()
    con.text_factory = str

    # create blogtexts table if not exists:
    cur.execute("CREATE TABLE IF NOT EXISTS \
    Blogtexts(\
    PostID INTEGER PRIMARY KEY, permalink TEXT, posttime TEXT,\
    postdate TEXT,  posttitle TEXT, \
    posttext TEXT, \
    numbercomments TEXT, commenturl TEXT, \
    blogger TEXT, ID INTEGER, \
    blogurl TEXT, blogID INTEGER, pagenumber INTEGER, addedtodb TEXT)")


    # Select the start_urls:
    # 1. get all the blogurl values that are only present in the Blogurls table,
    # but not yet in the Blogtext table:

    cur.execute("SELECT blogurl \
    FROM blogurls \
    WHERE blogID NOT IN (SELECT DISTINCT blogurls.ID \
    FROM blogurls, blogtexts WHERE blogurls.blogID = blogtexts.blogID)")
    blogurls = cur.fetchall()

    # Use this approach if you really want to get ALL blogtexts:
    # cur.execute("SELECT blogurl, MAX(pagenumber) \
    # FROM Blogtexts \
    # WHERE Blogtexts.blogurl IN \
    # (SELECT DISTINCT Blogurls.blogurl FROM Blogurls, Blogtexts WHERE Blogurls.blogurl = Blogtexts.blogurl) \
    # AND Blogtexts.blogurl NOT IN (SELECT DISTINCT Blogtexts.blogurl \
    # FROM Blogtexts WHERE Blogtexts.pagenumber=='last page' OR Blogtexts.pagenumber=='empty blog') \
    # GROUP BY blogurl")
    # blogurls_continue = cur.fetchall()

    # Use this approach if 50 blogtexts per blog are enough:
    cur.execute("SELECT blogurl, MAX(pagenumber) \
    FROM Blogtexts \
    WHERE blogurl IN \
    (SELECT DISTINCT Blogurls.blogurl FROM Blogurls, Blogtexts WHERE Blogurls.blogurl = Blogtexts.blogurl) \
    AND blogurl NOT IN (SELECT DISTINCT Blogtexts.blogurl \
    FROM Blogtexts WHERE Blogtexts.pagenumber=='last page' OR Blogtexts.pagenumber=='empty blog') \
    AND blogurl IN (SELECT blogurl FROM \
    (SELECT blogurl, COUNT(*) as c FROM Blogtexts GROUP BY blogurl) WHERE c <50) \
    GROUP BY blogurl")
    blogurls_continue = cur.fetchall()

    # 2. get all the blogurl that are already in the Blogtext table:


    if not blogurls:
        print "=== ERROR/WARNING MESSAGE FROM CRAWLER blogtexts: ===\n" \
          "=== No new Blogtext entries to scrape!! \n" \
          "=== Get instead more blogurls into Bloggerurl table."

    else:
        blogurls = [i[0] for i in blogurls]
        if not blogurls_continue:
            blogurls_continue = []
            # # take out the blogs which have been found to be empty:
            # blogurls_continue1 = [i for i in blogurls_continue if i[1]!="empty blog"]
            #
            # # I comment out the following line so that empty blogs aren't
            # # scraped again when restarting the scrawler:
            # # blogurls_continue2 = [(i[0], 1) for i in blogurls_continue if i[1]=="empty blog"]
            # blogurls_continue = blogurls_continue1 # + blogurls_continue2

        blogurls_continue = [("all-the-best", 0)] #debug
        blogurls = ["1711on"] # debug

        # start_urls = [("http://" + i + ".blogger.ba/arhiva/?start=" + str(j - 20 if j - 20 >= 0 else 0)) for i, j in
        #               blogurls_continue] # debug

        # start_urls = [("http://" + i + ".blogger.ba/arhiva/?start=" + str(j-20 if j-20>=0 else 0)) for i, j in blogurls_continue] + \
        # [("http://" + i +  ".blogger.ba/arhiva/?start=0") for i in blogurls]

        # len(start_urls)
        # testing start_url:
        start_urls = ["http://sion.blogger.ba/arhiva/?start=0"] # debug start_url

        print(start_urls)

        print "=============\n Number of start_urls for Blogtexts: %s\n=============" % len(start_urls)


    # con.close() # shouldn't be closed as con is defined as global variable.


    # *********************** #
    # *********************** #

    # Once I have the urls, scrape the urls for responses:

    # Make sure even the texts from the start page are crawled:
    def parse(self, response):
        return self.parse_item(response)

    def parse_item(self, response): # class CrawlSpider uses parse_item, class BaseSpider takes only parse.


        # *********************** #

        # Procedure to follow if the blog is not entirely empty
        # (i.e. contains at least one single post), and the blog is
        # not hosted by a different domain than blogger.ba:

        # define procedure to be followed for every individual post:
        if response.xpath("//div[@class='post']"):

            for sel in response.xpath("//div[@class='post']"):

            # *********************** #
                try: # some posts have invalid postdates etc.
                    # using "help" circumvents that the scraper stops
                    # following a given start_url as soon as it finds one single
                    # invalid postdate etc.

            # define posttime:
                    if sel.xpath("p[@class='footer-posta']/text()").re('\d{2}:\d{2}'):
                        posttime = sel.xpath("p[@class='footer-posta']/text()").re('\d{2}:\d{2}')[0]
                    else:
                        posttime = ''

            # postdate:
                    if sel.xpath("preceding::h5[@class='datum' and position()=1]/text()").extract():
                        postdate = [i[:-1] for i in sel.xpath("preceding::h5[@class='datum' and position()=1]/text()").extract()][0]
                    # make nice formatting of postdate ("2016-01-01" instead of 01.01.2016):
                        postdate = postdate.split(".")
                        postdate = "-".join([postdate[2], postdate[1], postdate[0]])
                    else:
                        postdate = ''

            # permalink to post (that's the url where the post is archived online):
                    if sel.xpath("p[@class='footer-posta']/a[@title='Permalink']/@href"):
                        permalink = sel.xpath("p[@class='footer-posta']/a[@title='Permalink']/@href").extract()[0]
                    # permalink = response.urljoin(permalink[0])
                        permalink = permalink[8:]
                    else:
                        permalink = ''

            # posttitle:
                    if sel.xpath("h4[@class='naslov-posta']/text()").extract():
                        posttitle = sel.xpath("h4[@class='naslov-posta']/text()").extract()[0]
                    else:
                        posttitle = ''


            # conditions for filling posttext ("reduce" not possible with empty posttext):
                    if sel.xpath("div[@class='body-posta']//text()"):
                        posttext = reduce((lambda x, y: x + y),
                                        sel.xpath("div[@class='body-posta']//text()").extract()).replace("\r\n",
                                                                                                       " ").strip().encode('utf8')
                    else:
                        posttext = ''

            # numbercomments if enabled by user:
                    if sel.xpath("p[@class='footer-posta']/a[@title='Komentari']/text()").re('\d+'):
                        numbercomments = sel.xpath("p[@class='footer-posta']/a[@title='Komentari']/text()").re('\d+')[0]
                    else:
                        numbercomments = 'Not enabled'

            # commenturl if enabled and if comments have been posted:
                    if sel.xpath("p[@class='footer-posta']/a[@title='Komentari']/@href").extract():
                        commenturl = sel.xpath("p[@class='footer-posta']/a[@title='Komentari']/@href").extract()[0]
                        commenturl = commenturl[32:]
                    else:
                        commenturl = ''

            # name of the blogger of the blog:
                    if sel.xpath("p[@class='footer-posta']/a[1]/@href"):
                        blogger = sel.xpath("p[@class='footer-posta']/a[1]/@href").extract()[0]
                        blogger = blogger.split("/")[-1]
                        # print "Scraping blogger %s " %blogger
                    else:
                        blogger = ''

            # url and pagenumber of the blog:
                    if re.search("(.blogger.ba/arhiva/)", response.url):
                        blogurl = response.url
                        blogurl = blogurl.split("/")[2]
                        blogurl = blogurl[:-11]

                        if response.xpath("//a[contains(text(), 'Stariji postovi')]"):
                            pagenumber = response.url.split("/")[-1]
                            pagenumber = pagenumber[7:]
                        else:
                            pagenumber = 'last page'

                    else:
                        blogurl = response.url
                        pagenumber = 'last page'



            # *********************** #

            # fill the item if the info as defined in the above code:
                    item = BlogTextItem()

                    item['posttime'] = posttime
                    item['postdate'] = postdate
                    item['permalink'] = permalink
                    item['posttitle'] = posttitle
                    item['posttext'] = posttext
                    item['numbercomments'] = numbercomments
                    item['commenturl'] = commenturl
                    item['blogger'] = blogger
                    item['blogurl'] = blogurl
                    item['addedtodb'] = time.strftime("%Y-%m-%d")
                    item['pagenumber'] = pagenumber

                    yield item

                except: # if there was an error message: just pass and continue
                    # with the next blogpost.
                    pass

            # *********************** #

            # extract the links which should be followed at the end of each page:
            # (do this only if there are any posts on the site; therefore this code
            # is subordered to the above code)

            if response.xpath("//a[contains(text(), 'Stariji postovi')]"):
                print "Contains older posts." # debug message

                url = response.xpath("//a[contains(text(), 'Stariji postovi')]/@href").extract()[0]
                current = response.url
                current = "/".join(current.split("/")[:3])
                url = "".join([current, url])

                print url # debug message

                yield Request(url, callback=self.check_lastpage)

            # *********************** #

        # How to fill the items if the blog is entirely emtpy
        # or is hosted inside a different domain than blogger.ba:
        else:
            # url of the blog:
            if re.search("(.blogger.ba)", response.url):
                blogurl = response.url
                blogurl = blogurl[7:-27]
            else:
                blogurl = response.url


            item = BlogTextItem()
            item['blogurl'] = blogurl

            # Get the name of the blogger from the db:
            # it's not possible to know the name of the blogger from the
            # website in cases the website is entirely empty or hosted by
            # a different domain.

            # Get the name of the blogger from the db:
            cur.execute("SELECT Blogurls.blogger FROM Blogurls\
                        WHERE Blogurls.blogurl = ? COLLATE NOCASE", (item['blogurl'],))
            try:
                blogger = cur.fetchone()[0]
                blogger = blogger
            except:
                blogger = ''

            con.close()

            item['posttime'] = 'empty blog'
            item['postdate'] = 'empty blog'
            item['permalink'] = 'empty blog'
            item['posttitle'] = 'empty blog'
            item['posttext'] = 'empty blog'
            item['numbercomments'] = 'empty blog'
            item['commenturl'] = 'empty blog'
            item['blogger'] = blogger
            # item['blogurl'] = blogurl
            item['addedtodb'] = time.strftime("%Y-%m-%d")
            item['pagenumber'] = 'empty blog'

            print "Blogger: %s; Blogurl: %s; Pagenumber: %s" % (item['blogger'], item['blogurl'], item['pagenumber'])
            yield item



    def check_lastpage(self, response):
        # print "check_lastpage running" # debug message
        # print response.url # debug message

        if response.xpath("//div[@class='post']"):
            # print "next page contains posts" # debug message
            next_url = response.url
            # print "next_url is %s" % next_url # debug message
            yield Request(next_url, dont_filter=True, callback=self.parse_item) # has to
            # add dont_filter=True, otherwise the Request won't run.
            # See http://stackoverflow.com/questions/20723371/scrapy-how-to-debug-scrapy-lost-requests

        else:
            # print "next page contains no posts -- last page" # debug message
            blogurl = response.url
            blogurl = blogurl.split("/")[2]
            blogurl = blogurl[:-11]
            # print blogurl # debug message
            cur.execute("UPDATE Blogtexts SET pagenumber='last page' \
            WHERE pagenumber= \
                        (SELECT MAX(pagenumber) FROM Blogtexts \
                        WHERE blogurl COLLATE NOCASE = ?) \
            AND blogurl COLLATE NOCASE = ? ", (blogurl, blogurl))
            con.commit()

        # *********************** #