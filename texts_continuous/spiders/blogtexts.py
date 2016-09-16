# -*- coding: utf-8 -*-
# Crawling all the blog post content!!

# This crawler should run on PYTHON 2!!!

# *********************** #
# Import all necessary modules:

from scrapy.spiders import Spider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.selector import HtmlXPathSelector
# from scrapy import Request
from scrapy.http import Request

from imp import reload # import reload in Python 3


import re
import time
import random

from texts_continuous.items import BlogTextItem
from texts_continuous.db import con

import sqlite3 as lite

# con = None

import platform
import sys # for using command line arguments
# *********************** #
# Define the BlogTextSpider class:

class BlogTextSpider(Spider):

    name = "texts_continuous"
    allowed_domains = ["blogger.ba"]

    handle_httpstatus_list = [404] # http://stackoverflow.com/questions/16909106/scrapyin-a-request-fails-eg-404-500-how-to-ask-for-another-alternative-reque
    # *********************** #

    con = con()
    cur = con.cursor()

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
    WHERE blogID NOT IN (SELECT DISTINCT blogurls.blogID \
    FROM blogurls, blogtexts WHERE blogurls.blogID = blogtexts.blogID)")
    blogurls = cur.fetchall()


    # 2. get all the blogurl that are already in the Blogtext table:
    # Use this approach if 50 blogtexts per blog are enough:
    cur.execute("SELECT blogurl, MAX(pagenumber) \
    FROM Blogtexts \
    WHERE blogID IN \
    (SELECT DISTINCT Blogurls.blogID FROM Blogurls, Blogtexts WHERE Blogurls.blogID = Blogtexts.blogID) \
    AND blogID NOT IN (SELECT DISTINCT Blogtexts.blogID \
    FROM Blogtexts WHERE Blogtexts.pagenumber=='last page' OR Blogtexts.pagenumber=='empty blog') \
    AND blogID IN (SELECT blogID FROM \
    (SELECT blogID, COUNT(*) as c FROM Blogtexts GROUP BY blogID) WHERE c <=50) \
    GROUP BY blogID")
    blogurls_continue = cur.fetchall()


    if not blogurls:
        print "=== ERROR/WARNING MESSAGE FROM CRAWLER blogtexts: ===\n" \
          "=== No new Blogtext entries to scrape!! \n" \
          "=== Get instead more blogurls into Bloggerurl table."

    else:
        blogurls = [i[0] for i in blogurls]
        if not blogurls_continue:
            blogurls_continue = []

        start_urls = [("http://" + i + ".blogger.ba/arhiva/?start=" + str(j-20 if j-20>=0 else 0)) for i, j in blogurls_continue] + \
        [("http://" + i +  ".blogger.ba/arhiva/?start=0") for i in blogurls]

        # len(start_urls)
        # testing start_url:
        # start_urls = ["http://cokoladai.blogger.ba/arhiva/?start=0"] # debug start_url
        # start_urls = ["http://protagonist.blogger.ba/arhiva/?start=0"]  # debug start_url

        print(start_urls)

        print "=============\n Number of start_urls for Blogtexts: %s\n=============" % len(start_urls)


    con.close() # shouldn't be closed if con is defined as global variable.


    # *********************** #
    # *********************** #
    # http://stackoverflow.com/questions/13476688/scrapy-unhandled-exception
    def start_requests(self):
        for url in self.start_urls:
            requests = self.make_requests_from_url(url)
            if type(requests) is list:
                for request in requests:
                    yield request
            else:
                yield requests

    # http://stackoverflow.com/questions/20081024/scrapy-get-request-url-in-parse
    # overwrite make_requests_from_url so that blogurls that redirect can be
    # correctly stored in the db:
    def make_requests_from_url(self, url):
        item = BlogTextItem()

        blogurl = url
        print "Request url: %s " % url #debug message
        blogurl = blogurl.split("/")[2]
        blogurl = blogurl.split(".")[0]
        item['blogurl'] = blogurl
        print "Request url: %s " % blogurl

        request = Request(url, dont_filter=True, callback=self.parse_item)

        # set the meta['item'] to use the item in the next call back
        request.meta['item'] = item
        # print request.meta['item'] #debug message
        return request


    # Once I have the urls, scrape the urls for responses:

    # Make sure even the texts from the start page are crawled:
    def parse(self, response):
        print response.status

        print "Response url: %s" % response.url
        return self.parse_item(response)

    def parse_item(self, response): # class CrawlSpider uses parse_item, class (Base)Spider takes only parse.
        print "Response status: %s" % response.status
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

            # pagenumber of the blog:
                    if re.search("(.blogger.ba/arhiva/)", response.url):

                        if response.xpath("//a[contains(text(), 'Stariji postovi')]"):
                            pagenumber = response.url.split("/")[-1]
                            pagenumber = pagenumber[7:]
                        else:
                            pagenumber = 'last page'

                    else:
                        pagenumber = 'last page'

            # *********************** #

            # fill the item if the info as defined in the above code:
                    item = response.meta['item']
                    print "%s contains posts" % response.meta['item']['blogurl']  # debug message


                    item['posttime'] = posttime
                    item['postdate'] = postdate
                    item['permalink'] = permalink
                    item['posttitle'] = posttitle
                    item['posttext'] = posttext
                    item['numbercomments'] = numbercomments
                    item['commenturl'] = commenturl
                    item['blogger'] = blogger
                    # item['blogurl'] = blogurl
                    item['addedtodb'] = time.strftime("%Y-%m-%d")
                    item['pagenumber'] = pagenumber
                    item['lastpage'] = None

                    yield item

                except: # if there was an error message: just pass and continue
                    # with the next blogpost.
                    pass

            # *********************** #

            # extract the links which should be followed at the end of each page:
            # (do this only if there are any posts on the site; therefore this code
            # is subordered to the above code)

            if response.xpath("//a[contains(text(), 'Stariji postovi')]"):

                url = response.xpath("//a[contains(text(), 'Stariji postovi')]/@href").extract()[0]
                current = response.url
                current = "/".join(current.split("/")[:3])
                url = "".join([current, url])

                print url # debug message
                print "%s contains older posts." % current  # debug message

                yield Request(url, callback=self.check_lastpage)

            # *********************** #

        # How to fill the items if the blog is entirely emtpy
        # or is hosted inside a different domain than blogger.ba:
        if response.status == 404 or not response.xpath("//div[@class='post']"):
            item = response.meta['item']

            print "%s contains NO posts" % item['blogurl']


            # Get the name of the blogger from the db:
            # it's not possible to know the name of the blogger from the
            # website in cases the website is entirely empty or hosted by
            # a different domain.

            # Get the name of the blogger from the db:
            from texts_continuous.db import con
            con = con() # this gives an error -- can't find class/variable db.
            cur = con.cursor()
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
            item['addedtodb'] = time.strftime("%Y-%m-%d")
            item['pagenumber'] = 'empty blog'
            item['lastpage'] = None

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
            item = response.meta['item']
            print "next page of %s contains no posts -- last page" % item['blogurl'] # debug message

            item['lastpage'] = 'last page'
            item['permalink']  = None
            item['pagenumber'] = None

            yield item





        # *********************** #
