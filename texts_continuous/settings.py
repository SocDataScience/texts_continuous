# -*- coding: utf-8 -*-

# **************************************************** #

import sys # for command line arguments.
import config # importing user agent from separate file.
# **************************************************** #

BOT_NAME = 'blogtexts'

SPIDER_MODULES = ['texts_continuous.spiders']
NEWSPIDER_MODULE = 'texts_continuous.spiders'


# **************************************************** #
DOWNLOAD_DELAY = 6
CONCURRENT_REQUESTS = 1
CONCURRENT_REQUESTS_PER_DOMAIN = 1
CONCURRENT_ITEMS = 1

# limits the depth of the search (not exactly sure what this really entails)
# DEPTH_LIMIT = 30

# limits the number of downloaded pages. 
# CLOSESPIDER_PAGECOUNT = 5

# limits the number of downloaded items.
# CLOSESPIDER_ITEMCOUNT = 20

# closes the spider after a certain amount of time: 
# CLOSESPIDER_TIMEOUT = 14


# **************************************************** #
# Use different pipelines for different spiders ---
# The code below has to be adjusted dynamically depending on which spider is run:

ITEM_PIPELINES = {'texts_continuous.pipelines.BlogTextPipeline': 0}


# DEPTH_LIMIT = 0 # crawl all pages of any blog.
DEPTH_LIMIT = 100 # crawl only 100 pages of any given blog. -- Don't
# crawl until last page of a blog is reached.
LOG_LEVEL = 'INFO'

# **************************************************** #
# **************************************************** #
# Downloader middleware using Tor:
DOWNLOADER_MIDDLEWARE = {'texts_continuous.middlewares.RetryChangeProxyMiddleware': 600}



# **************************************************** #
# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = config.user_agent

# Use a different user agent for example in case that the other user agent is blocked:
# USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64; rv:7.0.1) Gecko/20100101 Firefox/7.7'


# **************************************************** #
# Disable cookies to increase performance of the crawler:
COOKIES_ENABLED = False

# **************************************************** #
# Adjust the log level that is shown in the terminal when running the spider.
# LOG_LEVEL = 'INFO'