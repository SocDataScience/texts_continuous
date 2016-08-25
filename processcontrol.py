#!/usr/bin/python
# This is a process control file.

# It starts the python process for the continuousscraper; checks
# that the process is still running and relaunches the process at the
# right place if it is no longer running.

# start the process:
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

import config # import configuration file for mail address, password etc.
import smtplib # for sending emails automatically
import time


from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import platform

import sqlite3 as lite
con = None


import sys # for command line arguments.
# ***********************************#
# Defining the connection to the platform:
if platform.system() == "Darwin":
    con = lite.connect('/Users/Annerose/Documents/15-16/Data/blogger_continuous.db')
if platform.system() == "Linux":
    con = lite.connect('/home/annerose/Python/blogger_continuous.db')
cur = con.cursor()
con.text_factory = str

# ***********************************#
# Starting the process

start = time.time()
starttime = time.strftime("%a, %d %b %Y %H:%M:%S", time.gmtime())

# *************************************** #
# Send an email to tell me that the process has effectively started as scheduled.
msg = MIMEMultipart()

# Send message depending on the System the program in run on:
if platform.system() == "Linux":
    msg["Subject"] = "Status update from texts_continuous (Server)"
if platform.system() == "Darwin":
    msg["Subject"] = "Status update from texts_continuous (Mac)"


body = "\n\nCrawling of texts_continuous started at %s, \n\n" % starttime
msg.attach(MIMEText(body, 'plain'))


server = smtplib.SMTP(config.smtp_server, 587) # smtp server.
server.starttls()
server.login(config.mail_address, config.mail_password) # your email address, your email password.
server.sendmail(config.mail_address, config.mail_address, msg.as_string())
server.close()

# *************************************** #
process = CrawlerProcess(get_project_settings())

# setting the crawler:
process.crawl('texts_continuous', domain='blogger.ba')
process.start() # the script will block here until the crawling is finished


# *************************************** #
finish = time.time()
finishtime = time.strftime("%a, %d %b %Y %H:%M:%S", time.gmtime())

time_to_process = round((finish-start)/3600, 4)

# *************************************** #
# Sending an email once the process is stopped

msg = MIMEMultipart()

# Send message depending on the System the program in run on:
if platform.system() == "Linux":
    msg["Subject"] = "Status update from texts_continuous (Server)"
if platform.system() == "Darwin":
    msg["Subject"] = "Status update from texts_continuous (Mac)"


body = "\n\nCrawling took %s hours; and finished at %s\n\n" % (time_to_process, finishtime)
msg.attach(MIMEText(body, 'plain'))

server = smtplib.SMTP(config.smtp_server, 587) # smtp server.
server.starttls()
server.login(config.mail_address, config.mail_password) # your email address, your email password.
server.sendmail(config.mail_address, config.mail_address, msg.as_string())
server.close()