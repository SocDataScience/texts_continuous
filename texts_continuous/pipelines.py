# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

# -*- coding: utf-8 -*-

# **************************************************** #
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import sqlite3 as lite

con = None

import platform

class BlogTextPipeline(object):

    # this is a type of class constructor. It gets called automatically once the
    # class is opened.
    def __init__(self):
        self.setupDBcon()


    def setupDBcon(self):
        if platform.system() == "Darwin":
            self.con = lite.connect('/Users/Annerose/Desktop/test.db')
            # self.con = lite.connect('/Users/Annerose/Documents/15-16/Data/texts_continuous.db')
        if platform.system() == "Linux":
            self.con = lite.connect('/home/annerose/Python/texts_continuous.db')
        self.cur = self.con.cursor()
        self.con.text_factory = str


    # test whether permalink already exists. If permalink (=unique identifier)  already exists,
    # don't store in DB, don't process item further. ...
    # Set the conditions for processing the item further:
    def process_item(self, item, spider):
        self.cur.execute("SELECT * FROM Blogtexts WHERE permalink = ?",\
                         (item['permalink'],))
        result = self.cur.fetchone()
        if result and item['pagenumber']=='last page':
            self.updateInDB(item)
        if not result and not item['lastpage']:
            self.storeBlogTextsInDB(item)
        # deal with empty blogs:
        if item['permalink']=='empty blog':
            self.cur.execute("SELECT * FROM Blogtexts WHERE blogurl = ? COLLATE NOCASE",\
                         (item['blogurl'],))
            empty = self.cur.fetchone()
            if empty:
                self.updateEmptyInDB(item)
            if not empty:
                self.storeBlogTextsInDB(item)
        if item['lastpage']:
            self.updateLastPage(item)

        return item


    # Now actually insert item in database.
    def storeBlogTextsInDB(self, item):
        self.cur.execute("INSERT INTO Blogtexts(\
        ID, posttime, postdate, permalink, posttitle, posttext, numbercomments, \
        commenturl, blogger, blogurl, blogID, pagenumber, addedtodb) \
        VALUES ((SELECT ID FROM blogurls \
        WHERE blogurl = ? COLLATE NOCASE), ?, ?, ?, ?, ?, ?, ?, ?, ?, \
        (SELECT blogID from Blogurls WHERE blogurl = ? COLLATE NOCASE), ?, ?)",\
                         (item.get('blogurl', ''),
                          item.get('posttime', ''),
                          item.get('postdate', ''),
                          item.get('permalink', ''),
                          item.get('posttitle', ''),
                          item.get('posttext', ''),
                          item.get('numbercomments', ''),
                          item.get('commenturl', ''),
                          item.get('blogger', ''),
                          item.get('blogurl', ''),
                          item.get('blogurl', ''),
                          item.get('pagenumber', ''),
                          item.get('addedtodb', '')))
        self.con.commit()

    def updateInDB(self, item):
        self.cur.executemany("UPDATE Blogtexts SET pagenumber='last page' \
                         WHERE permalink=?", \
                         (item.get('permalink', '')))

    def updateEmptyInDB(self, item):
        self.cur.execute("UPDATE Blogtexts SET addedtodb=? \
                         WHERE blogurl=? COLLATE NOCASE", \
                         (item.get('addedtodb', ''),
                          item.get('blogurl', '')))

        self.con.commit()


    def updateLastPage(self, item):
        self.cur.execute("UPDATE Blogtexts SET pagenumber='last page' \
        WHERE pagenumber= \
                    (SELECT MAX(pagenumber) FROM Blogtexts \
                    WHERE blogurl COLLATE NOCASE = ?) \
        AND blogurl COLLATE NOCASE = ? ", (item['blogurl'], item['blogurl']))
        self.con.commit()

    # ************************** #

    # If you want to get a new table with every crawl instead of
    # # updating the table, add those two lines:
    # def dropBlogTextsTable(self):
      #  self.cur.execute("DROP TABLE IF EXISTS Blogtexts")

    # closes the connection to the database:
    def closeDB(self):
        self.con.close()

    # this is a type of class deconstructor. It gets called automatically once the class
    # is no longer used (i.e. once the spider is closed)
    def __del__(self):
        self.closeDB()
