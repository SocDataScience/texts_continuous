# Middleware file using Tor.

import platform


import sqlite3 as lite


def  con():
    if platform.system() == "Darwin":
        con = lite.connect('/Users/Annerose/Documents/15-16/Data/texts_continuous.db')
    if platform.system() == "Linux":
        con = lite.connect('/home/annerose/Python/texts_continuous.db')

    con.text_factory = str
    return con


