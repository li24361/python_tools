#-*- coding:utf-8 -*-

from bs4 import BeautifulSoup
from urlparse import urljoin
import MySQLdb
import requests
import json


class crawlPic:
	def __init__(self):
        self.code = sys.argv[1]

    def save_pic(self):
    	r = requests.get("http://search.ehaier.com/s?k="+self.code)
    	print r.text 




if __name__ == '__main__':
    if len(sys.argv)<2:
        print 'please input no! '
        exit(1)
    checkbid = checkbid()
    checkbid.check_data()