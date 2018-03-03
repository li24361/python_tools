#-*- coding:utf-8 -*-
__author__ = '01401544'
import requests
class bidSearch:
    def query(self):
        headers = {'Host': 'www.hairongyi.com',
                    'Connection': 'keep-alive',
                    'Cache-Control': 'max-age=0',
                    'Upgrade-Insecure-Requests': '1',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                    'DNT': '1',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7'}
        r = requests.get("https://www.hairongyi.com/bids", headers=headers)
        r.encoding = 'utf-8'
        try:
            if r.text.index('"bidStatus": "TBZ"'):
                requests.get('https://pushbear.ftqq.com/sub?sendkey=1869-3b3ae8e1bf6c00ad3b7fa2fcc2d2cf49&text=海融易有标了，快去抢！')
                print{'has bid'}
        except ValueError:
            print('no bid')


if __name__ == '__main__':
    bidSearch = bidSearch()
    bidSearch.query()

