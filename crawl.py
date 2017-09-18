#-*- coding:utf-8 -*-

from bs4 import BeautifulSoup
from urlparse import urljoin
import MySQLdb
import requests
import json

#promotion
HOST   = '10.135.111.22'
USER   = 'dev'
PASSWD = 'dev'
DB     = 'job'
PORT   = 3307

url = "http://www.lagou.com/jobs/positionAjax.json?city=%E9%9D%92%E5%B2%9B&needAddtionalResult=false"

page = 34
page_size=15
total_size=1000

conn = MySQLdb.connect(host=HOST, user=USER, passwd=PASSWD, db=DB, port=PORT, charset='utf8')
cur = conn.cursor()

while True:
	page = page+1
	if page_size*(page-1) > total_size:
		break
	data = {'first': 'false', 'pn': page,'kd':''}
	r = requests.post(url,data=data)
	try:
		jobs = json.loads(r.text)
	except Exception as e:
		print('read page error:', e)
		print r.text
		break
	print "query page %s,page_size:%s,total_size:%s" % (page,page_size,total_size)
	page=jobs['content']['pageNo']
	page_size=jobs['content']['pageSize']
	total_size=jobs['content']['positionResult']['totalCount']
	job_list = jobs['content']['positionResult']['result']
	# print job_list
	for info in job_list:
		company=info['companyFullName'].encode('utf8')
		position=info['positionName'].encode('utf8')
		position_advantage=info['positionAdvantage'].encode('utf8')
		salary=info['salary'].encode('utf8')
		work_year=info['workYear'].encode('utf8')
		education = info['education'].encode('utf8')
		company_id = info['companyId']
		position_id = info['positionId']
		position_url = "http://www.lagou.com/jobs/%s.html" % (position_id)
		r = requests.get(position_url)
		soup = BeautifulSoup(r.text, "html.parser")
		try:
			address = soup.select(".work_addr")[0].get_text(strip=True).encode('utf8')
		except Exception as e:
			address =''
		sql="INSERT INTO `job`.`job_info` (`company_id`, `company`, `position_id`, `position`, `position_advantage`, `salary`, `education`, `position_url`, `address`, `create_time`, `update_time`) VALUES (\'%s\', \'%s\', \'%s\', \'%s\', \'%s\',\'%s\',\'%s\',\'%s\',\'%s\', now(), now())"
		cur.execute(sql % (company_id, company, position_id, position, position_advantage, salary, education, position_url,address))
	conn.commit()
cur.close()

