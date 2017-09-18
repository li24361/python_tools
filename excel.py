import sys, xlrd, shutil, os


dic ="E:/xls/"
sql = "insert into `receivables_bid_detail` (`bid_id`, `debtor`, `due_start_time`, `due_end_time`, `buyback_time`, `principle`, `buyback_amount`, `assurance`, `assurance_detail`, `base_contract_no`, `base_contract_name`, `created_time`, `updated_time`) values('%s','%s','%s','%s','%s','%s','%d','%s','%s','%s','%s',now(),now());"

for root, dirs, files in os.walk(dic):
	for f in files:
		if f.find(".xlsx") != -1:
			excel_path =  os.path.join(dic, f)
			# print excel_path
			data = xlrd.open_workbook(excel_path)
			# print data
			table = data.sheets()[0]
			rows = table.nrows
			for i in range(2,rows):
                print os.path.basename(excel_path)
				# print sql % (table.cell(i,0).value,)