import pymysql

dbConn = pymysql.connect(
  user='admin', 
  passwd='QsBch0Pp1^4qcfM#F',
  host='mysql-nu-cs310-2.crc8ae6o63le.us-east-2.rds.amazonaws.com',
  port=3306, 
  database='sys')
  
dbCursor = dbConn.cursor()

dbCursor.execute("SHOW DATABASES")

for dbname in dbCursor:
  print(dbname)

dbCursor.close()
dbConn.close()
