import urllib.request, json  
import unicodedata
import datetime
from subprocess import Popen
import re

def getRate(fromCurrency, toCurrency, date):
	"""
	getFxRates for two currencies and a specific date

	""" 
	APIKEY = "APIKEY"  
	url = "https://www.oanda.com/rates/api/v1/rates/{1}.json?api_key={0}&decimal_places={4}&date={3}&fields=midpoint&quote={2}".format(APIKEY, fromCurrency, toCurrency, date, "all")
	response = urllib.request.urlopen(url)
	content = response.read()
	data = json.loads(content.decode("utf8"))    
	rate = data["quotes"][toCurrency]["midpoint"]  
	print (rate)
	return rate
 

def writeSQLQueryFile(FromCurrency, toCurrency, date, rate, dbName, query_filename):
	sqlText = """USE {4};
	GO
	INSERT INTO FxRates (FromCurrency, ToCurrency, Date, FxRate)
	VALUES ('{0}', '{1}', '{2}', '{3}');
	GO""".format(FromCurrency,toCurrency, date, rate, dbName)

	with open(query_filename, 'w') as f:
		f.write(sqlText)


def writeBatFile(bat_filename, serverName,queryRequest, queryResponse, query_filename): 
	with open(bat_filename, 'w') as f:
		f.write("sqlcmd -S {0} -i {1} -o {2}".format(serverName, query_filename, queryResponse))


def runBatFile(filename):
	p = Popen(filename)
	stdout, stderr = p.communicate()


def insertIntoDB_FromCurrencyToCurrencyDateRate(fromCurrency, toCurrency, date):
	bat_filename = "run_insertSqlQuery.bat"
	sqlquery_filename = "SQLInsertquery.sql"
	writeSQLQueryFile(fromCurrency, toCurrency, date, getRate(fromCurrency, toCurrency, date), "FxRates", sqlquery_filename)
	writeBatFile(bat_filename, "SERVERNAME","queryRequest.sql", "queryResponse.txt", sqlquery_filename)
	runBatFile(bat_filename)


def write_selectSQLQueryFile(query_filename, db_name):
	sqlText = """USE {0};
	GO
	SELECT max(Date) FROM dbo.FxRates ;
	GO""".format(db_name)

	with open(query_filename, 'w') as f:
		f.write(sqlText)


def get_max_date_in_db():
	sqlquery_filename = "SQLInsertquery.sql"
	bat_filename = "run_selectSqlQuery.bat"
	write_selectSQLQueryFile(sqlquery_filename, "FxRates")
	writeBatFile(bat_filename, "CH-W72469",sqlquery_filename, "queryResponse.txt", "SQLInsertquery.sql")
	runBatFile(bat_filename)
	with open('queryResponse.txt', 'r') as f:
		read_data = f.read() 
		match = re.search(r'\d{4}-\d{2}-\d{2}', read_data)
		return (datetime.datetime.strptime(match.group(0), '%Y-%m-%d').date())

#print (get_max_date_in_db())

 
def daterange( start_date, end_date ):
    if start_date <= end_date:
        for n in range( ( end_date - start_date ).days + 1 ):
            yield start_date + datetime.timedelta( n )
    else:
        for n in range( ( start_date - end_date ).days + 1 ):
            yield start_date - datetime.timedelta( n )
 

def insertIntoDB_RateRange(fromCurrency, toCurrency, dateStart, dateEnd):
	for date in daterange( dateStart, dateEnd ): 
		insertIntoDB_FromCurrencyToCurrencyDateRate(fromCurrency, toCurrency, date)



def fill_db_to_present_day():
	today = datetime.date.today()  
	max_date_in_db = get_max_date_in_db()
	if today > max_date_in_db:
		max_date_in_db_plus_one = max_date_in_db + datetime.timedelta(days=1)
		insertIntoDB_RateRange("CHF", "EUR", max_date_in_db_plus_one, today )
	elif today < max_date_in_db:
		print ("database max date is BIGGER than today - something is wrong here...")
	else:
		print ("database max date is equal to today")

def misc():
	with open('selectMaxDateSqlQuery.bat', 'w') as f:
		f.write("sqlcmd -S {0} -i {1} -o {2}".format(serverName, queryName, queryResponse))

	start = datetime.date( year = 2010, month = 1, day = 1 )
	end = datetime.date( year = 2010, month = 1, day = 5 )
	
def misc2():
	date_object = datetime.datetime.strptime("2011-09-04", '%Y-%m-%d')
	date_object2 = datetime.datetime.strptime("2014-09-04", '%Y-%m-%d') 
	insertIntoDB_FromCurrencyToCurrencyDateRate("CHF", "EUR", "2014-09-01")

def misc3():
	s = datetime.datetime.strptime("2014-09-01", '%Y-%m-%d').date()
	s += datetime.timedelta(days=1)

	e = datetime.datetime.strptime("2014-09-04", '%Y-%m-%d').date()
 
	printRateRange("CHF", "EUR", s, e)

fill_db_to_present_day()