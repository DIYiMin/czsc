
import pymysql
from scrapy.utils.project import get_project_settings

def insert(data):
    host = settings['MYSQL_HOST']
    user = settings['MYSQL_USER']
    psd = settings['MYSQL_PASSWORD']
    db = settings['MYSQL_DB']
    c = settings['CHARSET']
    port = settings['MYSQL_PORT']
    con = pymysql.connect(host=host, user=user, passwd=psd, db=db,
                          charset=c, port=port)
    cue = con.cursor()
    try:
        cue.execute("insert ignore into " +
                    data['table'] +
                    " (code, name, price, time, create_time) "
                    "values (%s,%s,%s,%s,%s)",
                    [data['code'], data['name'], data['price'],
                     'time', data['create_time']])
    except Exception as e:
        print('Insert error:', e)
        con.rollback()
    else:
        con.commit()
    con.close()
    return item