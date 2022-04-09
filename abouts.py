import pymysql
import sqlalchemy as db
import config
from sqlalchemy.dialects import postgresql
import sys


def get_data_from_mysql():
    pymysql.install_as_MySQLdb()
    engine_mysql = db.create_engine(config.DB_MYSQL)
    connection_mysql = engine_mysql.connect()

    metadata = db.MetaData()
    abouts = db.Table('abouts', metadata, autoload=True, autoload_with=engine_mysql)

    query = db.select([abouts])
    ResultProxy = connection_mysql.execute(query)
    ResultSet = ResultProxy.fetchall()
    return ResultSet

def push_data_to_postgres(data):
    engine_postgres = db.create_engine(config.DB_POSTGRES)
    connection_postgres = engine_postgres.connect()

    metadata = db.MetaData(bind=engine_postgres)
    about = db.Table('homepage_about', metadata, autoload=True)

    i = db.insert(about)
    q = i.values(**data)

    # print(str(q.compile(dialect=postgresql.dialect())))
    connection_postgres.execute(q)

def prepare_date(data):
    for i in data:
        payload = {
            'h1': i[3],
            'slug': i[1],
            'title': i[2],
            'breadcrumb_text': i[3],
            'post': i[5],
            'description': i[4],
            'is_published': True,
            'created_at': 'NOW()',
            'updated_at': 'NOW()',
        }
        push_data_to_postgres(payload)


def main():
    mysql_abouts = get_data_from_mysql()
    prepare_date(mysql_abouts)


if __name__ == '__main__':
    main()



