import pymysql
import sqlalchemy as db
import config

def get_data_from_mysql():
    pymysql.install_as_MySQLdb()
    engine_mysql = db.create_engine(config.DB_MYSQL['mysql'])
    connection_mysql = engine_mysql.connect()

    metadata = db.MetaData()
    mysql_table = db.Table('spravochnik', metadata, autoload=True, autoload_with=engine_mysql)

    query = db.select([mysql_table])
    ResultProxy = connection_mysql.execute(query)
    ResultSet = ResultProxy.fetchall()
    return ResultSet

def push_data_to_postgres(data):
    engine_postgres = db.create_engine(config.DB_POSTGRES['postgres'])
    connection_postgres = engine_postgres.connect()

    metadata = db.MetaData(bind=engine_postgres)
    postgres_table = db.Table('spravochnik_spravochnik', metadata, autoload=True)

    query = db.insert(postgres_table).values(**data)
    connection_postgres.execute(query)

def prepare_date(data):
    for i in data:
        payload = {
            'slug': i[2],
            'h1': i[3],
            'title': i[4],
            'description': i[5],
            'post_introduction': i[6],
            'post': i[7],
            'thumb_img_article': i[8],
            'is_published': True,
            'created_at': 'NOW()' if i[10] == None else i[10],
            'updated_at': 'NOW()' if i[11] == None else i[11],
            'category_id': 1,
        }
        push_data_to_postgres(payload)


def main():
    mysql_data = get_data_from_mysql()
    prepare_date(mysql_data)


if __name__ == '__main__':
    main()