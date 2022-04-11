import sys

import pymysql
import sqlalchemy as db
import config


def get_data_from_mysql():
    pymysql.install_as_MySQLdb()
    engine_mysql = db.create_engine(config.DB_MYSQL['mysql'])
    connection_mysql = engine_mysql.connect()

    metadata = db.MetaData()
    mysql_table = db.Table('material_sub_categories', metadata, autoload=True, autoload_with=engine_mysql)
    mysql_table2 = db.Table('material_categories', metadata, autoload=True, autoload_with=engine_mysql)

    query = db.select([mysql_table.join(mysql_table2)])
    ResultProxy = connection_mysql.execute(query)
    ResultSet = ResultProxy.fetchall()
    return ResultSet

def push_data_to_postgres(data):
    engine_postgres = db.create_engine(config.DB_POSTGRES['postgres'])
    connection_postgres = engine_postgres.connect()

    metadata = db.MetaData(bind=engine_postgres)
    postgres_table = db.Table('marochnik_subcategories', metadata, autoload=True)

    query = db.insert(postgres_table).values(**data)
    connection_postgres.execute(query)

def sub_query(slug):
    engine_postgres = db.create_engine(config.DB_POSTGRES['postgres'])
    connection_postgres = engine_postgres.connect()

    metadata = db.MetaData(bind=engine_postgres)
    join_table = db.Table('marochnik_categories', metadata, autoload=True)

    query = join_table.select().where(join_table.c.slug == slug)
    ResultProxy = connection_postgres.execute(query)
    ResultSet = ResultProxy.fetchall()
    razdel_id = ResultSet[0][0]
    return razdel_id

def prepare_date(data):
    for i in data:
        payload = {
            'name': i[2],
            'slug': i[3],
            'h1': i[2],
            'title': i[2],
            'description': i[2],
            'is_published': True,
            'created_at': 'NOW()',
            'updated_at': 'NOW()',
            'category_id': sub_query(i[7])
        }
        push_data_to_postgres(payload)
        # print(payload)
        # sys.exit()


def main():
    mysql_data = get_data_from_mysql()
    prepare_date(mysql_data)


if __name__ == '__main__':
    main()



