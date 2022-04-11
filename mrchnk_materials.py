import sys

import pymysql
import sqlalchemy as db
import config


def get_data_from_mysql():
    pymysql.install_as_MySQLdb()
    engine_mysql = db.create_engine(config.DB_MYSQL['mysql'])
    connection_mysql = engine_mysql.connect()

    metadata = db.MetaData()
    mysql_table = db.Table('materials', metadata, autoload=True, autoload_with=engine_mysql)
    mysql_table2 = db.Table('material_sub_categories', metadata, autoload=True, autoload_with=engine_mysql)

    query = db.select([mysql_table.join(mysql_table2)])
    ResultProxy = connection_mysql.execute(query)
    ResultSet = ResultProxy.fetchall()
    return ResultSet

def push_data_to_postgres(data):
    engine_postgres = db.create_engine(config.DB_POSTGRES['postgres'])
    connection_postgres = engine_postgres.connect()

    metadata = db.MetaData(bind=engine_postgres)
    postgres_table = db.Table('marochnik_materials', metadata, autoload=True)

    query = db.insert(postgres_table).values(**data)
    connection_postgres.execute(query)

def sub_query(slug):
    engine_postgres = db.create_engine(config.DB_POSTGRES['postgres'])
    connection_postgres = engine_postgres.connect()

    metadata = db.MetaData(bind=engine_postgres)
    join_table = db.Table('marochnik_subcategories', metadata, autoload=True)

    query = join_table.select().where(join_table.c.slug == slug)
    ResultProxy = connection_postgres.execute(query)
    ResultSet = ResultProxy.fetchall()
    razdel_id = ResultSet[0][0]
    return razdel_id

def prepare_date(data):
    for i in data:
        payload = {
            'name': i[2],
            'slug': i[6],
            'h1': i[4],
            'title': i[3],
            'description': i[5],
            'is_published': True,
            'created_at': 'NOW()' if i[20] == None else i[20],
            'updated_at': 'NOW()' if i[21] == None else i[21],
            'subcategory_id': sub_query(i[25]),
            'main_properties': i[8],
            'him_sostav': i[10],
            'meh_properties': i[12],
            'tehnol_properties': i[11],
            'fiz_properties': i[14],
            'tverdost': i[13],
            'temp_krit_tchk': i[15],
            'vidy_postavki': i[9],
            'inter_analogs': i[16],
            'faq': i[7],
            'sources': i[17],

        }
        push_data_to_postgres(payload)
        # print(payload)
        # sys.exit()


def main():
    mysql_data = get_data_from_mysql()
    prepare_date(mysql_data)


if __name__ == '__main__':
    main()



