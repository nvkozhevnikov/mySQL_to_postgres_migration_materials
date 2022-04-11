import pymysql
import sqlalchemy as db
import config


def get_data_from_mysql():
    pymysql.install_as_MySQLdb()
    engine_mysql = db.create_engine(config.DB_MYSQL['mysql'])
    connection_mysql = engine_mysql.connect()

    metadata = db.MetaData()
    mysql_table = db.Table('gost_materials', metadata, autoload=True, autoload_with=engine_mysql)
    mysql_table2 = db.Table('klassifikator_gostov_2_ur', metadata, autoload=True, autoload_with=engine_mysql)

    query = db.select([mysql_table.join(mysql_table2)])
    ResultProxy = connection_mysql.execute(query)
    ResultSet = ResultProxy.fetchall()
    return ResultSet

def push_data_to_postgres(data):
    engine_postgres = db.create_engine(config.DB_POSTGRES['postgres'])
    connection_postgres = engine_postgres.connect()

    metadata = db.MetaData(bind=engine_postgres)
    postgres_table = db.Table('gosts_gosts', metadata, autoload=True)

    query = db.insert(postgres_table).values(**data)
    connection_postgres.execute(query)

def sub_query(slug):
    engine_postgres = db.create_engine(config.DB_POSTGRES['postgres'])
    connection_postgres = engine_postgres.connect()

    metadata = db.MetaData(bind=engine_postgres)
    join_table = db.Table('gosts_gostsubsections', metadata, autoload=True)

    query = join_table.select().where(join_table.c.slug == slug)
    ResultProxy = connection_postgres.execute(query)
    ResultSet = ResultProxy.fetchall()
    razdel_id = ResultSet[0][0]
    return razdel_id

def prepare_date(data):
    for i in data:
        payload = {
            'standard': i[2],
            'standard_number': i[3],
            'slug': i[1],
            'h1': i[4],
            'title': i[4],
            'title_eng': i[5],
            'description': i[4],
            'is_published': True,
            'created_at': 'NOW()',
            'updated_at': 'NOW()',
            'subsection_id': sub_query(i[8]),
        }
        push_data_to_postgres(payload)

def main():
    mysql_data = get_data_from_mysql()
    prepare_date(mysql_data)


if __name__ == '__main__':
    main()





