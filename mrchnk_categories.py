import pymysql
import sqlalchemy as db
import config


def get_data_from_mysql():
    pymysql.install_as_MySQLdb()
    engine_mysql = db.create_engine(config.DB_MYSQL['mysql'])
    connection_mysql = engine_mysql.connect()

    metadata = db.MetaData()
    mysql_table = db.Table('material_categories', metadata, autoload=True, autoload_with=engine_mysql)

    query = db.select([mysql_table])
    ResultProxy = connection_mysql.execute(query)
    ResultSet = ResultProxy.fetchall()
    return ResultSet

def push_data_to_postgres(data):
    engine_postgres = db.create_engine(config.DB_POSTGRES['postgres'])
    connection_postgres = engine_postgres.connect()

    metadata = db.MetaData(bind=engine_postgres)
    postgres_table = db.Table('marochnik_categories', metadata, autoload=True)

    query = db.insert(postgres_table).values(**data)
    connection_postgres.execute(query)

def prepare_date(data):
    for i in data:
        payload = {
            'name': i[1],
            'slug': i[2],
            'metal_color': int(i[3]),
            'h1': i[1],
            'title': i[1],
            'description': i[1],
            'post': i[1],
            'is_published': True,
            'created_at': 'NOW()',
            'updated_at': 'NOW()',
        }
        push_data_to_postgres(payload)


def main():
    mysql_data = get_data_from_mysql()
    prepare_date(mysql_data)


if __name__ == '__main__':
    main()



