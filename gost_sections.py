import pymysql
import sqlalchemy as db
import config


def get_data_from_mysql():
    pymysql.install_as_MySQLdb()
    engine_mysql = db.create_engine(config.DB_MYSQL['mysql'])
    connection_mysql = engine_mysql.connect()

    metadata = db.MetaData()
    klassifikator_gostov_1_ur = db.Table('klassifikator_gostov_1_ur', metadata, autoload=True, autoload_with=engine_mysql)

    query = db.select([klassifikator_gostov_1_ur])
    ResultProxy = connection_mysql.execute(query)
    ResultSet = ResultProxy.fetchall()
    return ResultSet

def push_data_to_postgres(data):
    engine_postgres = db.create_engine(config.DB_POSTGRES['postgres'])
    connection_postgres = engine_postgres.connect()

    metadata = db.MetaData(bind=engine_postgres)
    gosts_section = db.Table('gosts_gostsections', metadata, autoload=True)

    query = db.insert(gosts_section).values(**data)
    connection_postgres.execute(query)

def prepare_date(data):
    for i in data:
        payload = {
            'section_number': i[2],
            'section_name': i[3],
            'slug': i[1],
            'h1': i[3],
            'title': i[3],
            'description': i[3],
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



