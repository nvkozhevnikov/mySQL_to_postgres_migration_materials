import sqlalchemy as db
import config

def get_articles():
    engine_postgres = db.create_engine(config.DB_POSTGRES['postgres'])
    connection_postgres = engine_postgres.connect()

    metadata = db.MetaData(bind=engine_postgres)
    join_table = db.Table('spravochnik_spravochnik', metadata, autoload=True)

    query = join_table.select()
    ResultProxy = connection_postgres.execute(query)
    ResultSet = ResultProxy.fetchall()
    return ResultSet

def get_tag_id(id):
    if id <= 15:
        tag_id = 1
    elif id >= 24:
        tag_id = 3
    else:
        tag_id = 2
    return tag_id

def prepare_date(articles):
    for article in articles:
        payload ={
            'spravochnik_id': article[0],
            'tags_id': get_tag_id(article[0]),
        }
        push_data_to_postgres(payload)

def push_data_to_postgres(data):
    engine_postgres = db.create_engine(config.DB_POSTGRES['postgres'])
    connection_postgres = engine_postgres.connect()

    metadata = db.MetaData(bind=engine_postgres)
    postgres_table = db.Table('spravochnik_spravochnik_tags', metadata, autoload=True)

    query = db.insert(postgres_table).values(**data)
    connection_postgres.execute(query)

def main():
    articles = get_articles()
    prepare_date(articles)

if __name__ == '__main__':
    main()