import os
import psycopg2


class Postgres():
    # static variable, best practice is to keep only a single connection and reuse it
    conn = None

    INSERT_TRIPLE = """
        INSERT INTO triples (subject, predicate, object) VALUES
    """

    INSERT_STORAGE = """
        INSERT INTO storage (hash, bucket, object) VALUES (%(hash)s, %(bucket)s, %(object)s)
    """


    def __init__(self) -> None:
        if Postgres.conn is None:
            try:
                conn = psycopg2.connect(database=os.environ.get("POSTGRES_DB"),
                                        user=os.environ.get("POSTGRES_USER"),
                                        password=os.environ.get("POSTGRES_PASSWORD"),
                                        host=os.environ.get("POSTGRES_URL"),
                                        port=os.environ.get("POSTGRES_PORT"))
                Postgres.conn = conn
            except Exception as e:
                print(e)
                print("Database connection unsuccesfull. Bailing out.")
                exit(1)
    
    # save triples of metadata for object + save storage details for object
    def insert_new_object(self, bucket, object, metadata):
        cur = Postgres.conn.cursor()
        try:
            cur.execute(Postgres.INSERT_STORAGE, {"hash": metadata["dcm_identifier"], "bucket": bucket, "object": object})
        except psycopg2.errors.UniqueViolation:
            print("Already inserted..")
            return
        # insert list of tuples as this is fastest way for postgres
        inserts = []
        for key in metadata:
            row = (metadata["dcm_identifier"], key, metadata[key])
            inserts.append(row)
        # mogrify sanitizes input, so this is SQL injection safe
        args_str = ','.join(cur.mogrify("(%s, %s, %s)", x).decode("utf-8") for x in inserts)
        cur.execute(Postgres.INSERT_TRIPLE + args_str)
        Postgres.conn.commit()
        cur.close()
