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

    BASIC_SELECT = """
        SELECT subject, predicate, object FROM triples WHERE predicate = 'dcm_title' OR predicate = 'dcm_description' OR predicate = 'tag'
    """

    DETAILS = """
        SELECT predicate, object FROM triples WHERE subject = %(id)s
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
    def insert_new_object(self, bucket, object, dcm, tags):
        cur = Postgres.conn.cursor()
        try:
            cur.execute(Postgres.INSERT_STORAGE, {"hash": dcm["dcm_identifier"], "bucket": bucket, "object": object})
        except psycopg2.errors.UniqueViolation:
            print("Already inserted..")
            return
        # insert list of tuples as this is fastest way for postgres
        inserts = []
        # dcm
        for key in dcm:
            row = (dcm["dcm_identifier"], key, dcm[key])
            inserts.append(row)
        # tags
        for tag in tags:
            row = (dcm["dcm_identifier"], "tag", tag)
            inserts.append(row)
        # mogrify sanitizes input, so this is SQL injection safe
        args_str = ','.join(cur.mogrify("(%s, %s, %s)", x).decode("utf-8") for x in inserts)
        cur.execute(Postgres.INSERT_TRIPLE + args_str)
        Postgres.conn.commit()
        cur.close()

    def get_objects(self):
        cur = Postgres.conn.cursor()
        cur.execute(Postgres.BASIC_SELECT)
        records = cur.fetchall()
        objects = {}
        for r in records:
            if r[0] not in objects:
                # tags need to be predefined because it's a list, other attributes are strings so can be created dynamically
                objects[r[0]] = {"tags": []}
            if r[1] == "tag":
                objects[r[0]]["tags"].append(r[2])
            else:
                objects[r[0]][r[1]] = r[2]
        cur.close()
        return objects

    def get_details(self, id):
        cur = Postgres.conn.cursor()
        cur.execute(Postgres.DETAILS, {"id": id})
        records = cur.fetchall()
        result = {"tags": []}
        print(cur.query)
        for r in records:
            if r[0] == "tag":
                result["tags"].append(r[1])
            else:
                result[r[0]] = r[1]
        if len(result) == 0:
            return None
        return result
            
