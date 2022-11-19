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
        SELECT subject, predicate, object FROM triples WHERE (predicate = 'dcm_title' OR predicate = 'dcm_description' OR predicate = 'tag')
    """
    # this is written in such a way so its easy to append another filter parameters to it
    FILTER_TAGS_SELECT = """
        SELECT DISTINCT subject FROM triples WHERE (predicate = 'tag' AND (object IN %(tags)s OR %(tags)s IS NULL))
    """

    FILTER_TAGS_SELECT2 = """
        SELECT subject FROM (SELECT subject, COUNT(*) as count FROM triples WHERE $$CLAUSES$$ GROUP BY subject) as sc
        WHERE sc.count = %(count)s
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

    def __filter_by_tags(self, tags: list) -> tuple:
        cur = Postgres.conn.cursor()
        query = Postgres.FILTER_TAGS_SELECT2
        empty_clause = "(predicate = 'tag' AND object = %s)"
        full_clause = ""
        for index, tag in enumerate(tags):
            if index != 0:
                full_clause += " OR "
            full_clause += cur.mogrify(empty_clause,(tag, )).decode("utf-8")
        query = query.replace("$$CLAUSES$$", full_clause)
        cur.execute(query, {"count": len(tags)})
        ids = []
        results = cur.fetchall()
        for r in results:
            ids.append(r[0])
        return tuple(ids)

    def get_objects(self, tags=None, properties=None):
        cur = Postgres.conn.cursor()
        query_to_execute = Postgres.BASIC_SELECT        # here check if all possible filter variables are None or not. If not, apply filter
        tag_filter_ids = tuple()
        property_filter_ids = tuple()
        if tags is not None:
            tag_filter_ids = self.__filter_by_tags(tags.split(","))
            # if tag filter is returns nothing, filter is too strinct, return no objects. Same thing happens near property filer
            if len(tag_filter_ids) == 0:
                return {}
            query_to_execute += " AND subject IN %(tag_filter_ids)s "
        if properties is not None:
            # TODO: this. replicate tag if condition
            pass
        cur.execute(query_to_execute, {'tag_filter_ids': tag_filter_ids, "property_filter_ids": property_filter_ids})
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
            
    def get_tags(self):
        cur = Postgres.conn.cursor()
        cur.execute("SELECT DISTINCT object from triples WHERE predicate = 'tag'")
        records = cur.fetchall()
        tags = []
        for tag in records:
            tags.append(tag[0])
        return tags