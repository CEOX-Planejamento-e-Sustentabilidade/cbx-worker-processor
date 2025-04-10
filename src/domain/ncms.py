from services.utils import get_db_connection

def get_ncms():
    with get_db_connection() as connection, connection.cursor() as cursor:
        query = """
            SELECT id, name, ncm, status, type_ncm, group_ncm, properties
            FROM cbx.ncms
        """
        cursor.execute(query)
        rs = cursor.fetchall()
        results = [
            {
                "id": ncms[0],
                "name": ncms[1],
                "ncm_number": ncms[2],
                "status": ncms[3],
                "type": ncms[4],
                "group": ncms[5],
                "properties": ncms[6]
            } for ncms in rs]

        return rs, results
