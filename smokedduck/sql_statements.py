# Not resilient to multiple users of this connection, but should be good enough for now
def get_query_id_and_plan(query):
    return f'''
    select
        query_id,
        plan
    from duckdb_queries_list()
    where query = '{query}'
    order by query_id desc
    limit 1
    '''

def get_tables_without_lineage():
    return '''
    select *
    from information_schema.tables
    where not contains(lower(table_name), 'lineage')
    '''

def get_lineage_tables(query_id, only_names=False):
    return f'''
    select {"table_name" if only_names else "*"}
    from information_schema.tables
    where contains(lower(table_name), 'lineage{"" if query_id == -1 else f"_{query_id}_"}')
    order by table_name asc
    '''
