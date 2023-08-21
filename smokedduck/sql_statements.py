# Not resilient to multiple users of this connection, but should be good enough for now
def get_query_id_and_plan() -> str:
    return '''
    select
        query_id,
        plan
    from duckdb_queries_list()
    where query = ?
    order by query_id desc
    limit 1
    '''

def get_tables_without_lineage() -> str:
    return '''
    select *
    from information_schema.tables
    where not contains(lower(table_name), 'lineage')
    '''

def get_lineage_tables(query_id: int, only_names: bool = False) -> str:
    return f'''
    select {"table_name" if only_names else "*"}
    from information_schema.tables
    where contains(lower(table_name), 'lineage{"" if query_id == -1 else f"_{query_id}_"}')
    order by table_name asc
    '''

def check_finalize(finalize_table_name) -> str:
    return f'select count(*) as cnt from {finalize_table_name}'
