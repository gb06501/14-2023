import argparse, db_functions

# --docker bemeneti paraméter parancssorból amennyiben konténer
parser = argparse.ArgumentParser(description='Run database tests.')
parser.add_argument('--docker', action='store_true', help='Run tests on Docker')

args = parser.parse_args()

# adatbázis tesztekhez kapcsolódó függvények

service_restart=f"service_restart('postgres', {args.docker} )"
restore_postgres_database="capture_pidstat(postgres_functions.restore_postgres_database, logged_functions, csv_collections)" 
backup_postgres_database="capture_pidstat(postgres_functions.backup_postgres_database, logged_functions, csv_collections)"
create_postgres_tables="capture_pidstat(postgres_functions.create_postgres_tables, logged_functions, csv_collections)"
postgres_copy_csv="capture_pidstat(postgres_functions.postgres_copy_csv, logged_functions,csv_collections)"
drop_postgres_database="capture_pidstat(postgres_functions.drop_postgres_database, logged_functions, csv_collections)"
postgres_update_simple="capture_pidstat(postgres_functions.execute_postgres_query,logged_functions,csv_collections,postgres_functions.postgres_update_simple)"
postgres_update_complex="capture_pidstat(postgres_functions.execute_postgres_query,logged_functions,csv_collections,postgres_functions.postgres_update_complex)"
postgres_query_single="capture_pidstat(postgres_functions.execute_postgres_query,logged_functions,csv_collections,postgres_functions.postgres_query_single)"
postgres_query_multi="capture_pidstat(postgres_functions.execute_postgres_query,logged_functions,csv_collections,postgres_functions.postgres_query_multi)"
postgres_query_aggregate="capture_pidstat(postgres_functions.execute_postgres_query,logged_functions,csv_collections,postgres_functions.postgres_query_aggregate)"
postgres_query_conditions="capture_pidstat(postgres_functions.execute_postgres_query,logged_functions,csv_collections,postgres_functions.postgres_query_conditions)"

# szótár ami a teszteket és azok lépéseit, ismétlődésszámát írja le

postgres_tests = {
    'test1': {'functions': [create_postgres_tables, postgres_copy_csv], 'repeat': 1},
    'test2': {'functions': [drop_postgres_database], 'repeat': 1},
    'test3': {'functions': [create_postgres_tables, postgres_copy_csv, drop_postgres_database], 'repeat': 3},
    'test4': {'functions': [create_postgres_tables, postgres_copy_csv, postgres_update_simple, drop_postgres_database], 'repeat': 1},
    'test5': {'functions': [create_postgres_tables, postgres_copy_csv, postgres_update_complex, drop_postgres_database], 'repeat': 1},
    'test6': {'functions': [create_postgres_tables, postgres_copy_csv, postgres_query_single, drop_postgres_database], 'repeat': 1},
    'test7': {'functions': [create_postgres_tables, postgres_copy_csv, postgres_query_multi, drop_postgres_database], 'repeat': 1},
    'test8': {'functions': [create_postgres_tables, postgres_copy_csv, postgres_query_aggregate, drop_postgres_database], 'repeat': 1},    
    'test9': {'functions': [create_postgres_tables, postgres_copy_csv, postgres_query_conditions, drop_postgres_database], 'repeat': 1},
    'global_repeat': 5  
}

# adatbázis tesztek futtatása
db_functions.run_tests(postgres_tests)