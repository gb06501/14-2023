import argparse, db_functions

# --docker bemeneti paraméter parancssorból amennyiben konténer
parser = argparse.ArgumentParser(description='Run database tests.')
parser.add_argument('--docker', action='store_true', help='Run tests on Docker')

args = parser.parse_args()

# adatbázis tesztekhez kapcsolódó függvények

restore_mysql_database="capture_pidstat(mysql_functions.restore_mysql_database, logged_functions, csv_collections)" 
backup_mysql_database="capture_pidstat(mysql_functions.backup_mysql_database, logged_functions, csv_collections)"
create_mysql_tables="capture_pidstat(mysql_functions.create_mysql_tables, logged_functions, csv_collections)"
copy_csv_mysql="capture_pidstat(mysql_functions.copy_csv_mysql, logged_functions,csv_collections)"
drop_mysql_database="capture_pidstat(mysql_functions.drop_mysql_database, logged_functions, csv_collections)"
mysql_update_simple="capture_pidstat(mysql_functions.execute_mysql_query,logged_functions,csv_collections,mysql_functions.mysql_update_simple)"
mysql_update_complex="capture_pidstat(mysql_functions.execute_mysql_query,logged_functions,csv_collections,mysql_functions.mysql_update_complex)"
mysql_query_single="capture_pidstat(mysql_functions.execute_mysql_query,logged_functions,csv_collections,mysql_functions.mysql_query_single)"
mysql_query_multi="capture_pidstat(mysql_functions.execute_mysql_query,logged_functions,csv_collections,mysql_functions.mysql_query_multi)"
mysql_query_aggregate="capture_pidstat(mysql_functions.execute_mysql_query,logged_functions,csv_collections,mysql_functions.mysql_query_aggregate)"
mysql_query_conditions="capture_pidstat(mysql_functions.execute_mysql_query,logged_functions,csv_collections,mysql_functions.mysql_query_conditions)"
service_restart=f"service_restart('mysql', {args.docker} )"

# szótár ami a teszteket és azok lépéseit, ismétlődésszámát írja le

mysql_tests = {
    'test1': {'functions': [create_mysql_tables, copy_csv_mysql], 'repeat': 1},
    'test2': {'functions': [drop_mysql_database], 'repeat': 1},
    'test3': {'functions': [create_mysql_tables, copy_csv_mysql, drop_mysql_database], 'repeat': 3},
    'test4': {'functions': [create_mysql_tables, copy_csv_mysql, mysql_update_simple, drop_mysql_database], 'repeat': 1},
    'test5': {'functions': [create_mysql_tables, copy_csv_mysql, mysql_update_complex, drop_mysql_database], 'repeat': 1},
    'test6': {'functions': [create_mysql_tables, copy_csv_mysql, mysql_query_single, drop_mysql_database], 'repeat': 1},
    'test7': {'functions': [create_mysql_tables, copy_csv_mysql, mysql_query_multi, drop_mysql_database], 'repeat': 1},
    'test8': {'functions': [create_mysql_tables, copy_csv_mysql, mysql_query_aggregate, drop_mysql_database], 'repeat': 1},    
    'test9': {'functions': [create_mysql_tables, copy_csv_mysql, mysql_query_conditions, drop_mysql_database], 'repeat': 1},
    'global_repeat': 5 
}

# adatbázis tesztek futtatása
db_functions.run_tests(mysql_tests)