import argparse, db_functions

# --docker bemeneti paraméter parancssorból amennyiben konténer
parser = argparse.ArgumentParser(description='Run database tests.')
parser.add_argument('--docker', action='store_true', help='Run tests on Docker')

args = parser.parse_args()

# adatbázis tesztekhez kapcsolódó függvények

restore_mongo_database="capture_pidstat(mongo_functions.restore_mongo_database, logged_functions, csv_collections)" 
backup_mongo_database="capture_pidstat(mongo_functions.backup_mongo_database, logged_functions, csv_collections)"
create_ais_collections="capture_pidstat(mongo_functions.create_ais_collections, logged_functions, csv_collections)" 
import_csv_to_mongodb="capture_pidstat(mongo_functions.import_csv_to_mongodb, logged_functions, csv_collections)" 
delete_mongo_database="capture_pidstat(mongo_functions.delete_mongo_database, logged_functions, csv_collections)"
mongo_update_single="capture_pidstat(mongo_functions.mongo_test,logged_functions,mongo_functions.mongo_update_single)"
mongo_update_multi="capture_pidstat(mongo_functions.mongo_test,logged_functions,mongo_functions.mongo_update_multi)"
mongo_query_single="capture_pidstat(mongo_functions.mongo_test,logged_functions,mongo_functions.mongo_query_single)"
mongo_query_multi="capture_pidstat(mongo_functions.mongo_test,logged_functions,mongo_functions.mongo_query_multi)"
mongo_query_aggregate="capture_pidstat(mongo_functions.mongo_test,logged_functions,mongo_functions.mongo_query_aggregate)"
mongo_query_conditions="capture_pidstat(mongo_functions.mongo_test,logged_functions,mongo_functions.mongo_query_conditions)"
service_restart=f"service_restart('mongo', {args.docker} )"


# szótár ami a teszteket és azok lépéseit, ismétlődésszámát írja le

mongo_tests = {
    'test1': {'functions': [create_ais_collections, import_csv_to_mongodb, service_restart], 'repeat': 1},
    'test2': {'functions': [delete_mongo_database, service_restart], 'repeat': 1},
    'test3': {'functions': [create_ais_collections, import_csv_to_mongodb, delete_mongo_database, service_restart], 'repeat': 3},
    'test4': {'functions': [restore_mongo_database, mongo_update_single, service_restart], 'repeat': 1},
    'test5': {'functions': [restore_mongo_database, mongo_update_multi, service_restart], 'repeat': 1},
    'test6': {'functions': [restore_mongo_database, mongo_query_single, service_restart], 'repeat': 1},
    'test7': {'functions': [restore_mongo_database, mongo_query_multi, service_restart], 'repeat': 1},
    'test8': {'functions': [restore_mongo_database, mongo_query_aggregate, service_restart], 'repeat': 1},      
    'test9': {'functions': [restore_mongo_database, mongo_query_conditions, service_restart], 'repeat': 1},
    'test10': {'functions': [delete_mongo_database], 'repeat': 1},  
    'global_repeat': 5 
}

# adatbázis tesztek futtatása
db_functions.run_tests(mongo_tests)