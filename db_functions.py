import os, urllib.request, zipfile, subprocess, time, datetime, signal, inspect
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from mysecrets import smtp_password



def get_time():
# Formázott idő string és a Unix időbélyeg 
    return f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} ({int(time.time())})"

def service_restart(db_engine,docker = False):
# szervíz vagy docker konténer újraindítása
    if docker:
        # Docker konténer újraindítása
        service_restart_cmd=f"docker restart $(docker ps -a"
    else:
        # Linux szervíz újraindítása
        service_restart_cmd=f"sudo systemctl restart $(systemctl list-unit-files"
    # konténer/szervíz a találat első oszlopa
    service_restart_cmd+=f" | grep {db_engine} | awk '{{print $1}}')"
    subprocess.run(service_restart_cmd, shell=True)
    return service_restart_cmd


def run_tests(tests, until_test=None):
# teszt sorozat futtatása a bemeneti szótár alapján
    script_start_time = time.time()
    test_count = sum(isinstance(val, dict) for val in tests.values())

    # ha nem null tesztelés céljából csak eddig a tesztig 
    if until_test is not None:
        until_test = min(until_test, test_count)

    # Adatbázis modul importálása függvény stringek alapján
    db_modules = {}
    for test_name, test in tests.items():
        if test_name == 'global_repeat':
            continue
        for func_str in test['functions']:
            if 'mysql' in func_str:
                import mysql_functions
                db_modules['mysql'] = mysql_functions                
            elif 'postgres' in func_str:
                import postgres_functions
                db_modules['postgres'] = postgres_functions                
            elif 'mongo' in func_str:
                import mongo_functions
                db_modules['mongo'] = mongo_functions
                
    for global_run in range(tests.get('global_repeat', 1)):
        run_test = True
        for test_idx, (test_name, test) in enumerate(tests.items()):
            if test_name == 'global_repeat':
                continue
            if not until_test or test_idx <= until_test:
                # Teszt futtatása
                print(f"\n\nRunning {test_name} (global {global_run+1}/{tests.get('global_repeat', 1)})...\n")
                for i in range(test['repeat']):
                    test_start_time = time.time()
                    test_start_local_time = time.localtime()
                    print(f"{time.strftime('%Y-%m-%d %H:%M:%S', test_start_local_time)} ({int(test_start_time)}) test started")
                    for j, func_str in enumerate(test['functions']):
                        try:
                            print(f"\n{get_time()} Running {func_str} (function={j+1}/{len(test['functions'])}, repeat={i+1}/{test['repeat']}, global {global_run+1}/{tests.get('global_repeat', 1)}) \n")
                            func_start_time = time.time()
                            # Aktuális tesztlépés futtatása
                            func = eval(func_str)
                            func_end_time = time.time()
                            func_elapsed_time = func_end_time - func_start_time
                            print(f"\n{get_time()} Executed {func_str} (function={j+1}/{len(test['functions'])}, repeat={i+1}/{test['repeat']}, global {global_run+1}/{tests.get('global_repeat', 1)}) in {func_elapsed_time:.3f} seconds\n\n")
                        except Exception as e:
                            print(f"\n{get_time()} Error running {func_str}: {e}\n")
                    test_end_time = time.time()
                    test_elapsed_time = test_end_time - test_start_time
                    test_end_local_time = time.localtime()
                    print(f"{time.strftime('%Y-%m-%d %H:%M:%S', test_end_local_time)} ({int(test_end_time)}) Executed {test_name} ({i+1}/{test['repeat']}, global {global_run+1}/{tests.get('global_repeat', 1)}) in {test_elapsed_time:.3f} seconds\n")
                if until_test is not None and test_idx >= until_test:
                    break
    script_end_time = time.time()
    script_elapsed_time = script_end_time - script_start_time
    script_end_local_time = time.localtime()
    print(f"\n\n{time.strftime('%Y-%m-%d %H:%M:%S', script_end_local_time)} ({int(script_end_time)}) Executed script in {script_elapsed_time:.3f} seconds")
    


# szótár amely a tesztlépések logolási paramétereit tartalmazza
logged_functions = {
    'import_csv_to_mongodb': {
        'cmds_logged': 'python3|mongo|awk|gzip',
        'perf_log': '/home/tesztek/mongo_import.log',
        'log_interval': 10
    },   
    'backup_mongo_database': {
        'cmds_logged': 'python3|mongo|gzip',
        'perf_log': '/home/tesztek/mongo_backup.log',
        'log_interval': 10
    },      
     'restore_mongo_database': {
        'cmds_logged': 'python3|mongo|gzip',
        'perf_log': '/home/tesztek/mongo_restore.log',
        'log_interval': 10
    },
    'delete_mongo_database': {
        'cmds_logged': 'python3|mongo',
        'perf_log': '/home/tesztek/mongo_deletedb.log',
        'log_interval': 1
        },
    'create_ais_collections': {
        'cmds_logged': 'python3|mongo',
        'perf_log': '/home/tesztek/mongo_createcollections.log',
        'log_interval': 1
    },
    'mongo_test': {
        'cmds_logged': 'python3|mongo',
        'perf_log': '/home/tesztek/mongo_test.log',
        'log_interval': 5
    },
    'postgres_copy_csv': {
        'cmds_logged': 'postgres|python3',
        'perf_log': '/home/tesztek/postgres_copy.log',
        'log_interval': 10
     },
     'drop_postgres_database': {
        'cmds_logged': 'postgres|python3',
        'perf_log': '/home/tesztek/postgres_drop.log',
        'log_interval': 1
     },
     'create_postgres_tables': {
        'cmds_logged': 'postgres|python3',
        'perf_log': '/home/tesztek/postgres_create.log',
        'log_interval': 1
     },         
     'execute_postgres_query': {
        'cmds_logged': 'postgres|python3',
        'perf_log': '/home/tesztek/postgres_exec_query.log',
        'log_interval': 5
     },         
    'copy_csv_mysql': {
        'cmds_logged': 'mysql|gzip|python3',
        'perf_log': '/home/tesztek/mysql_copy.log',
        'log_interval': 10
     },
    'restore_mysql_database': {
        'cmds_logged': 'mysql|gzip|python3',
        'perf_log': '/home/tesztek/mysql_restore.log',
        'log_interval': 10
     },
     'backup_mysql_database': {
        'cmds_logged': 'mysql|gzip|python3',
        'perf_log': '/home/tesztek/mysql_backup.log',
        'log_interval': 10
     },
     'drop_mysql_database': {
        'cmds_logged': 'mysql|python3',
        'perf_log': '/home/tesztek/mysql_drop.log',
        'log_interval': 1
     },
     'create_mysql_tables': {
        'cmds_logged': 'mysql|python3',
        'perf_log': '/home/tesztek/mysql_createtables.log',
        'log_interval': 1
     },
     'execute_mysql_query': {
        'cmds_logged': 'mysql|python3',
        'perf_log': '/home/tesztek/mysql_execquery.log',
        'log_interval': 5
     }
}

#szótár ami a kollekciók/adatbázisok létrehozásához, beolvasásához és egyéb műveleteihez szükséges
csv_collections = {
    "database": "ais_data",
    "mongo_backup": "/var/db/csv_store/mongodb",
    "postgres_backup": "/var/db/csv_store/postgres",
    "mysql_backup": "/var/db/csv_store/mysql",
    "basestationreport": {
        "filepath": "/var/db/csv_store/*_basestationreport.csv.gz",
        "convert": {
            "MMSI": int,
            "BaseDateTime": lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S'),
            "LAT": float,
            "LON": float,
            "SOG": float,
            "COG": float,
            "Heading": float,
            "Status": int,
            "Draft": float,
            "Cargo": int
        },
        "fields": [
            "MMSI",
            "BaseDateTime",
            "LAT",
            "LON",
            "SOG",
            "COG",
            "Heading",
            "Status",
            "Draft",
            "Cargo"
        ],
        "unique": None,
        "index": "BaseStationReport"
    },
    "vessels": {
        "filepath": "/var/db/csv_store/*_vessels.csv.gz",
        "convert": {
            "MMSI": int,
            "VesselType": int,
            "Length": int,
            "Width": int
        },
        "fields": [
            "MMSI",
            "VesselName",
            "IMO",
            "CallSign",
            "VesselType",
            "Length",
            "Width",
            "TranscieverClass"
        ],
        "unique": "MMSI"
    },
    "vesselstatus": {
        "filepath": "/home/ubuntu/adatbazis/vesselstatus.csv.gz",
        "convert": {
            "status": int
        },
        "fields": [
            "status",
            "text"
        ],
        "unique": "status"
    },
    "vesseltypes": {
        "filepath": "/home/ubuntu/adatbazis/vesseltypes.csv.gz",
        "convert": {
            "type": int
        },
        "fields": [
            "type",
            "text"
        ],
        "unique": "type"
    },
    "maritime_id": {
        "filepath": "/home/ubuntu/adatbazis/maritime_id.csv.gz",
        "convert": {
            "mid": int
        },
        "fields": [
            "mid",
            "country",
            "code"
        ],
        "unique": "mid"
    }
}


def function_name():
# a hívó függvény neve
    frame = inspect.currentframe()
    if frame:
        frame = frame.f_back
    # a hívó függvény lekérdezése
    calling_function = frame.f_code.co_name if frame else None
    return calling_function

def send_email_with_attachment(subject, body, attachment_path):
# email küldés csatolmánnyal
    # Multipart üzenet objektum létrehozása
    msg = MIMEMultipart()

    # Feladó és címzett címek beállítása
    msg['From'] = 'gergely.baross81@gmail.com'
    msg['To'] = msg['From']

    # Az üzenet tárgyának és tartalmának beállítása
    msg['Subject'] = subject
    body_text = MIMEText(body)
    msg.attach(body_text)

    # Fájl megnyitása bináris módban
    with open(attachment_path, 'rb') as attachment_file:
        # Fájl hozzáadása application/octet-stream formátumban
        # Az e-mail kliens általában automatikusan letölti ezt mellékletként
        attachment = MIMEApplication(attachment_file.read(), _subtype='octet-stream')

    attachment.add_header('Content-Disposition', 'attachment', filename=attachment_path.split("/")[-1])
    msg.attach(attachment)

    # SMTP objektum létrehozása és bejelentkezés az e-mail fiókba
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587
    smtp_conn = smtplib.SMTP(smtp_server, smtp_port)
    smtp_conn.ehlo()
    smtp_conn.starttls()
    smtp_conn.login(msg['From'], smtp_password)

    # E-mail elküldése és az SMTP kapcsolat bezárása
    smtp_conn.sendmail(msg['From'] , msg['To'] , msg.as_string())
    smtp_conn.quit()
    return subject

def kill_process(pid):
# folyamat leállítása pid alapján
    # Megpróbáljuk többször leállítani a folyamatot.
    for i in range(5):
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            # A folyamat már leállt
            return

        time.sleep(0.75)

        # Nézzük meg hogy a folyamat fut-e még
        try:
            os.kill(pid, 0)
        except OSError:
            # OSError esetében a folyamat már valóban nem létezik
            return



def capture_pidstat(func, logged_functions, *args, **kwargs):
# a bemeneti függvény futási metrikáinak mérése    
    
    # Gyorsítótárak törlése, egyébként nem mindig mérhetőek az olvasási műveletek.
    os.system('sudo sync && sudo echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null')

    calling_function = func.__name__.split('(')[0]
    print(f"Cache cleared, starting {calling_function}")
 
    # Logfájl, logolt parancsok, és a gyakoriság kiolvasása
    file_name, file_ext = os.path.splitext(logged_functions[calling_function]['perf_log'])
    cmd = f"{logged_functions[calling_function]['cmds_logged']}"
    freq = logged_functions[calling_function]['log_interval']
    
    # ha létezik argumentum és az első argumentum meghívható akkor a hívó függvény legyen ennek a neve
    if len(args) > 0 and callable(args[0]):
        calling_function = args[0].__name__
    time.sleep(freq)

    start_time = time.time()
    start_timestamp = str(int(time.time()))

    output_file_with_timestamp = f"{file_name}_{calling_function}_{start_timestamp}{file_ext}"
    print(output_file_with_timestamp)
    
    # monitorozás kezdése
    pidstat_process = subprocess.Popen(
        "while true; do sudo pidstat -d -H " + str(freq) + " 1 -C '" + cmd + "' -r -s -u --dec=2 -h  |  awk '{sum8+=$8; sum14+=$14; sum17+=$17; sum18+=$18} END {print $1, sum8, sum14, sum17, sum18}' >> " + output_file_with_timestamp + " & sleep " + str(freq) + " ; done",
        shell=True  
    )
    
    # az argumentumban megadott függvénny hívása
    result=func(*args, **kwargs)
    
    # a monitorozás befejezése
    kill_process(pidstat_process.pid)

    end_time = time.time()
    duration = time.strftime("%H:%M:%S.%S", time.gmtime(end_time - start_time))
    output_file_with_duration = f"{output_file_with_timestamp}"
    time.sleep(freq)

    # eredmények mentése/küldése
    with open(output_file_with_duration, 'a') as f:
        f.write(f"\nExecution duration: {duration}\n")
    send_email_with_attachment(f"{calling_function} finished.", f"\nExecution duration: {duration}\n", output_file_with_duration)
    result_file_with_timestamp = f"{file_name}_{calling_function}_results_{start_timestamp}{file_ext}"
    with open(result_file_with_timestamp, 'a') as f:
        f.write(f"{result}")
    print(calling_function)


def download_csv_files():
# csv fájlok letöltése
    if not os.path.exists('/var/db/csv'):
        os.makedirs('/var/db/csv')

    day_input = input("Enter a day number between 1 and 31: ")
    if not day_input.isdigit() or int(day_input) < 1 or int(day_input) > 31:
        print("Invalid input. Please enter a day number between 1 and 31.")
        return

    day_num = int(day_input)
    for day in range(1, day_num+1):
        day_str = str(day).zfill(2)
        url = f"https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2021/AIS_2021_01_{day_str}.zip"
        filename = f"AIS_2021_01_{day_str}.zip"
        filepath = f"/var/db/csv/{filename}"
        csv_filepath = f"/var/db/csv/AIS_2021_01_{day_str}.csv"
        if os.path.exists(csv_filepath):
            print(f"{csv_filepath} already exists, skipping...")
            continue
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, filepath)
        print(f"Extracting {filename}...")
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            zip_ref.extractall('/var/db/csv')
        os.remove(filepath)


def process_csv_files(directory, awk_script, parallel=True):
# csv fájlok feldolgozása bemeneti könyvtár és awk szkript alapján, opcionálisan több szállon futtatva
    parallel_option = " -P $(nproc)" if parallel else ""
    cmd = f"find {directory} -type f -name '*.csv' | xargs{parallel_option} -I{{}} sh -c 'sudo pv -c -N $(basename \"{{}}\") {{}} | sudo awk -f {os.path.join(os.getcwd(), awk_script)} -v aisfile=\"{{}}\"'"
    cmd2 = f"find {directory} -type f -name '*vesselsdup.csv' | xargs{parallel_option} -I{{}} sh -c 'sudo pv -c -N $(basename \"{{}}\") {{}} | (sed -u 1q; sudo sort -t \",\" -u -k1,1 ) > \"$(echo \"{{}}\" | sed 's/dup.csv/.csv/')\" ; sudo rm -f {{}}'"

    subprocess.run(cmd, shell=True)
    subprocess.run(cmd2, shell=True)
    return "csv finished"

