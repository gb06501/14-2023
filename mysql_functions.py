import os, subprocess, glob, time, mysql.connector, re, sys
from tqdm import tqdm

mysql_update_simple = '''
-- tábla frissítése egyszerű feltétel alapján
UPDATE basestationreport
SET Status = 17
WHERE Status = 7;
'''

mysql_update_complex = '''
-- tábla frissítése komplex feltétel alapján    
UPDATE basestationreport AS br
SET status = (
  -- 'not under command' státusz lekérdezése a vesselstatus gyűjteményből
  SELECT vs.status
    FROM vesselstatus AS vs
  WHERE LOWER(vs.text) LIKE '%not under command%'
  LIMIT 1
)
WHERE br.sid IN (
  SELECT hongkong_bs.sid
  FROM (
    -- distance_from_hongkong kiszámítása a Haversine formula segítségével
    SELECT
      br.sid,
      m_id.country,
      6371 * ACOS(
        SIN(RADIANS(22.33333)) * SIN(RADIANS(br.LAT))
        + COS(RADIANS(22.33333)) * COS(RADIANS(br.LAT)) * COS(RADIANS(br.LON) - RADIANS(114.11666))
      ) AS distance_from_hongkong
    FROM basestationreport AS br
    -- basestationreport összekapcsolása az maritime_id táblával az MMSI első 3 karaktere alapján
    JOIN maritime_id AS m_id ON CAST(LEFT(CAST(br.MMSI AS CHAR), 3) AS UNSIGNED) = m_id.mid
    -- szűrés időintervallum alapján
    WHERE br.BaseDateTime BETWEEN '2021-01-01 09:00:00' AND '2021-01-01 11:00:00'
  ) AS hongkong_bs
  -- szűrés az 1000 km-nél távolabbi Hong Kong-i hajókra
  WHERE hongkong_bs.distance_from_hongkong > 1000
    AND hongkong_bs.country = 'Hong Kong'
);
'''


mysql_query_single='''
-- lekérdezés egyetlen táblából
SELECT * FROM basestationreport LIMIT 5000000;
'''

mysql_query_multi = '''
-- Lekérdezés táblakapcsolatok alapján - A legtávolabbi hajó megtalálása a Miami kikötőtől
-- distance_from_miami kiszámítása (Miami kikötőtől való távolság) a Haversine formula segítségével, VesselName, CallSign, country lekérdezése
SELECT
    br.MMSI, br.LAT, br.LON,
    6371 * ACOS(
        SIN(RADIANS(25.77952)) * SIN(RADIANS(br.LAT)) +
        COS(RADIANS(25.77952)) * COS(RADIANS(br.LAT)) * COS(RADIANS(-80.17867 - br.LON))
    ) AS distance_from_miami,
    v.VesselName, v.CallSign, mi.country
FROM basestationreport br
-- táblák összekapcsolása Miami kikötőtől való legtávolabbi jelentés sid azonosítója alapján
JOIN (
    SELECT sid, MMSI, LAT, LON
    FROM basestationreport
    ORDER BY 6371 * ACOS(
        SIN(RADIANS(25.77952)) * SIN(RADIANS(LAT)) +
        COS(RADIANS(25.77952)) * COS(RADIANS(LAT)) * COS(RADIANS(-80.17867 - LON))
    ) DESC
    LIMIT 1
) td ON br.sid = td.sid
-- táblák összekapcsolása az MMSI alapján
JOIN vessels v ON br.MMSI = v.MMSI
-- táblák összekapcsolása az MMSI első 3 karaktere (amennyiben MMSI > 100) és a maritime_id.mid alapján
JOIN maritime_id mi ON CAST(CASE WHEN br.MMSI > 100 THEN LEFT(CAST(br.MMSI AS CHAR), 3) ELSE CAST(br.MMSI AS CHAR) END AS UNSIGNED INTEGER) = mi.mid;
'''

mysql_query_aggregate = '''
-- lekérdezés aggregált adatok alapján
SELECT vessels.VesselType AS sid, COUNT(*) AS num_reports
FROM basestationreport
-- basestationreport és vessels táblák összekapcsolása az MMSI oszlop alapján
INNER JOIN vessels ON basestationreport.MMSI = vessels.MMSI
WHERE BaseDateTime >= '2021-01-01' AND BaseDateTime <= '2021-01-05'
-- csoportosítás VesselType alapján
GROUP BY vessels.VesselType
-- rendezés a num_reports alapján csökkenő sorrendben
ORDER BY num_reports DESC;
'''
mysql_query_conditions = '''
-- lekérdezés kondíciók alapján
-- basetationreport,vessels,vesseltypes táblából lekérdezés 
SELECT br.MMSI, br.BaseDateTime, v.VesselName, v.VesselType, vt.text AS VesselTypeText
FROM basestationreport br
-- vessels táblával összekapcsolás az MMSI alapján
JOIN vessels v ON br.MMSI = v.MMSI
-- vesseltypes táblával összekapcsolás VesselType alapján
JOIN vesseltypes vt ON v.VesselType = vt.type
-- szűrés időintervallum,státusz, hajó hossz és típus (*Cargo*) alapján
WHERE br.BaseDateTime >= '2021-01-01 00:00:00' AND br.BaseDateTime < '2021-01-05 00:00:00'
AND br.Status = 1
AND v.Length > 30
AND vt.text LIKE '%Cargo%'
-- találatok limitálása
LIMIT 5000000;
'''


def execute_mysql_query(csv_collections,query):
# lekérdezés futtatása a bemeneti szótár és egy lekérdezést/parancsot tároló sztring alapján
    db_name = csv_collections['database']
    config = {
        'user': 'ubuntu',
        'password': '',
        'unix_socket': '/var/run/mysqld/mysqld.sock', 
        'database': db_name
    }


    try:
        # Kapcsolódás a MySQL szerverhez
        conn = mysql.connector.connect(**config)

        # Kurzor objektum létrehozása
        cur = conn.cursor()

        # SQL parancs futtatási és annak idejének mérése
        start_time = time.time()
        result=cur.execute(query)
        end_time = time.time()
        execution_time = end_time - start_time

        print("Execution time: ", execution_time)

        counter = 0
        # ha van UPDATE a parancsban akkor a rowcount attribútum használata
        if "UPDATE" in query:
            counter=cur.rowcount
        else:
        # egyébként a fetchmany metódus használatával a sorok megszámlálása
            while True:
                rows = cur.fetchmany(1000)
                if not rows:
                    break
                for row in rows:
                    counter += 1


        # változtatások kommitálása, kurzor és kapcsolat bezárása
        conn.commit()
        cur.close()
        conn.close()

        return execution_time, query, counter

    except mysql.connector.Error as err:
            if re.search(r"Unread result found", str(err)):
            # ezt a hibát ignoráljuk
                return execution_time, query, counter
            else:
                # egyébként meg írjuk ki
                print(f"Something went wrong: {err}")
                return f"Something went wrong: {err}"

def drop_mysql_database(csv_collections):
# adatbázis eltávolítása
    db_name = csv_collections['database']
    try:
        # MySQL szerverhez kapcsolódás

        conn = mysql.connector.connect(
            unix_socket="/var/run/mysqld/mysqld.sock",
            user="ubuntu",
            password="",
            database=db_name
            
        )
        cur = conn.cursor()
        
        # A táblák "csonkítása" funckió meghívása,hogy az OS visszakövetelhesse a felszabadult helyet
        cur.callproc(f"{db_name}.truncate_tables", (db_name, ))
        print("Truncate tables finished...")

        # kurzor objektum létrehozása
        cur = conn.cursor()

         # adatbázis törlése ha létezik
        cur.execute(f"DROP DATABASE IF EXISTS {db_name};")
        print(f"Drop database {db_name} finished...")
        # bináris logok "kitisztítása" amik kb 256MB-t tesznek ki minden beolvasott AIS csv után
        cur.execute(f"PURGE BINARY LOGS BEFORE DATE_ADD(CURDATE(), INTERVAL 1 DAY);")
        print(f"Purge all binary logs finished...")
        # változtatások kommitálása, kurzor és kapcsolat bezárása
        conn.commit()
        cur.close()
        conn.close()


        print(f"The {db_name} database has been successfully dropped.")
        return f"The {db_name} database has been successfully dropped."
        
    except mysql.connector.Error as err:
        print(f"Something went wrong: {err}")
        return f"Something went wrong: {err}"

def create_mysql_database(csv_collections):
    try:
        startup_config = {
            'user': 'ubuntu',
            'password': '',
            'unix_socket': '/var/run/mysqld/mysqld.sock', 
            'database': 'mysql'
        }

        # kapcsolódás a MySQL szerverhez

        conn = mysql.connector.connect(**startup_config)

        db_name=csv_collections['database']
        # kursor objektum létrehozása
        cur = conn.cursor()
        
        # Megnézzük létezik-e az adatbázis, ha igen akkor töröljük.
        cur.execute(f"SHOW DATABASES LIKE '{db_name}';")
        result = cur.fetchone()
        if result:
            cur.close()
            conn.close()
            drop_mysql_database(csv_collections)
            conn = mysql.connector.connect(**startup_config)

            db_name=csv_collections['database']
            cur = conn.cursor()

        # adatbázis létrehozása
        cur.execute(f'CREATE DATABASE {db_name};')

        # változtatások kommitálása, kurzor és kapcsolat bezárása
        conn.commit()
        cur.close()
        conn.close()

        return "Successfully created AIS database."
    
    except mysql.connector.Error as err:    
        return f"Error creating AIS tables: {err}"


def create_mysql_tables(csv_collections):
    try:
        # kapcsolódás a MySQL szerverhez

        startup_config = {
            'user': 'ubuntu',
            'password': '',
            'unix_socket': '/var/run/mysqld/mysqld.sock', 
            'database': ''
        }


        conn = mysql.connector.connect(**startup_config)

        db_name=csv_collections['database']
        # kursor objektum létrehozása
        cur = conn.cursor()

        # Nézzük meg, hogy létezik-e az adatbázis és töröljük
        cur.execute(f'DROP DATABASE IF EXISTS {db_name};')
        
        # Új adatbázis létrehozása
        cur.execute(f'CREATE DATABASE {db_name};')

        # változtatások kommitálása, kurzor és kapcsolat bezárása
        conn.commit()
        cur.close()
        conn.close()


        config = {
            'user': 'ubuntu',
            'password': '',
            'unix_socket': '/var/run/mysqld/mysqld.sock', 
            'database': db_name
        }

        # kapcsolódás a MySQL szerverhez
        conn = mysql.connector.connect(**config)

        cur = conn.cursor()

        # basestationreport tábla létrehozása
        print("Creating table basestationreport...")    
        cur.execute('''
            CREATE TABLE basestationreport (
                sid INT AUTO_INCREMENT PRIMARY KEY,
                MMSI INT NOT NULL,
                BaseDateTime DATETIME NOT NULL,
                LAT DECIMAL(8,6),
                LON DECIMAL(9,6),
                SOG DECIMAL(4,1),
                COG DECIMAL(4,1),
                Heading DECIMAL(4,1) NULL,
                Status SMALLINT NULL,
                Draft DECIMAL(4,1) NULL,
                Cargo SMALLINT NULL
            )
        ''')
        print("Table basestationreport created.")

        # vessels tábla létrehozása
        print("Creating table vessels...")
        cur.execute('''
            CREATE TABLE vessels (
                MMSI INT PRIMARY KEY,
                VesselName VARCHAR(20) NULL,
                IMO VARCHAR(16) NULL,
                CallSign VARCHAR(10) NULL,
                VesselType SMALLINT NULL,
                Length SMALLINT NULL,
                Width SMALLINT NULL,
                TranscieverClass CHAR(1) NULL
            )
        ''')
        print("Table vessels created.")

        # maritime_id tábla létrehozása
        print("Creating table maritime_id...")
        cur.execute('''
            CREATE TABLE maritime_id (
                mid SMALLINT PRIMARY KEY,
                country VARCHAR(30),
                code VARCHAR(2)
            )
        ''')
        print("Table maritime_id created.")

        # vesseltypes tábla létrehozása
        print("Creating table vesseltypes...")
        cur.execute('''
            CREATE TABLE vesseltypes (
                type SMALLINT PRIMARY KEY,
                text VARCHAR(60)
            )
        ''')
        print("Table vesseltypes created.")

        # vesselstatus tábla létrehozása
        print("Creating table vesselstatus...")
        cur.execute('''
            CREATE TABLE vesselstatus (
                status SMALLINT PRIMARY KEY,
                text VARCHAR(255)
            )
        ''')
        print("Table vesselstatus created.")
       
        
        # ais_data.truncate_tables procedúra törlése ha már létezne
        cur.execute("DROP PROCEDURE IF EXISTS ais_data.truncate_tables;")
        # a procedúra létrehozása amivel az adatbázis összes tábláját csonkítjuk
        # https://stackoverflow.com/a/33462820/21931685
        cur.execute("""
            CREATE PROCEDURE ais_data.truncate_tables(mydb VARCHAR(64))
            BEGIN
                DECLARE tables VARCHAR(64);
                DECLARE done INT DEFAULT FALSE;
                DECLARE tcursor CURSOR FOR 
                    SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = mydb;
                DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
                SET FOREIGN_KEY_CHECKS = 0;
                OPEN tcursor;
                l1: LOOP
                    FETCH tcursor INTO tables;
                    IF done THEN
                        LEAVE l1;
                    END IF;
                    SET @sql = CONCAT('TRUNCATE TABLE `', mydb, '`.`', tables, '`;');
                    PREPARE stmt FROM @sql;
                    EXECUTE stmt;
                    DEALLOCATE PREPARE stmt;
                END LOOP l1;
                CLOSE tcursor;
                SET FOREIGN_KEY_CHECKS = 1;
            END
        """)

        print("Creating procedure truncate_tables or not...")

        # változtatások kommitálása, kurzor és kapcsolat bezárása
        conn.commit()
        cur.close()
        conn.close()

        return "Successfully created AIS tables and truncate_tables procedure."

    except mysql.connector.Error as err:
        print(f"Error creating AIS tables: {err}")
        return f"Error creating AIS tables: {err}"
        
def copy_csv_mysql(csv_collections, Test=False):
    try:
        for table_name, collection in csv_collections.items():
            # folytatás ha egy szótár
            if isinstance(collection, dict):
                print(table_name)
                # sorrendbe teszi a fájlokat
                file_list = sorted(glob.glob(collection["filepath"]))
                if Test:
                    file_list = file_list[:3]  # ha Test=True akkor max 3 fájl
                for filename in tqdm(file_list, desc=table_name):
                    input_file = os.path.join(filename)
                    print(input_file)
                    # autocommit,foreign_key_checks,sql_log_bin ideiglenes kikapcsolása a gyorsabb beolvasás érdekében
                    fields = ', '.join(collection["fields"])
                    prefix = f'''
                    set autocommit = 0;   
                    set foreign_key_checks = 0;
                    set sql_log_bin = 0;'''
                    suffix = f'''
                    commit;
                    set autocommit = 1;
                    set foreign_key_checks = 1;
                    set sql_log_bin = 1;'''
                    
                    # basestationreport tábla esetén az egyediség ellenőrzésének kikapcsolása a gyorsabb beolvasás érdekében
                    if table_name == 'basestationreport':
                        ignore = ''
                        prefix += f'''
                        set unique_checks = 0;'''
                        suffix += f'''
                        set unique_checks = 1;'''
                    else:
                        ignore = 'IGNORE '

                    # a mysql által végrehajtott parancs összeállítása,
                    load_sql = f"""zcat {input_file} | mysql --force --local-infile --verbose -D {csv_collections['database']} -e "{prefix}LOAD DATA LOCAL INFILE '/dev/stdin' {ignore}INTO TABLE {table_name} FIELDS TERMINATED BY ',' ENCLOSED BY '\\"' LINES TERMINATED BY '\\n' IGNORE 1 ROWS ({fields})"""
                    if table_name == 'basestationreport':
                        load_sql += ' SET sid=NULL;'
                    else:
                        load_sql += ';'
                    load_sql = load_sql + suffix + '" '
                    # a parancs végrehajtása és az idő mérése
                    start_time = time.time()
                    mysql_process = subprocess.Popen([load_sql], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    output, error = mysql_process.communicate()
                    print(error)
                    elapsed_time = time.time() - start_time
                    if error:
                        raise Exception(error)
                    print(f"Processed {input_file} in {elapsed_time:.2f} seconds")
        return "Data import successful"
    except Exception as err:
        return f"Data import failed: {err}"




def backup_mysql_database(csv_collections):
    """
    Ez a funckió mentést csinál egy MySQL adatbázisról és a backup_folder könyvtárba menti el
    Args:
        csv_collections: a könyvtár és az adatbázis nevét adja át a szótár segítségével
    """

    backup_folder = csv_collections['mysql_backup']
    db_name = csv_collections['database']

    # a mentés fájl nevének meghatározása
    backup_file = os.path.join(backup_folder, f"{db_name}.sql.gz")

    # mysqldump parancs összeállítása 
    command = f"mysqldump --add-drop-table {db_name} | gzip > {backup_file}"

    # mysqldump parancs végrehajtása és a kimenet elmentése
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()

    print(output.decode('utf-8'))
    if error:
        print(error.decode('utf-8'))
    return f"Backup MySQL db into {backup_file} successful."

def restore_mysql_database(csv_collections):
    """
    Ez a funkció visszaállítja a MySQL adatbázist a backup_folder könyvtárból
    Args:
        csv_collections: a könyvtár és az adatbázis nevét adja át a szótár segítségével
    """
    # adatbázis létrehozása
    create_mysql_database(csv_collections)
    db_name = csv_collections['database']
    backup_folder = csv_collections['mysql_backup']
    backup_file = os.path.join(backup_folder, f"{db_name}.sql.gz")
    # mysql parancs összeállítása
    command = f"zcat {backup_file} | mysql {db_name}"
    
    # mysql parancs végrehajtása és a kimenet elmentése
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()

    print(output.decode('utf-8'))
    if error:
        print(error.decode('utf-8'))
        return f"Restore database failed with {error.decode('utf-8')}"
    else:
        return "Restore database finished."
