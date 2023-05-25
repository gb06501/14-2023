import psycopg2, os, glob, time, subprocess
from psycopg2.extras import execute_values
from tqdm import tqdm 

# Database configuration
database = "ais_data"

postgres_update_simple='''
-- tábla frissítése egyszerű feltétel alapján
UPDATE basestationreport
SET Status = 17
WHERE Status = 7;
'''

postgres_update_complex='''
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
        SIN(RADIANS(22.33333)) * SIN(RADIANS(br.lat))
        + COS(RADIANS(22.33333)) * COS(RADIANS(br.lat)) * COS(RADIANS(br.lon) - RADIANS(114.11666))
      ) AS distance_from_hongkong
    FROM basestationreport AS br
    -- basestationreport összekapcsolása az maritime_id táblával az MMSI első 3 karaktere alapján
    JOIN maritime_id AS m_id ON CAST(LEFT(CAST(br.mmsi AS TEXT), 3) AS INTEGER) = m_id.mid
    -- szűrés időintervallum alapján
    WHERE br.basedatetime BETWEEN '2021-01-01 09:00:00' AND '2021-01-01 11:00:00'
  ) AS hongkong_bs
  -- szűrés az 1000 km-nél távolabbi Hong Kong-i hajókra
  WHERE hongkong_bs.distance_from_hongkong > 1000
    AND hongkong_bs.country = 'Hong Kong'
);
'''

postgres_query_single='''
-- lekérdezés egyetlen táblából
SELECT * FROM basestationreport LIMIT 5000000;
'''

postgres_query_multi='''
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
JOIN maritime_id mi ON CAST(CASE WHEN br.MMSI > 100 THEN LEFT(CAST(br.MMSI AS TEXT), 3) ELSE CAST(br.MMSI AS TEXT) END AS INTEGER) = mi.mid;
'''

postgres_query_aggregate='''
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

postgres_query_conditions='''
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


def execute_postgres_query(csv_collections,query):
# lekérdezés futtatása a bemeneti szótár és egy lekérdezést/parancsot tároló sztring alapján
    try:
        # kapcsolódás az adatbázishoz
        conn = psycopg2.connect(
            database=csv_collections['database'],
            user='postgres')
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
        # egyébként a fetchmany metódus használatával a sorok megszámlálása
        else:
            while True:
                rows = cur.fetchmany(1000)
                if not rows:
                    break
                for row in rows:
                    counter += 1

        # kurzor és kapcsolat bezárása
        cur.close()
        conn.close()

        return execution_time, query, counter
    except Exception as e:
        print(f"Error executing query: {query}")
        print(str(e))



# Function to connect to database
def connect(csv_collections):
    conn = psycopg2.connect(
        database=csv_collections['database']
    )
    return conn

def drop_postgres_database(csv_collections):
    db_name = csv_collections['database']
    try:
        # Connect to the PostgreSQL server
        conn = psycopg2.connect(
        dbname=db_name,
        user='postgres'
        )

        
        # Create a cursor object
        conn.autocommit = True
        cur = conn.cursor()

        # Truncate tables to free up space
        cur.execute(f"SELECT truncate_tables();")

        # Commit the changes and close the cursor and connection
        cur.close()
        conn.close()

        # Connect to the PostgreSQL server
        conn = psycopg2.connect(
        dbname='postgres',
        user='postgres'
        )
        
        conn.autocommit = True
        cur = conn.cursor()
        # Drop the database
        cur.execute(f"DROP DATABASE IF EXISTS {database};")
        

        # Close the cursor and connection
        cur.close()
        conn.close()

        print(f"The {database} database has been successfully dropped.")
        return True
        
    except psycopg2.Error as err:
        print(f"Something went wrong: {err}")
        return False



def create_postgres_tables(csv_collections):
    try:
        # kapcsolódás a PostgreSQL szerverhez
        conn = psycopg2.connect(
            database="postgres",
            user="postgres"
            )

        # kursor objektum létrehozása, autocommit bekapcsolása
        conn.autocommit = True
        cur = conn.cursor()

        # Új adatbázis létrehozása
        db_name = csv_collections['database']
        cur.execute(f"CREATE DATABASE {db_name};")

        # kurzor és kapcsolat bezárása
        cur.close()
        conn.close()

        # kapcsolódás a PostgreSQL szerverhez
        conn = psycopg2.connect(
            database=db_name,
            user="postgres"
            )

        # kursor objektum létrehozása        
        cur = conn.cursor()

        # basestationreport tábla létrehozása
        print("Creating table basestationreport...")
        cur.execute('''
            CREATE TABLE basestationreport (
                sid SERIAL PRIMARY KEY,
                MMSI INTEGER NOT NULL,
                BaseDateTime TIMESTAMP WITH TIME ZONE NOT NULL,
                LAT NUMERIC(8,6),
                LON NUMERIC(9,6),
                SOG NUMERIC(4,1),
                COG NUMERIC(4,1),
                Heading NUMERIC(4,1) NULL,
                Status SMALLINT NULL,
                Draft NUMERIC(4,1) NULL,
                Cargo SMALLINT NULL
            )
        ''')
        print("Table basestationreport created.")

        # vessels tábla létrehozása
        print("Creating table vessels...")
        cur.execute('''
            CREATE TABLE vessels (
                MMSI INTEGER PRIMARY KEY,
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

        # funkció/trigger létrehozása a duplikáció elkerüléséhez a vessels táblában
        print("Creating function/trigger in vessels...")
        cur.execute("""
            CREATE OR REPLACE FUNCTION check_duplicate_vessels()
            RETURNS TRIGGER AS $$
            BEGIN
                -- Ellenőrizzük, hogy létezik-e már hajó ezzel az MMSI-vel a "vessels" táblában
                IF EXISTS (SELECT 1 FROM vessels WHERE mmsi = NEW.mmsi) THEN
                    RETURN NULL;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)
        cur.execute("""
            CREATE TRIGGER prevent_duplicate_vessels
            -- vessels táblában való beszúrás előtt   
            BEFORE INSERT ON vessels
            FOR EACH ROW
            EXECUTE FUNCTION check_duplicate_vessels();
        """)
        print("Successfully created function and trigger.")



        # https://stackoverflow.com/questions/2829158/truncating-all-tables-in-a-postgres-database/2829485#2829485
        cur.execute('''
            CREATE OR REPLACE FUNCTION truncate_tables() RETURNS void AS $$
            -- statements kurzor létrehozása és abban a public sémahoz tartozó tábla nevek tárolása
            DECLARE
                statements CURSOR FOR
                    SELECT tablename FROM pg_tables
                    WHERE schemaname = 'public';
            BEGIN
                -- az összes elemen végigmenni majd végrehajtani a 'TRUNCATE TABLE <tábla név> CASCADE' parancsot
                FOR stmt IN statements LOOP
                    EXECUTE 'TRUNCATE TABLE ' || quote_ident(stmt.tablename) || ' CASCADE;';
                END LOOP;
            END;
            $$ LANGUAGE plpgsql;
        ''')
        print("Successfully created truncate function.")

        # változtatások kommitálása, kurzor és kapcsolat bezárása
        conn.commit()
        cur.close()
        conn.close()
        print("Successfully committed changes.")
        return True

    except psycopg2.Error as err:
        print(f"Error: {err}")
        return False


def backup_postgres_database(csv_collections):
    """
    Ez a funckió mentést csinál egy PostgreSQL adatbázisról és a backup_folder könyvtárba menti el
    Args:
        csv_collections: a könyvtár és az adatbázis nevét adja át a szótár segítségével
    """
    backup_folder = csv_collections['postgres_backup']
    database = csv_collections['database']

    # a mentés fájl nevének meghatározása
    backup_file = os.path.join(backup_folder, f"{database}.sql.gz")

    # pg_dump parancs összeállítása
    command = f"sudo pg_dump -U postgres -Fc {database} | gzip > {backup_file}"

    # pg_dump parancs végrehajtása és a kimenet mentése
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()

    print(output.decode('utf-8'))
    if error:
        print(error.decode('utf-8'))
    return backup_file

def restore_postgres_database(csv_collections):
    """
    Ez a funkció visszaállítja a PostgreSQL adatbázist a backup_folder könyvtárból
    Args:
        csv_collections: a könyvtár és az adatbázis nevét adja át a szótár segítségével
    """
    db_name = csv_collections['database']
    backup_folder = csv_collections['postgres_backup']
    backup_file = os.path.join(backup_folder, f"{db_name}.sql.gz")

    # pg_restore parancs összeállítása
    command = f"zcat {backup_file} | pg_restore --create --dbname postgres"

    # pg_restore parancs végrehajtása és a kiment mentése
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()

    print(output.decode('utf-8'))
    if error:
        print(error.decode('utf-8'))
        return False
    else:
        return True


def postgres_copy_csv(csv_collections):
    try:
        for table_name, collection in csv_collections.items():
            if isinstance(collection, dict):
                # a beolvasandó fájlok listázása a 'filepath' alapján
                print(table_name,collection)
                # a fájlok sorrendezése
                file_list = sorted(glob.glob(collection["filepath"]))
                print(file_list)
                total_files = len(file_list)
                print(total_files)
                # menjünk végig a fájlokon a listában
                for i, filename in enumerate(tqdm(file_list, desc=f"Importing {table_name} data", total=total_files)):
                    # a bemeneti fájl elérésének meghatározása
                    input_file = os.path.join(filename)

                    print(f"Processing {input_file}...")

                    # COPY parancs explicit oszlopnevek megadásával, gzip -dc szerver oldali futtatása
                    fields = ', '.join(collection["fields"])
                    copy_sql = f"COPY {table_name} ({fields}) FROM PROGRAM 'gzip -dc {input_file}' DELIMITER ',' CSV HEADER ;"

                    # a kész parancs futtatása
                    copy_process = subprocess.Popen(['sudo', 'su', 'postgres', '-c', f'psql -d "{csv_collections["database"]}" -c "{copy_sql}"'], stdout=subprocess.PIPE)
                    copy_process.communicate()

        return "Data import successful"
    except Exception as e:
        return "Data import failed: " + str(e)