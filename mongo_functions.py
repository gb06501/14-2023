import glob, os, subprocess, datetime, re
from tqdm import tqdm
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import pymongo

client = MongoClient()
db = client.ais_data


def mongo_test(func):
# mongo tesztlépés futtatása
    client = MongoClient()
    db = client.ais_data
    return func()

def mongo_update_single():
# tábla frissítése egyszerű feltétel alapján
    result = db.basestationreport.update_many(
        {"Status": 7},
        {"$set": {"Status": 17}}
    )
    return {'matched': result.matched_count, 'modified': result.modified_count}

def mongo_update_multi():
# tábla frissítése komplex feltétel alapján    
    find_pipeline = [
    # Az alábbi blokk az adatok szűrésére és előkészítésére szolgál

        # A dokumentumokat időintervallum alapján szűrjük
        {
            '$match': {
                'BaseDateTime': {
                    '$gte': datetime.datetime(2021, 1, 1, 9, 0),
                    '$lte': datetime.datetime(2021, 1, 1, 11, 0),
                },
            },
        },
        # A 'mid' mezőt hozzáadjuk a dokumentumokhoz, ami az 'MMSI' mező első 3 karaktere egész számmá konvertálva
        {
            '$addFields': {
                'mid': {
                    '$toInt': {
                        '$substr': [
                            {'$toString': '$MMSI'},
                            0,
                            3,
                        ],
                    },
                },
            },
        },
        # A 'mid' mező alapján csatlakoztatjuk a 'maritime_id' kollekciót a dokumentumokhoz
        {
            '$lookup': {
                'from': 'maritime_id',
                'localField': 'mid',
                'foreignField': 'mid',
                'as': 'maritime_id',
            },
        },
        # Szűrés országnév alapján
        {
            '$match': {
                'maritime_id.country': 'Hong Kong',
            },
        },
        # Távolság mérése Hong Kongtól a már szűrt halmazból
        {
            '$addFields': {
                'distance_from_hongkong': {
                    '$multiply': [
                        6371,
                        {
                            '$acos': {
                                '$add': [
                                    {
                                        '$multiply': [
                                            {'$sin': {'$degreesToRadians': 22.33333}},
                                            {'$sin': {'$degreesToRadians': '$LAT'}},
                                        ],
                                    },
                                    {
                                        '$multiply': [
                                            {'$cos': {'$degreesToRadians': 22.33333}},
                                            {'$cos': {'$degreesToRadians': '$LAT'}},
                                            {'$cos': {'$subtract': [{'$degreesToRadians': 114.11666}, {'$degreesToRadians': '$LON'}]}}
                                        ],
                                    },
                                ],
                            },
                        },
                    ],
                },
            },
        },
        # az 1000 km-nél távolabbi hajók szűrése
        {
            '$match': {
                'distance_from_hongkong': {'$gt': 1000},
            },
        },
        # az _id és a distance_from_hongkong dokumentumhoz adása
        {
            '$project': {
                '_id': 1,
                'distance_from_hongkong': 1,
            },
        },
    ]

    # Az eredmények lekérdezése a megadott pipeline alapján
    matching_docs = db.basestationreport.aggregate(find_pipeline)

    # A találatokhoz tartozó azonosítók összegyűjtése
    matching_ids = [doc['_id'] for doc in matching_docs]

    # 'not under command' státusz lekérdezése a vesselstatus gyűjteményből
    not_under_command_status = db.vesselstatus.find_one({'text': {'$regex': 'not under command', '$options': 'i'}})['status']

    # A találatokhoz tartozó dokumentumok frissítése a megadott státusszal
    result = db.basestationreport.update_many(
        {'_id': {'$in': matching_ids}},
        {'$set': {'Status': not_under_command_status}}
    )

    return {'matched': result.matched_count, 'modified': result.modified_count}

def mongo_query_single():
# lekérdezés egyetlen kollekcióból
    cursor = db.basestationreport.find().limit(5000000)
    count = 0
    # a ciklus azért kell hogy a lekérdezés eredményén valóban végigmenjünk
    for doc in cursor:
        count += 1
    return count


def mongo_query_multi():
# Lekérdezés táblakapcsolatok alapján - A legtávolabbi hajó megtalálása a Miami kikötőtől
    pipeline = [
        {
            # distance_from_miami mező hozzáadása ami a Miami kikötőtől való távolságot számolja ki a Haversine formula segítségével
            "$addFields": {
                "distance_from_miami": {
                    "$multiply": [
                        6371,
                        {
                            "$acos": {
                                "$add": [
                                    {
                                        "$multiply": [
                                            { "$sin": { "$degreesToRadians": 25.77952 } },
                                            { "$sin": { "$degreesToRadians": "$LAT" } }
                                        ]
                                    },
                                    {
                                        "$multiply": [
                                            { "$cos": { "$degreesToRadians": 25.77952 } },
                                            { "$cos": { "$degreesToRadians": "$LAT" } },
                                            { "$cos": { "$subtract": [ { "$degreesToRadians": -80.17867 }, { "$degreesToRadians": "$LON" } ] } }
                                        ]
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        },
        # csökkenő (-1) sorrendbe rendezi a dokumentumokat a mező alapján
        {
            "$sort": { "distance_from_miami": -1 }
        },
        # az első dokumentumot választja ki és dolgozza fel tovább
        {
            "$limit": 1
        },
        # összekapcsolás az MMSI alapján
        {
            "$lookup": {
                "from": "vessels",
                "localField": "MMSI",
                "foreignField": "MMSI",
                "as": "vessel"
            }
        },
        # mid mező létrehozása (amennyiben 3 mezőnél nem kisebb az MMSI hossza), ami az MMSI első három karaktere
        {
            "$addFields": {
                "mid": {
                    "$cond": [
                        { "$lt": [ { "$strLenCP": { "$toString": "$MMSI" } }, 3 ] },
                        None,
                        {
                            "$toInt": {
                                "$substr": [
                                    { "$toString": "$MMSI" },
                                    0,
                                    3
                                ]
                            }
                        }
                    ]
                }
            }
        },
        # mid mező alapján összekapcsoljuk az eddigi dokumentumokat a maritime_id kollekcióval
        {
            "$lookup": {
                "from": "maritime_id",
                "localField": "mid",
                "foreignField": "mid",
                "as": "maritime_id"
            }
        },
        # szűrés azon dokumentumokra ahol létezik és nem null a mid mező
        {
            "$match": {
                "mid": { "$exists": True }
            }
        },
        # az összekapcsolás során létrejött vessel tömb egyedi dokumentumokká bontása
        {
            "$unwind": "$vessel"
        },
        # a megjelenítésnél az _id mező kizárása, a felsoroltak megjelenítése
        {
            "$project": {
                "_id": 0,
                "MMSI": 1,
                "LAT": 1,
                "LON": 1,
                "distance_from_miami": 1,
                "VesselName": "$vessel.VesselName",
                "CallSign": "$vessel.CallSign",
                "maritime_id.country": 1
            }
        }
    ]

    return list(db.basestationreport.aggregate(pipeline))
  
def mongo_query_aggregate():
# aggregált lekérdezés
    pipeline = [
        # basestationreport és vessels összekapcsolás az MMSI alapján
        {
            "$lookup": {
                "from": "vessels",
                "localField": "MMSI",
                "foreignField": "MMSI",
                "as": "vessel"
            }
        },
        # szűrés időintervallum alapján
        {
            "$match": {
                "BaseDateTime": {
                    "$gte": datetime.datetime(2021, 1, 1),
                    "$lte": datetime.datetime(2021, 1, 5)
                }
            }
        },
        # VesselType mező alapján csoportosítás, az adott csoport előfordulásának számolása (num_reports)
        {
            "$group": {
                "_id": "$vessel.VesselType",
                "num_reports": { "$sum": 1 }
            }
        },
        # num_reports alapján csökkenő sorrendbe rendezés
        {
            "$sort": { "num_reports": -1 }
        }
    ]

    return list(db.basestationreport.aggregate(pipeline))


def mongo_query_conditions():
# lekérdezés kondíciók alapján
    pipeline = [
        # basestationreport és vessels összekapcsolás az MMSI alapján
        {
            "$lookup": {
            "from": "vessels",
            "localField": "MMSI",
            "foreignField": "MMSI",
            "as": "vessel"
            }
        },
        # az összekapcsolás során létrejött vessel tömb egyedi dokumentumokká bontása
        {
            "$unwind": "$vessel"
        },
        # vessel, vesseltypes összekapcsolása a vessel.VesselType alapján vesseltype néven
        {
            "$lookup": {
            "from": "vesseltypes",
            "localField": "vessel.VesselType",
            "foreignField": "type",
            "as": "vesseltype"
            }
        },
        # vesseltype tömb dokumentumokká bontása
        {
            "$unwind": "$vesseltype"
        },
        # szűrés időintervallum,státusz, hajó hossz és típus (*Cargo*) alapján
        {
            "$match": {
            "BaseDateTime": {
                "$gte": datetime.datetime(2021, 1, 1, 0, 0, 0),
                "$lt": datetime.datetime(2021, 1, 5, 0, 0, 0)
            },
            "Status": 1,
            "vessel.Length": {
                "$gt": 30
            },
            "vesseltype.text": re.compile(".*Cargo.*"),
            }
        },
        # a megjelenítésnél az _id mező kizárása, a felsoroltak megjelenítése
        {
            "$project": {
            "_id": 0,
            "MMSI": 1,
            "BaseDateTime": 1,
            "VesselName": "$vessel.VesselName",
            "VesselType": "$vessel.VesselType",
            "VesselTypeText": "$vesseltype.text"
            }
        },
        # a találatok limitálása
        {
            "$limit": 5000000
        }
    ]
    return list(db.basestationreport.aggregate(pipeline))

def delete_mongo_database(csv_collections):
# mongo adatbázis törlése argumentum alapján
    db_name=csv_collections['database']
    client = MongoClient()
    try:
        client.drop_database(db_name)
        message = f"Database '{db_name}' deleted successfully."
    except:
        message = f"Failed to delete database '{db_name}'."
    finally:
        client.close()
    return message


def create_ais_collections(csv_collections):
# kollekciók létrehozása bemeneti szótár alapján
    client = MongoClient()
    db_name = csv_collections['database']
    
    # ha létezik az adatbázis annak törlése
    if db_name in client.list_database_names():
        delete_mongo_database(csv_collections)

    db = client[db_name]
    # iteráció csak a szótár típusú elemek közt 
    for collection_name, collection in csv_collections.items():
        if isinstance(collection, dict):

            required_fields = []
            properties = {}
            # a megfelelő mezők hozzáadása, szükség esetén konverzió
            for field in collection["fields"]:
                if field in collection["convert"]:
                    field_type = collection["convert"][field]
                    if field_type == int:
                        properties[field] = {"bsonType": "int"}
                    elif field_type == float:
                        properties[field] = {"bsonType": "double"}
                    elif field_type == datetime.datetime:
                        properties[field] = {"bsonType": "date"}
                    else:
                        properties[field] = {"bsonType": "string"}
                else:
                    properties[field] = {"bsonType": "string"}
                if collection.get("unique") and field == collection["unique"]:
                    required_fields.append(field)

            print(f"Collection: {collection_name}")
            
            #séma létrehozása
            schema = {
                "$jsonSchema": {
                    "properties": properties
                }
            }

            # amennyiben volt egyedi mező, annak hozzáadása
            if required_fields:
                schema["$jsonSchema"]["required"] = required_fields

            db.create_collection(
                collection_name,
                validator=schema,
                validationAction="warn"
            )
            # index létrehozása az egyedi értékekre
            if collection.get("unique"):
                collection_obj = db.get_collection(collection_name)
                collection_obj.create_index([(collection["unique"], 1)], unique=True)
            if collection.get("index"):
                collection_obj = db.get_collection(collection_name)
                collection_obj.create_index([(collection["index"], pymongo.ASCENDING)], unique=False)


    client.close()



def backup_mongo_database(csv_collections):
    """
    Ez a funckió mentést csinál egy MongoDB adatbázisról és a backup_folder könyvtárba menti el
    Args:
        csv_collections: a könyvtár és az adatbázis nevét adja át a szótár segítségével
    """
    backup_folder=csv_collections["mongo_backup"]
    db_name=csv_collections["database"]
    
    # amennyiben nem létezik a könyvtár létrehozása
    if not os.path.exists(backup_folder):
        os.makedirs(backup_folder)

    # Az adatbázis mentése mongodump segítségével.
    cmd = f'mongodump --gzip --archive={backup_folder}/{db_name}.gz --db={db_name}'
    print(cmd)
    subprocess.run(cmd, shell=True)

    return backup_mongo_database


def restore_mongo_database(csv_collections):
    """
    Ez a funkció visszaállítja a MongoDb adatbázist a backup_folder könyvtárból
    Args:
        csv_collections: a könyvtár és az adatbázis nevét adja át a szótár segítségével
    """
    
    # könyvtár/adatbázis 
    backup_folder=csv_collections["mongo_backup"]
    db_name=csv_collections["database"]
    
    # visszaállítás mongorestore segítségével, validálás mellőzése
    cmd = f'mongorestore --gzip --archive={backup_folder}/{db_name}.gz --drop --bypassDocumentValidation --numInsertionWorkersPerCollection=8'
    print(cmd)
    subprocess.run(cmd, shell=True)

    return restore_mongo_database

def import_csv_to_mongodb(csv_collections, test=False):
# tömörített csv állományok beolvasása
    for collection_name, collection in csv_collections.items():
        if isinstance(collection, dict):
            fields = []
            for field in collection["fields"]:
                convert = collection["convert"].get(field)
                if convert:
                    if convert == int:
                        fields.append(f"{field}.int32()")
                    elif convert == float:
                        fields.append(f"{field}.double()")
                    elif callable(convert):
                        fields.append(f"{field}.date(2006-01-02 15:04:05)")
                else:
                    fields.append(f"{field}.string()")
            fields_str = ",".join(fields)
            files = sorted(glob.glob(collection["filepath"]))
            if test:
                files = files[:1]   
            for file in tqdm(files , desc=collection_name):
            # Előkészítés a shell parancsok végrehajtásához
                cmd1 = ['zcat', file]
                if collection_name == "basestationreport":
                    cmd2 = ['awk', '-F', ',', '-v', 'OFS=,', '{gsub("T", " ", $2);gsub(/"/, "\"\""); if (NR>1) print}']
                elif collection_name == "vessels":
                    cmd2 = ['awk', '-F', ',', '-v', 'OFS=,', '{gsub(/"/, "\"\""); if (NR>1) print}']
                else:
                    cmd2 = ['awk', '-F', ',', '-v', 'OFS=,', '{if (NR>1) print}']
                cmd3 = ['mongoimport',f'--db={csv_collections["database"]}',f'--collection={collection_name}', '--type=csv','--ignoreBlanks','--columnsHaveTypes',f'--fields={fields_str}','--numInsertionWorkers=8','--bypassDocumentValidation']
                # Shell parancsok végrehajtása
                with subprocess.Popen(cmd1, stdout=subprocess.PIPE) as p1:
                    with subprocess.Popen(cmd2, stdin=p1.stdout, stdout=subprocess.PIPE) as p2:
                        with subprocess.Popen(cmd3, stdin=p2.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as p3:
                                    out, err = p3.communicate()
    return import_csv_to_mongodb
