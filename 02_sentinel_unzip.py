# распаковка загруженных файлов sentinel2

import sys, os.path
import argparse
import zipfile
#import xml.dom.minidom as minidom
#import re
import psycopg2
from sentinelsat import SentinelAPI, geojson_to_wkt
from datetime import date, datetime, timedelta
import geojson

SENTINEL_DIR = './sentinel2/'
PG_USER = 'pseed'
PG_PWD  = ''
PG_DB   = 'pseed_db'
PG_HOST = 'localhost'

COPERNIC_USER = ''
COPERNIC_PWD = ''
COPERNIC_SAT = 'Sentinel-2'
COPERNIC_LEVEL = 'Level-2A'

def createParser ():
    parser = argparse.ArgumentParser()
    parser.add_argument ( '-i', '--id_comp', type=int, default=0 )
    parser.add_argument ( '-d', '--delta', type=int, default=1 )
    parser.add_argument ( '-c', '--cloud', type=int, default=30 )
    return parser
 
if __name__ == '__main__':
    parser = createParser()
    namespace = parser.parse_args(sys.argv[1:])
    id_comp = namespace.id_comp 

# определение временного диапазона для поиска 
delta = timedelta(days=int(namespace.delta))
end=datetime.today()
start=end-delta
geostart = str(start.strftime("%Y%m%d"))
geoend   = str(end.strftime("%Y%m%d"))

api = SentinelAPI(COPERNIC_USER, COPERNIC_PWD, 'https://scihub.copernicus.eu/dhus')
conn = psycopg2.connect(dbname=PG_DB, user=PG_USER,
                        password=PG_PWD, host=PG_HOST)
cursor = conn.cursor()
geosql = 'SELECT ST_AsGeoJSON(geom),company_id,field_name FROM meteo_fields_catalog'
if id_comp != 0:
    geosql = 'SELECT ST_AsGeoJSON(geom),company_id,field_name FROM meteo_fields_catalog where company_id={}'.format(id_comp)
cursor.execute(geosql)

for row in cursor:
    if row[0] != None:
        print ("company_id=",row[1]," field_name=",row[2])
        s=geojson.loads(row[0])
        footprint = geojson_to_wkt(s)
        products = api.query(footprint,
                     date = (geostart,geoend),
                     platformname = COPERNIC_SAT,
                     processinglevel = COPERNIC_LEVEL,
                     cloudcoverpercentage = (0, namespace.cloud))
        products_gdf = api.to_geodataframe(products)
        if (len(products_gdf))>0:
            products_gdf_sorted = products_gdf.sort_values(['beginposition'], ascending=[True])
            for item in products_gdf_sorted.index:
                #определение имен файлов-архивов по индексу плиток
                x = api.get_product_odata(item)
                zip_dir = x['title'] + ".SAFE"
                zip_file = x['title'] + ".zip"
                if os.path.isfile (SENTINEL_DIR + zip_file) and os.path.isdir(SENTINEL_DIR + zip_dir):
                    # есть распакованный каталог и есть zip-файл из списка copernicus
                    print ('directory exist :' + zip_dir)
                if os.path.isfile (SENTINEL_DIR + zip_file) and not os.path.isdir(SENTINEL_DIR + zip_dir):
                    # нет распакованного каталога но есть zip-файл из списка copernicus
                    print ('processing extract :' + zip_file)
                    fzip = zipfile.ZipFile(SENTINEL_DIR + zip_file,'r')
                    fzip.extractall(SENTINEL_DIR)
                    fzip.close()

cursor.close()
conn.close()
