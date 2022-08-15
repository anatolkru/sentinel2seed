import sys, os.path, argparse
import psycopg2
from sentinelsat import SentinelAPI, geojson_to_wkt
from datetime import date, datetime, timedelta
import geojson


SENTINEL_DIR = os.environ.get('SENTINEL_DIR')
PG_USER = os.environ.get('PG_USER')
PG_PWD  = os.environ.get('PG_PWD')
PG_DB   = os.environ.get('PG_DB')
PG_HOST = os.environ.get('PG_HOST')

COPERNIC_USER = os.environ.get('SENTINEL_USER')
COPERNIC_PWD = os.environ.get('SENTINEL_PWD')
COPERNIC_SAT = os.environ.get('SENTINEL_SAT')
COPERNIC_LEVEL = os.environ.get('SENTINEL_LEVEL')

def createParser ():
    parser = argparse.ArgumentParser()
    parser.add_argument ( '-i', '--id_comp', type=int, required=True )
    parser.add_argument ( '-d', '--delta', type=int, default=1 )
    parser.add_argument ( '-c', '--cloud', type=int, default=30 )
    return parser
 
if __name__ == '__main__':
    parser = createParser()
    namespace = parser.parse_args(sys.argv[1:])
    id_comp = namespace.id_comp 

delta = timedelta(days=int(namespace.delta))
end=datetime.today()
start=end-delta
geostart = str(start.strftime("%Y%m%d"))
geoend   = str(end.strftime("%Y%m%d"))
api = SentinelAPI(COPERNIC_USER, COPERNIC_PWD, 'https://scihub.copernicus.eu/dhus')

conn = psycopg2.connect(dbname=PG_DB, user=PG_USER,
                        password=PG_PWD, host=PG_HOST)
cursor = conn.cursor()
#  output name company
geosql_name = 'SELECT company_name FROM meteo_company_catalog where company_id={}'.format(id_comp)
cursor.execute(geosql_name)
comp_name = cursor.fetchone()
print (comp_name[0])
# выборка всех плиток для компании
geosql = 'SELECT ST_AsGeoJSON(geom) FROM meteo_fields_catalog where company_id={}'.format(id_comp)
cursor.execute(geosql)
for row in cursor:
    if row[0] != None:
        #
        if not os.path.isdir(SENTINEL_DIR):
            print ('Do you not have directory : '+SENTINEL_DIR)
            quit()
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
            print (products_gdf_sorted.iloc[0]["title"],
            products_gdf_sorted.iloc[0]["beginposition"],
            products_gdf_sorted.iloc[0]["cloudcoverpercentage"])
            api.download_all(products_gdf_sorted.index,SENTINEL_DIR)

cursor.close()
conn.close()
