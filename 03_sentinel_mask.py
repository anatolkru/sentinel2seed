import sys, os, argparse
import zipfile
import xml.dom.minidom as minidom
import re
import psycopg2
import pyproj
from sentinelsat import SentinelAPI, geojson_to_wkt
from datetime import date, datetime, timedelta
import geojson
import rasterio
from rasterio.mask import mask
from pathlib import Path
import numpy

import matplotlib.pyplot as plt
#%matplotlib inline


SENTINEL_DIR = './sentinel2/'
PG_USER = 'pseed'
PG_PWD  = ''
PG_DB   = 'pseed_db'
PG_HOST = 'localhost'

COPERNIC_USER = ''
COPERNIC_PWD = ''
COPERNIC_SAT = 'Sentinel-2'
COPERNIC_LEVEL = 'Level-2A'

from_proj = pyproj.Proj(init='epsg:4326')
to_proj = pyproj.Proj(init='epsg:32636')

def createParser ():
    parser = argparse.ArgumentParser()
    parser.add_argument ( '-i', '--id_comp', type=int, default=0  )
    parser.add_argument ( '-d', '--delta', type=int, default=1 )
    parser.add_argument ( '-c', '--cloud', type=int, default=30 )
    return parser
 
if __name__ == '__main__':
    parser = createParser()
    namespace = parser.parse_args(sys.argv[1:])
    id_comp = namespace.id_comp 

def calc_ndvi(b8,b4):
    '''Calculate NDVI from integer arrays'''
    nir = b8.astype(float)
    red = b4.astype(float)
    ndvi = (nir - red) / (nir + red)
    return ndvi

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
geosql = 'SELECT ST_AsGeoJSON(geom),company_id,field_name,field_id FROM meteo_fields_catalog'
if id_comp != 0:
    geosql = 'SELECT ST_AsGeoJSON(geom),company_id,field_name,field_id FROM meteo_fields_catalog where company_id={}'.format(id_comp)
cursor.execute(geosql)

for row in cursor:
    if row[0] != None:
        comp_id = row[1]
        field_name = row[2]
        field_id = row[3]
        print ("company_id=",comp_id," field_name=",field_name," field_id=",field_id)
        s=geojson.loads(row[0])
        footprint = geojson_to_wkt(s)
        products = api.query(footprint,
                     date = (geostart,geoend),
                     platformname = COPERNIC_SAT,
                     processinglevel = COPERNIC_LEVEL,
                     cloudcoverpercentage = (0, namespace.cloud))
        products_gdf = api.to_geodataframe(products)
        # обработка каждого поля по выбранным компаниям
        if (len(products_gdf))>0:
            products_gdf_sorted = products_gdf.sort_values(['beginposition'], ascending=[True])
            for item in products_gdf_sorted.index:
                #определение имен файлов-архивов по индексу плиток
                x = api.get_product_odata(item)
                if os.path.isdir(SENTINEL_DIR + x['title'] + '.SAFE') :
                    print ('work with directory : ' + SENTINEL_DIR + x['title'] + '.SAFE')
                    # парсим xml - для поиска пути к файлам каналов
                    xml_file=SENTINEL_DIR + x['title'] + '.SAFE' + '/MTD_MSIL2A.xml'
                    doc = minidom.parse(xml_file);
                    date_xml = doc.getElementsByTagName("PRODUCT_START_TIME")
                    for node in date_xml:
                        result = re.match(r'\w{4}-\w{2}-\w{2}',node.childNodes[0].nodeValue)
                        date_ftp = result.group(0)
                    img_files = doc.getElementsByTagName("IMAGE_FILE")
                    for node in img_files:
                        #print (node.childNodes[0].nodeValue)
                        res20_02 = re.search(r'B02_20m', node.childNodes[0].nodeValue)
                        res20_03 = re.search(r'B03_20m', node.childNodes[0].nodeValue)
                        res20_04 = re.search(r'B04_20m', node.childNodes[0].nodeValue)
                        res20_05 = re.search(r'B05_20m', node.childNodes[0].nodeValue)
                        res20_06 = re.search(r'B06_20m', node.childNodes[0].nodeValue)
                        res20_07 = re.search(r'B07_20m', node.childNodes[0].nodeValue)
                        res20_11 = re.search(r'B11_20m', node.childNodes[0].nodeValue)
                        res20_12 = re.search(r'B12_20m', node.childNodes[0].nodeValue)
                        res20_8a = re.search(r'B8A_20m', node.childNodes[0].nodeValue)
                        res20_AOT = re.search(r'AOT_20m', node.childNodes[0].nodeValue)
                        res20_SCL = re.search(r'SCL_20m', node.childNodes[0].nodeValue)
                        res20_TCI = re.search(r'TCI_20m', node.childNodes[0].nodeValue)
                        res20_WVP = re.search(r'WVP_20m', node.childNodes[0].nodeValue)
                        res10_02 = re.search(r'B02_10m', node.childNodes[0].nodeValue)
                        res10_03 = re.search(r'B03_10m', node.childNodes[0].nodeValue)
                        res10_04 = re.search(r'B04_10m', node.childNodes[0].nodeValue)
                        res10_08 = re.search(r'B08_10m', node.childNodes[0].nodeValue)
                        res10_AOT = re.search(r'AOT_10m', node.childNodes[0].nodeValue)
                        res10_TCI = re.search(r'TCI_10m', node.childNodes[0].nodeValue)
                        res10_WVP = re.search(r'WVP_10m', node.childNodes[0].nodeValue)
                        if res20_02 :
                            path20_B02 = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                            res20_02 = None
                        elif res20_03 :
                            path20_B03 = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                            res20_03 = None
                        elif res20_04 :
                            path20_B04 = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                            res20_04 = None
                        elif res20_05 :
                            path20_B05 = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                            res20_05 = None
                        elif res20_06 :
                            path20_B06 = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                            res20_06 = None
                        elif res20_07 :
                            path20_B07 = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                            res20_07 = None
                        elif res20_11 :
                            path20_B11 = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                            res20_11 = None
                        elif res20_12 :
                            path20_B12 = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                            res20_12 = None
                        elif res20_8a :
                            path20_B8A = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                            res20_8a = None
                        elif res20_AOT :
                            path20_AOT = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                            res20_AOT = None
                        elif res20_SCL :
                            path20_SCL = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                            res20_SCL = None
                        elif res20_TCI :
                            path20_TCI = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                            res20_TCI = None
                        elif res20_WVP :
                            path20_WVP = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                            res20_WVP = None
                        elif res10_02 :
                            path10_B02 = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                        elif res10_03 :
                            path10_B03 = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                        elif res10_04 :
                            path10_B04 = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                        elif res10_08 :
                            path10_B08 = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                        elif res10_AOT :
                            path10_AOT = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                        elif res10_TCI :
                            path10_TCI = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                        elif res10_WVP :
                            path10_WVP = SENTINEL_DIR + x['title'] + '.SAFE/' + node.childNodes[0].nodeValue + '.jp2'
                        else :
                            pass

                # проверка, создание каталогов для обрезанных изображений по полям
                # /upload/id_company/id_field/*.jp2 *.tiff
                if not os.path.isdir(SENTINEL_DIR + 'upload/'):
                   print ('create '+ SENTINEL_DIR + 'upload/')
                   os.mkdir(SENTINEL_DIR + 'upload/')
                if not os.path.isdir(SENTINEL_DIR + 'upload/' + str(comp_id)):
                   print ('create '+ SENTINEL_DIR + 'upload/' + str(comp_id))
                   os.mkdir (SENTINEL_DIR + 'upload/' + str(comp_id))
                upload_dir = SENTINEL_DIR + 'upload/' + str(comp_id)  + '/' + str(field_id) + '/'
                if not os.path.isdir(upload_dir):
                   print (upload_dir)
                   os.mkdir (upload_dir)
                coords = []
                for z in s['coordinates'][0][0]:
                    from_x, from_y = z
                    coords.append(pyproj.transform(from_proj, to_proj, from_x, from_y))
                geoms = [{'type': 'Polygon','coordinates':[coords]}]
                with rasterio.open(path10_B02) as src: 
                    out_image, out_transform = mask(src, geoms, crop=True)
                out_meta = src.meta.copy()
                out_meta.update ({"driver": "JP2OpenJPEG",
                                  "height": out_image.shape[1],
                                  "width": out_image.shape[2]  })
                upload_file2 = upload_dir + date_ftp + '_L2A_B02_10m.jp2'
                with rasterio.open(upload_file2, "w", **out_meta) as dest:
                    dest.write(out_image)
                    
                with rasterio.open(path10_B03) as src: 
                    out_image, out_transform = mask(src, geoms, crop=True)
                out_meta = src.meta.copy()
                out_meta.update ({"driver": "JP2OpenJPEG",
                                  "height": out_image.shape[1],
                                  "width": out_image.shape[2]  })
                upload_file3 = upload_dir + date_ftp + '_L2A_B03_10m.jp2'
                with rasterio.open(upload_file3, "w", **out_meta) as dest:
                    dest.write(out_image)
                
                with rasterio.open(path10_B04) as src: 
                    out_image, out_transform = mask(src, geoms, crop=True)
                out_meta = src.meta.copy()
                out_meta.update ({"driver": "JP2OpenJPEG",
                                  "height": out_image.shape[1],
                                  "width": out_image.shape[2]  })
                upload_file4 = upload_dir + date_ftp + '_L2A_B04_10m.jp2'
                with rasterio.open(upload_file4, "w", **out_meta) as dest:
                    dest.write(out_image)
                
                with rasterio.open(path10_B08) as src: 
                    out_image, out_transform = mask(src, geoms, crop=True)
                out_meta = src.meta.copy()
                out_meta.update ({"driver": "JP2OpenJPEG",
                                  "height": out_image.shape[1],
                                  "width": out_image.shape[2]  })
                upload_file8 = upload_dir + date_ftp + '_L2A_B08_10m.jp2'
                with rasterio.open(upload_file8, "w", **out_meta) as dest:
                    dest.write(out_image)
                
                # NDVI
                print (upload_file4,upload_file8,upload_dir + date_ftp + '_ndvi.tiff')
                with rasterio.open(upload_file4) as src:
                    b4 =src.read(1)
                with rasterio.open(upload_file8) as src:
                    b8 =src.read(1)
                # Allow division by zero
                numpy.seterr(divide='ignore', invalid='ignore')
                
                ndvi = calc_ndvi(b8,b4)
                #print (ndvi)
                plt.imshow(ndvi, cmap='RdYlGn')
                plt.colorbar()
                plt.title('NDVI {}'.format(date))
                plt.xlabel('Column #')
                plt.ylabel('Row #')
                #plt.show()
                
                out_meta.update ({"driver": "GTiff",
                  "dtype": rasterio.float32,
                  "height": out_image.shape[1],
                  "width": out_image.shape[2]  })
                print (out_meta)
                with rasterio.open(upload_dir + date_ftp + '_ndvi.tiff', 'w', **out_meta) as dst:
                    dst.write_band(1, ndvi.astype(rasterio.float32))
