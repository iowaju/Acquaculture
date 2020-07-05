from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
import pandas
from datetime import date
#api = SentinelAPI('jimju', 'product_info = api.get_product_odata(<product_id>)', 'https://scihub.copernicus.eu/dhus')
api = SentinelAPI('juiowa', 'N92qbj0Q', 'https://coda.eumetsat.int')
#https://sentinel.esa.int/web/sentinel/technical-guides/sentinel-3-olci/level-2/products-description
footprint='POLYGON ((-72.714934 -46.596841, -71.386323 -41.468724,-73.806147 -41.179165,-75.046388 -46.397790,-72.714934 -46.596841))'
products = api.query(footprint,
                    date=('20200701', date(2020, 7, 2)),
                      producttype='OL_2_WFR___',
                      platformname='Sentinel-3')
products_df_OL = api.to_dataframe(products)
products_df_OL['keys'] = products_df_OL.index
download_keys=products_df_OL['keys'].tolist()
api.download_all(download_keys, directory_path=path, checksum=True)