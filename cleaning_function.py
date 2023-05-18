def pipeline(file):
    import pandas as pd
    from sklearn.impute import KNNImputer
    from geopy.geocoders import Nominatim
    from geopy.geocoders import GoogleV3
    from geopy.extra.rate_limiter import RateLimiter
    #read ad set col names
    df =pd.read_csv(file, index_col=None)
    df.columns=['price', 'price_per_meter', 'address', 'area', 'ownership_form', 'num_rooms', 'condition', 'floor', 'balcony_garden_terrace', 'maintenance_fee', 'parking_space', 'heating', 'description', 'market', 'advertiser', 'availability', 'construction_year', 'building_type', 'windows', 'elevator', 'utilities', 'security', 'equipment', 'additional_info', 'material', 'offer_number']

    df.drop_duplicates(subset=['price', 'price_per_meter', 'address', 'area', 'ownership_form', 'num_rooms', 'condition', 'floor', 'balcony_garden_terrace', 'maintenance_fee', 'parking_space', 'heating', 'description', 'market', 'advertiser', 'availability', 'construction_year', 'building_type', 'windows', 'elevator', 'utilities', 'security', 'equipment', 'additional_info', 'material', 'offer_number'], keep='first', inplace=True)

    #cleansing data from scraping
    mask=df[ df['price'] == 'Zapytaj o cenę' ].index
    df.drop(mask,inplace=True)
    mask=df[ df['address'].str.contains('Ta nieruchomość jest częścią inwestycji') ].index
    df.drop(mask,inplace=True)
    mask=df[ df['price'].str.contains('EUR') ].index
    df.drop(mask,inplace=True)
    mask=df[ df['price'].str.contains('USD') ].index
    df.drop(mask,inplace=True)
    mask=df[ df['floor'].str.contains('Zapytaj') ].index
    df.drop(mask,inplace=True)
    mask=df[ df['floor'].str.contains('wolnostojący') ].index
    df.drop(mask,inplace=True)
    df[['floor_apartment','total_floors']]= df.floor.str.split("/",expand=True)
    df['floor_apartment']=df['floor_apartment'].str.replace("Parter",'0')
    df['floor_apartment']=df['floor_apartment'].str.replace("parter",'0')
    df['floor_apartment']=df['floor_apartment'].str.replace("poddasze",'-2')
    df['floor_apartment']=df['floor_apartment'].str.replace("Poddasze",'-2')
    df['floor_apartment']=df['floor_apartment'].str.replace("suterena",'-1')
    df['floor_apartment']=df['floor_apartment'].str.replace("Suterena",'-1')
    df['price']=df['price'].str.replace(" ",'')
    df['price']=df['price'].str.replace("zł",'')
    df['price']=df['price'].str.replace(",",'.')
    df['price_per_meter']=df['price_per_meter'].str.replace(" zł/m²",'')
    df['price_per_meter']=df['price_per_meter'].str.replace("zł/м²",'')
    df['price_per_meter']=df['price_per_meter'].str.replace(" ",'')
    df['area']=df['area'].str.replace(" m²",'')
    df['area']=df['area'].str.replace(" ",'')
    df['maintenance_fee']=df['maintenance_fee'].str.replace(" zł",'')
    df['maintenance_fee']=df['maintenance_fee'].str.replace(",",'.')
    df['maintenance_fee']=df['maintenance_fee'].str.replace(" ",'')
    df['price_per_meter']=df['price_per_meter'].str.replace(",",'.')
    df['area']=df['area'].str.replace(",",'.')
    df['num_rooms']=df['num_rooms'].str.replace("więcej niż 10",'11')
    df['floor_apartment']=df['floor_apartment'].str.replace("więcej niż 10",'11')
    df['floor_apartment']=df['floor_apartment'].str.replace("> 10",'11')
    df["balcony_garden_terrace"]=df["balcony_garden_terrace"].str.replace(" ","")
    df['utilities']=df['utilities'].str.replace(" ","")
    df["security"]=df["security"].str.replace(" ","")
    df['equipment']=df['equipment'].str.replace(" ","")
    df['additional_info']=df['additional_info'].str.replace(" ","")
    df_mean_mask=df['maintenance_fee']!='Zapytaj'
    df['maintenance_fee1'] = df['maintenance_fee'].apply(pd.to_numeric, errors='coerce')
    df_mean=df.maintenance_fee1[df_mean_mask].mean()
    df['maintenance_fee']=df['maintenance_fee'].replace('Zapytaj',df_mean)
    df['ownership_form']=df['ownership_form'].str.replace('Zapytaj','pełna własność')
    df['condition']=df['condition'].str.replace('Zapytaj','do zamieszkania')
    df['heating']=df['heating'].str.replace('Zapytaj','brak informacji')
    df['windows']=df['windows'].str.replace('brak informacji','plastikowe')
    cols = ['price', 'price_per_meter', 'area', 'num_rooms', 'maintenance_fee', 'floor_apartment', 'total_floors']
    df[cols] = df[cols].apply(pd.to_numeric)

    #leave only important columns
    df = df[['price', 'price_per_meter', 'address', 'area', 'ownership_form', 'num_rooms', 'condition', 'balcony_garden_terrace', 'maintenance_fee', 'parking_space', 'heating', 'market', 'advertiser', 'availability', 'construction_year', 'building_type', 'windows', 'elevator', 'utilities', 'security', 'equipment', 'additional_info', 'material', 'floor_apartment', 'total_floors']]

    #there are some columns that has for example ['tv','oven','washing_machine'] in one row and ['oven',furnitures] in next one. we want to separate them into correct collumns
    columns = ['balcony_garden_terrace', 'utilities', 'security', 'equipment', 'additional_info']
    delimiter=','
    for column in columns:
        column_values=df[column].str.split(delimiter).explode()
        a=pd.crosstab(index=column_values.index,columns=column_values.values)
        b=pd.DataFrame(a.values,columns=a.columns.values)
        df=pd.concat([df.reset_index(drop=True),b.reset_index(drop=True)],axis=1)
        del a,b
        df.drop(columns=column,inplace=True)

    #set english names again since utilities and others where listed in different language
    df.columns=['price', 'price_per_meter', 'address', 'area', 'ownership_form', 'num_rooms','condition', 'maintenance_fee', 'parking_space', 'heating', 'market','advertiser', 'availability', 'construction_year', 'building_type','windows', 'elevator', 'material', 'floor_apartment', 'total_floors', 'inquire','balcony', 'garden', 'terrace', 'no_information', 'internet', 'telephone','cable_tv', 'no_information', 'intercom', 'anti_burglary_doors','security_monitoring', 'anti_burglary_rollershutters', 'alarm_system', 'gated_community','no_information', 'stove', 'fridge', 'furniture', 'oven', 'washing_machine','TV', 'dishwasher', 'no_information', 'duplex','separate_kitchen', 'cellar', 'utility_room']
    df = df[['price', 'price_per_meter', 'address', 'area', 'ownership_form', 'num_rooms','condition', 'maintenance_fee', 'parking_space', 'heating', 'market','advertiser', 'availability', 'construction_year', 'building_type','windows', 'elevator', 'material', 'floor_apartment', 'total_floors', 'inquire','balcony', 'garden', 'terrace', 'internet', 'telephone','cable_tv', 'intercom','anti_burglary_doors', 'security_monitoring','anti_burglary_rollershutters', 'alarm_system', 'gated_community', 'stove', 'fridge', 'furniture', 'oven', 'washing_machine','TV', 'dishwasher', 'duplex','separate_kitchen', 'cellar', 'utility_room']]

    #turn adresses into coordinates
    API_key='lol' #google maps API key
    geolocator_free = Nominatim(domain='localhost:8080', scheme='http') #private nominatim instance
    geolocator_ggl = GoogleV3(api_key=API_key)
    geocode_free = RateLimiter(geolocator_free.geocode, min_delay_seconds=0.2)
    latlong=[]
    for index in df.index:
        try:
            a=geocode_free(df.address[index])
            latlong.append((index,a.latitude,a.longitude))
        except:
            try:
                location=geolocator_ggl.geocode(df.address[index])
                latlong.append((location.latitude,location.longitude))
                print(latlong[index],"ERROR")
            except:
                df.drop(index=index,inplace=True)
    df_latlong=pd.DataFrame(latlong,columns=['ind','latitude','longitude'])
    df_latlong.index=df_latlong.ind
    df_latlong=df_latlong[['latitude','longitude']]
    df_finale=pd.concat([df,df_latlong],axis=1,ignore_index=False)
    df_finale.drop("ind",axis=1,inplace=True).dropna(inplace=True)

    #fill 'no data' using coordinates and knnimputer, here heating
    maps={'miejskie':0, 'kotłownia':1, 'brak informacji':2, 'gazowe':3, 'inne':4,'elektryczne':5, 'piece kaflowe':6}
    df_finale.heating=df_finale.heating.map(maps)
    imputer = KNNImputer(n_neighbors=1, weights="uniform",missing_values=2)
    df_finale[['heating','latitude','longitude']]=pd.DataFrame(imputer.fit_transform(df_finale[['heating','latitude','longitude']]),columns=['heating','latitude','longitude'],index=df_finale.index)

    #here we imput building year in very ugly way
    maps={'2008': 2008,'2010': 2010,'2023': 2023,'brak informacji': 0,'2022': 2022,'1970': 1970,'2018': 2018,'2021': 2021,'1927': 1927,'2020': 2020,'2004': 2004,'1978': 1978,'2012': 2012,'2013': 2013,'1990': 1990,'1980': 1980,'1986': 1986,'2009': 2009,'2007': 2007,'1976': 1976,'1992': 1992,'2011': 2011,'1964': 1964,'2002': 2002,'1984': 1984,'1971': 1971,'2000': 2000,'1993': 1993,'1938': 1938,'2015': 2015,'2017': 2017,'2016': 2016,'1983': 1983,'1945': 1945,'1931': 1931,'2019': 2019,'1981': 1981,'1952': 1952,'2005': 2005,'2024': 2024,'1975': 1975,'1910': 1910,'1985': 1985,'1999': 1999,'1950': 1950,'1982': 1982,'1901': 1901,'1913': 1913,'2014': 2014,'1979': 1979,'1974': 1974,'1956': 1956,'2006': 2006,'2003': 2003,'1920': 1920,'1972': 1972,'1930': 1930,'2025': 2025,'1965': 1965,'1904': 1904,'1955': 1955,'1963': 1963,'1989': 1989,'2001': 2001,'1995': 1995,'1968': 1968,'1960': 1960,'1906': 1906,'1958': 1958,'1998': 1998,'1997': 1997,'1954': 1954,'1957': 1957,'1898': 1898,'1907': 1907,'1939': 1939,'1905': 1905,'1973': 1973,'1977': 1977,'1935': 1935,'1900': 1900,'1994': 1994,'1996': 1996,'1991': 1991,'1926': 1926,'1928': 1928,'1943': 1943,'1934': 1934,'1903': 1903,'1891': 1891,'1962': 1962,'1959': 1959,'1961': 1961,'1908': 1908,'1967':1967,'1932': 1932,'1988': 1988,'1987': 1987,'1951': 1951,'1936': 1936,'1921': 1921,'1953': 1953,'1922': 1922,'1915': 1915,'1918': 1918,'1940': 1940,'1893': 1893,'1966': 1966,'1890': 1890,'1830': 1830,'1914': 1914,'1969': 1969,'1925': 1925,'1833': 1833,'1929': 1929,'1933': 1933,'1949': 1949,'1946': 1946,'1912': 1912,'1895': 1895,'1875': 1875,'1880': 1880,'1909': 1909,'1937': 1937,'1911': 1911,'1899': 1899,'1': 0,'1902': 1902,'203': 2003,'1881': 1881,'1916': 1916,'1941': 1941,'1896': 1896,'1897': 1897,'1924': 1924,'1500': 1500,'19': 0,'2121': 2121,'1882': 1882,'1923': 1923,'1919': 1919,'1947': 1947,'60': 1960,'1510': 1510,'1942': 1942,'70': 1970,'1878': 1878,'1860': 1860,'2922': 2922,'85': 1985,'1948': 1948,'1917': 1917,'1865': 1865,'1854': 1854,'1885': 1885,'80': 1980,'1894': 1894,'1887': 1887,'1870': 1870,'210': 2010,'1861': 1861,'1850': 1850,'1944': 1944,'1876': 1876,'91': 1991,'1664': 1664,'1873': 1873,'1886': 1886,'1889': 1889,'1700': 1700,'20': 0,'65': 1965,'1872': 1872,'90': 1990,'1780': 1780,'4': 0,'198': 1980,'1800': 1800,'1805': 1805,'1776': 1776,'1820': 1820,'1842': 1842,'1892': 1892,'1868': 1868,'20007': 2007,'1075': 1075,'19890': 1989,'1883': 1883,'1812': 1812,'1000': 0,'16': 2016,'1888': 1888,'73':1973,'111930': 1930,'1580': 1580,'1750': 1750,'1350': 1350,'670': 1970,'1730': 1730,'200': 2000,'3': 0,'20177': 2017,'1843': 1843,'1863': 1863,'1859': 1859,'1828': 1828,'1841': 1841,'2200': 2200}
    df_finale.construction_year=df_finale.construction_year.map(maps)
    imputer = KNNImputer(n_neighbors=5, weights="uniform",missing_values=0)
    df_finale[['construction_year','latitude','longitude']]=pd.DataFrame(imputer.fit_transform(df_finale[['construction_year','latitude','longitude']]),columns=['construction_year','latitude','longitude'],index=df_finale.index)

    #here building type
    maps={'apartamentowiec':1, 'blok':2, 'kamienica':3, 'brak informacji':4,'szeregowiec':5, 'dom wolnostojący':6, 'loft':7, 'plomba':8}
    df_finale.building_type=df_finale.building_type.map(maps)
    imputer = KNNImputer(n_neighbors=1, weights="uniform",missing_values=4)
    df_finale[['building_type','latitude','longitude']]=pd.DataFrame(imputer.fit_transform(df_finale[['building_type','latitude','longitude']]),columns=['building_type','latitude','longitude'],index=df_finale.index)

    #and material used
    maps={'cegła':1, 'żelbet':2, 'silikat':3, 'brak informacji':4, 'pustak':5,'beton komórkowy':6, 'wielka płyta':7, 'inne':4, 'beton':8, 'keramzyt':9,'drewno':10}
    df_finale.material=df_finale.material.map(maps)
    imputer = KNNImputer(n_neighbors=1, weights="uniform",missing_values=4)
    df_finale[['material','latitude','longitude']]=pd.DataFrame(imputer.fit_transform(df_finale[['material','latitude','longitude']]),columns=['material','latitude','longitude'],index=df_finale.index)

    #do one hot encoding
    df_finale = pd.get_dummies(df_finale, columns=['ownership_form', 'condition', 'parking_space', 'heating', 'market', 'advertiser', 'building_type', 'windows', 'elevator', 'material'])
    df_finale.columns=['price', 'price_per_meter', 'address', 'area', 'number_of_rooms', 'rent', 'availability','year_of_construction', 'floor', 'number_of_floors', 'inquire', 'balcony','garden', 'terrace', 'internet', 'phone', 'cable_tv','intercom', 'security_door_windows', 'security_monitoring','anti-burglary_blinds', 'alarm_system', 'closed_terrace', 'stove','refrigerator', 'furniture', 'oven', 'washing_machine', 'tv', 'dishwasher','duplex', 'separate_kitchen', 'cellar', 'utility_room', 'individual','latitude', 'longitude', 'ownership_form_full_ownership','ownership_form_cooperative_with_land_and_mortgage_register','ownership_form_cooperative_ownership','ownership_form_share', 'condition_to_renovate', 'condition_to_finish','condition_ready_to_move_in', 'parking_space_inquire','parking_space_garage_parking_space', 'heating_district_heating','heating_boiler_room', 'heating_gas', 'heating_other', 'heating_electric','heating_tiled_stove', 'market_primary', 'market_secondary','advertiser_real_estate_agency', 'advertiser_developer','advertiser_private', 'building_type_apartment_building','building_type_block', 'building_type_detached_house','building_type_townhouse', 'building_type_loft','building_type_plomba', 'building_type_row_house','windows_aluminium', 'windows_wood', 'windows_plastic', 'elevator_no','elevator_yes', 'material_concrete', 'material_aerated_concrete','material_brick', 'material_wood', 'material_ceramsite','material_silicate', 'material_large_panel', 'material_reinforced_concrete']
    df_finale = df_finale[['price', 'price_per_meter', 'address', 'area', 'number_of_rooms', 'rent', 'availability','year_of_construction', 'floor', 'number_of_floors', 'balcony','garden', 'terrace', 'internet', 'phone', 'cable_tv','intercom', 'security_door_windows', 'security_monitoring','anti-burglary_blinds', 'alarm_system', 'closed_terrace', 'stove','refrigerator', 'furniture', 'oven', 'washing_machine', 'tv', 'dishwasher','duplex', 'separate_kitchen', 'cellar', 'utility_room','latitude', 'longitude', 'ownership_form_full_ownership','ownership_form_cooperative_with_land_and_mortgage_register','ownership_form_cooperative_ownership', 'ownership_form_share','condition_to_renovate', 'condition_to_finish', 'condition_ready_to_move_in','parking_space_garage/parking_space', 'heating_district_heating','heating_boiler_room', 'heating_gas', 'heating_other', 'heating_electric','heating_tiled_stove', 'market_primary','advertiser_real_estate_agency', 'advertiser_developer','advertiser_private', 'building_type_apartment_building','building_type_block', 'building_type_detached_house','building_type_townhouse', 'building_type_loft','building_type_plomba', 'building_type_row_house','windows_aluminium', 'windows_wood', 'windows_plastic','elevator_yes', 'material_concrete', 'material_aerated_concrete','material_brick', 'material_wood', 'material_ceramsite','material_silicate', 'material_large_panel', 'material_reinforced_concrete']]
    df_finale.to_csv("finale.csv")
