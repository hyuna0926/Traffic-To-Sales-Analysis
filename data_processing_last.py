import pandas as pd
import numpy as np
import geopandas as gpd
from multiprocessing import Pool

# 시군구 읍면동 합치기
def sigungu_emd_concat(sig, seoul_map):
    seoul_map.geometry = seoul_map.geometry.set_crs("EPSG:5181")
    seoul_map = seoul_map.to_crs("EPSG:4326")

    sig.geometry = sig.geometry.set_crs("EPSG:5179")
    sig = sig.to_crs("EPSG:4326")
    
    
    seoul_map['seoul_map_CD_prefix'] = seoul_map['ADSTRD_CD'].astype(str).str[:5]
    sig_seoul_map = pd.merge(sig[['SIG_KOR_NM', 'SIG_CD']], seoul_map, left_on = 'SIG_CD', right_on = 'seoul_map_CD_prefix')

    sig_seoul_map['SIG_emd_KOR_NM'] = sig_seoul_map['SIG_KOR_NM'].fillna('') + ' '+ sig_seoul_map['ADSTRD_NM'].fillna('')
    sig_seoul_map.drop(columns=['SIG_KOR_NM', 'XCNTS_VALU','YDNTS_VALU', 'seoul_map_CD_prefix', 'RELM_AR'], inplace=True)

    sig_seoul_map = gpd.GeoDataFrame(sig_seoul_map, crs='EPSG:4326', geometry=sig_seoul_map['geometry'])
    
    return sig_seoul_map


# pcell데이터와 시군구읍면동 데이터에서 포함되는 데이터 결과 뽑기
def data_sigungu(row):
    geom = row['geometry']
    # sigungu_emd 데이터프레임에서 contains 메서드를 사용하여 조건을 검사합니다.
    # 조건을 만족하는 행이 있다면 SIG_KOR_NM 값을 가져와서 결과 리스트에 추가합니다.
    sigungu = sigungu_emd[geom.within(sigungu_emd.geometry)]
    if not sigungu.empty:
        row['SIG_emd_KOR_NM'] = sigungu['SIG_emd_KOR_NM'].iloc[0]
        row['ADSTRD_CD'] = sigungu['ADSTRD_CD'].iloc[0]
    else:
        row['SIG_emd_KOR_NM'] = ""
        row['ADSTRD_CD'] = ""
        
    print(row)    
    return row


def process_data(data_chunk):
    print(data_chunk.head())
    return data_chunk.apply(data_sigungu, axis=1)


def multi_process(data):
    women = data[data['GENDER']=='W']
    men = data[data['GENDER']=='M']

    # 데이터를 청크로 나눕니다.
    chunk_size = 5000
    women_chunks = [women[i:i+chunk_size] for i in range(0, len(women), chunk_size)]
    men_chunks = [men[i:i+chunk_size] for i in range(0, len(men), chunk_size)]

    # 멀티프로세싱 풀을 생성합니다.
    with Pool() as pool:
        # 여성 데이터를 처리합니다.
        women_results = pool.map(process_data, women_chunks)
        # 남성 데이터를 처리합니다.
        men_results = pool.map(process_data, men_chunks)
        
    # 결과를 합칩니다.
    result_w  = pd.concat(women_results)
    result_m = pd.concat(men_results)

    gdf = pd.concat([result_w, result_m], axis=0)

    gdf.to_csv('/home/hdd_data/서울/hyun/people/gdf_20_0423.csv', index=False, encoding='utf-8')
        
    
if __name__ == "__main__":
    # 데이터 불러오기
    sigungu = gpd.read_file('/home/hdd_data/서울/hyun/people/SIG_20221119/sig.shp', encoding='cp949')
    seoul_map = gpd.read_file('/home/hdd_data/서울/hyun/people/서울시 상권분석서비스(영역-행정동)/서울시 상권분석서비스(영역-행정동).shp')

    
    # 시군구 읍면동 데이터 합치기
    sigungu_emd = sigungu_emd_concat(sigungu, seoul_map)
    
    # geopandas로 변경
    df_10 = pd.read_csv('/home/hdd_data/서울/hyun/people/skt2210_20.csv', index_col=0)
    df_11 = pd.read_csv('/home/hdd_data/서울/hyun/people/skt2211_20.csv', index_col=0)
    df_12 = pd.read_csv('/home/hdd_data/서울/hyun/people/skt2212_20.csv', index_col=0)
    df_a = pd.concat([df_10, df_11])
    df = pd.concat([df_a, df_12])
    
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['X_COORD'], df['Y_COORD']), crs='EPSG:5179')
    df_c = gdf.to_crs(epsg=4326)
    
    # 데이터 멀티프로세싱
    multi_process(df_c)
        
    
    
    