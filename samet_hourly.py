import os
import requests
import xarray as xr
import geopandas as gpd
import pandas as pd
import numpy as np
from rasterstats import zonal_stats
from datetime import datetime
from bs4 import BeautifulSoup
import rioxarray
from rasterio import open as rasterio_open
from rasterio.transform import from_bounds
from affine import Affine
import shutil
from sqlalchemy import create_engine  
from sqlalchemy import text  
from dotenv import load_dotenv 
import psycopg2
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()

# URLs e caminhos
url_base = "https://ftp.cptec.inpe.br/modelos/tempo/SAMeT/HOURLY/2024/"
grid_path = f"data/grade_estatistica_wgs.shp"
RASTER_DIR = f"rasters"


user = quote_plus(os.getenv("DB_USER"))
password = quote_plus(os.getenv("DB_PASSWORD"))
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
SCHEMA = os.getenv("SCHEMA")
TABELA = os.getenv("TABELA")

# Montar a URL do banco com encoding seguro
DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{db_name}?client_encoding=utf8"


# Criar diretório para salvar rasters, se não existir
os.makedirs(RASTER_DIR, exist_ok=True)

def obter_ultimo_mes_e_dia(url_base):
    response = requests.get(url_base)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    subdirs = [link.text.strip('/') for link in soup.find_all('a') if link.text.strip('/').isdigit()]
    ultimo_subdir = sorted(subdirs)[-1]
    return ultimo_subdir

def baixar_ultima_imagem():
    ultimo_mes = obter_ultimo_mes_e_dia(url_base)
    url_mes = f"{url_base}{ultimo_mes}/"
    ultimo_dia = obter_ultimo_mes_e_dia(url_mes)
    url_dia = f"{url_mes}{ultimo_dia}/"

    response = requests.get(url_dia)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    arquivos = [link.get('href') for link in soup.find_all('a') if link.get('href').endswith('.nc')]
    ultimo_arquivo = sorted(arquivos)[-1]

    url_arquivo = f"{url_dia}{ultimo_arquivo}"
    caminho_arquivo = os.path.join(RASTER_DIR, ultimo_arquivo)

    if not os.path.exists(caminho_arquivo):
        print(f"Baixando: {url_arquivo}")
        with requests.get(url_arquivo, stream=True) as r:
            r.raise_for_status()
            with open(caminho_arquivo, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    else:
        print(f"Arquivo {ultimo_arquivo} já existe.")

    return caminho_arquivo

def converter_netcdf_para_geotiff(nc_path):
    ds = xr.open_dataset(nc_path)
    if 'tt2m' not in ds:
        raise ValueError("A variável 'tt2m' não foi encontrada no NetCDF.")

    data_array = ds['tt2m']
    if "time" in data_array.dims:
        data_array = data_array.isel(time=0)

    data_array = data_array.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
    if not data_array.rio.crs:
        data_array.rio.write_crs("EPSG:4326", inplace=True)

    bounds = data_array.rio.bounds()
    transform = from_bounds(*bounds, data_array.rio.width, data_array.rio.height)

    geotiff_path = nc_path.replace('.nc', '.tif')
    data_array.rio.to_raster(geotiff_path, transform=transform)
    print(f"Arquivo convertido para GeoTIFF: {geotiff_path}")

    return geotiff_path

def flipud(raster, affine):
    raster = np.flipud(raster)
    affine = Affine(
        affine.a, affine.b, affine.c, affine.d, -1 * affine.e, affine.f + (affine.e * (raster.shape[0] - 1))
    )
    return raster, affine

def verificar_alinhamento_crs(raster_path, grid):
    raster = rioxarray.open_rasterio(raster_path)
    raster_crs = raster.rio.crs
    grid_crs = grid.crs

    if raster_crs != grid_crs:
        print(f"Reprojetando grid de {grid_crs} para {raster_crs}")
        grid = grid.to_crs(raster_crs)

    return grid

def testar_conexao(engine):
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            if result.fetchone()[0] == 1:
                print("Conexão ao banco de dados bem-sucedida!")
    except UnicodeDecodeError as e:
        print(f"Erro de Unicode: {e}")
    except Exception as e:
        print(f"Erro ao conectar: {e}")

def salvar_em_postgresql(df):
    """Salva as colunas mapeadas no PostgreSQL."""
    # Cria a conexão com o banco de dados
    engine = create_engine(DATABASE_URL)

    testar_conexao(engine)


    # Adiciona uma coluna com a data e hora atuais / verificar se mantemos dessa forma já que existe uma latência!!
    df['date'] = datetime.now()
   
    #adiciona o type no banco com valor satmet (avaliar se mantém)
    df['type'] = 'temperatura_samet'

    # Mapeamento entre colunas do DataFrame e as do banco
    MAPEAMENTO_COLUNAS = {
        'indice_gre': 'grade_id',
        'mean': 'value'
    }

    # Renomeia as colunas do DataFrame para corresponder ao banco
    df_renomeado = df.rename(columns=MAPEAMENTO_COLUNAS)

    # Seleciona apenas as colunas que existem no banco
    df_para_inserir = df_renomeado[['grade_id', 'value', 'date', 'type']]


    # Envia os dados para o banco, fazendo append na tabela existente
    df_para_inserir.to_sql(
        name=TABELA,  # Nome da tabela no banco
        con=engine,
        schema=SCHEMA,
        if_exists='append',  # Faz append se a tabela já existir
        index=False  # Não salva o índice do DataFrame como coluna
    )

    print(f"Dados salvos na tabela {TABELA} com sucesso.")

def calcular_estatisticas_zonais(raster_path, grid_path):
    grid = gpd.read_file(grid_path, encoding='utf-8')
    grid = verificar_alinhamento_crs(raster_path, grid)

    with rasterio_open(raster_path) as src:
        affine = src.transform
        array = src.read(1)

    array, affine = flipud(array, affine)

    stats = zonal_stats(
        grid, array, affine=affine, stats=["mean"], nodata=np.NaN, all_touched=True
    )

    df_stats = pd.DataFrame(stats)
    df_result = pd.concat([grid, df_stats], axis=1)

    df_result = df_result.ffill().bfill()

    # Salva apenas 'indice_gre', 'mean', 'date' e 'type' no PostgreSQL
    salvar_em_postgresql(df_result)

def limpar_diretorio(diretorio):
    try:
        shutil.rmtree(diretorio)
        print(f"Diretório {diretorio} removido com sucesso.")
    except Exception as e:
        print(f"Erro ao remover o diretório {diretorio}: {e}")

def main():
    nc_path = baixar_ultima_imagem()
    geotiff_path = converter_netcdf_para_geotiff(nc_path)
    calcular_estatisticas_zonais(geotiff_path, grid_path)
    limpar_diretorio(RASTER_DIR)

if __name__ == "__main__":
    main()
