# umas funcao de extract que le e consolida no json
import pandas as pd
import glob, os



def extrair_dados_e_consolidar(pasta: str) -> pd.DataFrame:
    
    arquivos_json = glob.glob(os.path.join(pasta, "*.json"))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list)
    return df_total

# uma funcao que transforma
def calculo_kpi_total_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df_novo = df.copy()
    df_novo["Total"] = df["Quantidade"] * df["Venda"]
    return df_novo

# uma funcao que da load em csv ou parquet
def carrega_dados(df: pd.DataFrame, formato_saida: list):
    for format in formato_saida:
        if format == 'csv':
            df.to_csv('dados.csv', index=False)
        elif format == 'parquet':
            df.to_parquet('dados.parquet', index=False)

def pipeline_calcular_kpi_de_vendas_consolidado(pasta: str, formato_saida: list):
    dataframe = extrair_dados_e_consolidar(pasta)
    dataframe_calculado = calculo_kpi_total_vendas(dataframe)
    formato_saida = ['csv', 'parquet']
    carrega_dados(df=dataframe_calculado, formato_saida=formato_saida)
