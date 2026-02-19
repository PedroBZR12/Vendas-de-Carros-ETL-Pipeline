import pandas as pd
import matplotlib.pyplot as plt
import os

def extract(base_url, anos):
    """
    Etapa de EXTRAÇÃO (Extract) do pipeline ETL.

    Lê múltiplos arquivos CSV hospedados remotamente (links RAW do GitHub)
    e armazena cada um em um DataFrame do pandas.

    Parâmetros
    ----------
    base_url : str
        URL base onde os arquivos CSV anuais estão localizados.
    anos : iterable
        Sequência de anos que serão extraídos.

    Retorno
    -------
    list[pandas.DataFrame]
        Lista contendo um DataFrame para cada ano carregado com sucesso.
    """

    dataframes = []

    for ano in anos:
        try:
            url = f"{base_url}{ano}.csv"
            df = pd.read_csv(url)

            # Adiciona coluna de ano
            df["year"] = ano

            dataframes.append(df)

        except Exception as e:
            print(f"Erro ao extrair {ano}: {e}")

    return dataframes


def transform(dataframes):
    """
    Etapa de TRANSFORMAÇÃO (Transform) do pipeline ETL.

    Concatena todos os DataFrames extraídos em um único dataset
    e remove registros duplicados.

    Parâmetros
    ----------
    dataframes : list[pandas.DataFrame]
        Lista de DataFrames gerados na etapa de extração.

    Retorno
    -------
    pandas.DataFrame
        Dataset unificado e limpo.
    """

    df_final = pd.concat(dataframes, ignore_index=True)

    # Remove linhas duplicadas para garantir consistência dos dados
    df_final.drop_duplicates(inplace=True)

    return df_final


def load(df, path):
    """
    Etapa de CARGA (Load) do pipeline ETL.

    Salva o DataFrame transformado em um arquivo CSV local.

    Parâmetros
    ----------
    df : pandas.DataFrame
        Dataset final após transformação.
    path : str
        Caminho onde o arquivo será salvo.
    """
    os.makedirs("data", exist_ok=True)
    df.to_csv(path, index=False)


def run_pipeline():
    """
    Orquestra o fluxo completo do pipeline ETL.

    Fluxo:
        1. Extração dos dados (Extract)
        2. Transformação e consolidação (Transform)
        3. Salvamento do dataset final (Load)
    """

    base_url = "https://raw.githubusercontent.com/PedroBZR12/us-car-models-data/master/"
    anos = range(1992, 2026)

    data = extract(base_url, anos)
    df_final = transform(data)
    load(df_final, "data/us_car_models_cleaned.csv")
    
    
    modelos_por_ano = df_final.groupby("year").size()

    plt.figure(figsize=(12, 6))
    modelos_por_ano.plot()

    plt.title("Evolução do Número de Modelos de Carros (1992–2025)")
    plt.xlabel("Ano")
    plt.ylabel("Quantidade de Modelos")
    plt.grid(True)

    plt.tight_layout()
    plt.savefig("data/evolucao_modelos.png")
    plt.close()
    
    df_final["decade"] = (df_final["year"] // 10) * 10
    modelos_por_decada = df_final.groupby("decade").size()
    plt.figure(figsize=(10, 5))
    modelos_por_decada.plot(kind="bar")

    plt.title("Modelos por Década")
    plt.xlabel("Década")
    plt.ylabel("Quantidade")
    plt.tight_layout()
    plt.savefig("data/modelos_por_decada.png")
    plt.close()


if __name__ == "__main__":
    run_pipeline()


