# %% [markdown]
# # Lê os arquivos base

# %%
import pandas as pd
pd.options.display.float_format = '{:.2f}'.format
from datetime import timedelta
from pandasql import sqldf
import numpy as np
from csv import Sniffer as sniffer
from chardet import detect

def detect_encoding(csv_file):
    with open(csv_file, 'rb') as file:
        result = detect(file.read())
    return result['encoding']

def detect_delimiter(csv_file):
    with open(csv_file, 'r', encoding=detect_encoding(csv_file)) as file:
        dialect = sniffer().sniff(file.read(2048))
    return dialect.delimiter

def load_csv(csv_file):
    return pd.read_csv(csv_file,delimiter=detect_delimiter(csv_file),decimal=",")

# Carrega bases de dados do sistema
plano_contas_base = load_csv("Plano de contas base.csv")
dre = load_csv("DRE.csv")
dfc = load_csv("DFC.csv")
bp = load_csv("BP.csv")
plano_contas_real_exemplo = load_csv("Plano de contas real.csv")
indicadores = load_csv("Indicadores.csv")

# Carrega bases de dados do usuário
balancetes = load_csv("Balancetes.csv")
plano_contas_real = load_csv("Plano de contas real.csv")

def calcula_base(balancetes, plano_contas_real):
    # %% [markdown]
    # # Tratamento dos dados importados

    # %% [markdown]
    # ## Colunas em maiúsculo

    # %%
    #local_vars = list(locals().keys())

    #for var_name in local_vars:
    #    var = locals()[var_name]
    #    if isinstance(var, pd.DataFrame):
    #        var.columns = var.columns.str.upper()

    # %% [markdown]
    # ## plano_contas_real

    # %%
    # Add coluns "DESCE NÍVEL"
    plano_contas_real["DESCE NÍVEL"] = plano_contas_real["CÓDIGO PLANO DE CONTAS"].notna()

    # Add coluna "DESCRIÇÃO PLANO DE CONTAS"
    plano_contas_real["DESCRIÇÃO PLANO DE CONTAS"] = plano_contas_real.merge(right=plano_contas_base,
                                                                            left_on="CÓDIGO PLANO DE CONTAS",
                                                                            right_on="CÓDIGO PLANO DE CONTAS",
                                                                            how="left")["DESCRIÇÃO"]

    # Add coluna "PLANO DE CONTAS CÓDIGO FINAL"                                                              
    plano_contas_real["CÓDIGO PLANO DE CONTAS FINAL"] = plano_contas_real["CÓDIGO PLANO DE CONTAS"].astype("str") + plano_contas_real["CÓDIGO"].astype("str")
    plano_contas_real.loc[plano_contas_real["DESCE NÍVEL"].astype("bool") == False, "CÓDIGO PLANO DE CONTAS FINAL"] = ""

    # Add coluna "PLANO DE CONTAS CÓDIGO FINAL"
    plano_contas_real["CÓDIGO PLANO DE CONTAS FINAL"] = plano_contas_real["CÓDIGO PLANO DE CONTAS"].astype(str) + "." + plano_contas_real["CÓDIGO"].astype(str)
    plano_contas_real.loc[plano_contas_real["DESCE NÍVEL"].astype(bool)==False, "CÓDIGO PLANO DE CONTAS FINAL"] = np.nan

    # Add coluna "CONTA NEGATIVA"
    plano_contas_real["CONTA NEGATIVA"] = plano_contas_real["DESCRIÇÃO PLANO DE CONTAS"].str.startswith("(-)") & plano_contas_real["DESCRIÇÃO PLANO DE CONTAS"].notna()

    # %% [markdown]
    # ## plano_contas_base

    # %%

    # %% [markdown]
    # ## balancetes

    # %%
    balancetes["MÊS"] = pd.to_datetime(balancetes["MÊS"])

    # %% [markdown]
    # # ETL

    # %% [markdown]
    # ## base_balancetes

    # %%
    # Merge balancetes com plano_contas_real (busca apenas 3 colunas)
    base_balancetes = pd.merge(balancetes,plano_contas_real[["CLASSIFICAÇÃO", "TIPO DE CONTA", "CÓDIGO PLANO DE CONTAS FINAL"]],on="CLASSIFICAÇÃO",how="left")

    # Renomeia a coluna "CÓDIGO PLANO DE CONTAS FINAL" para "CÓDIGO PLANO DE CONTAS"
    base_balancetes.rename(columns={'CÓDIGO PLANO DE CONTAS FINAL': 'CÓDIGO PLANO DE CONTAS'},inplace=True)

    # Add coluna "CONTA NEGATIVA"
    def conta_negativa(row):
        if str(row['CÓDIGO PLANO DE CONTAS']).startswith("1"):
            return row['TIPO'] == 'C'
        else:
            return row['TIPO'] == 'D'
    base_balancetes['CONTA NEGATIVA'] = base_balancetes.apply(conta_negativa, axis=1)

    # Add coluna "MÊS ANTERIOR"
    min_date = min(base_balancetes["MÊS"])
    def mês_anterior(row):
        if row["MÊS"] == min_date:
            return min_date
        else:
            return row["MÊS"].replace(day=1) - timedelta(days=1)
    base_balancetes["MÊS ANTERIOR"] = base_balancetes.apply(mês_anterior, axis=1)

    # Add coluna "TIPO ANTERIOR"
    # Merge base_balancetes com ele mesmo, mas colunas de "MÊS ATUAL" com "MÊS ANTERIOR", para fazer o deslocamento
    base_balancetes = base_balancetes.merge(base_balancetes[["CLASSIFICAÇÃO","MÊS", "TIPO"]],
                                            how="left",
                                            left_on=["CLASSIFICAÇÃO","MÊS ANTERIOR"],
                                            right_on=["CLASSIFICAÇÃO","MÊS"])
    # Renomeia colunas
    base_balancetes.rename(columns={"TIPO_y":"TIPO ANTERIOR",
                            "TIPO_x": "TIPO",
                            "MÊS_x":"MÊS"},inplace=True)

    # Preenche vazios da nova coluna "TIPO ANTERIOR" com valores da coluna "TIPO"
    base_balancetes['TIPO ANTERIOR'].fillna(base_balancetes['TIPO'])

    # Remove colua "MÊS_y" resultante do merge anterior
    base_balancetes.drop(columns=["MÊS_y"],inplace=True)


    # %%
    # Add coluna "CONTA NEGATIVA ANTERIOR"
    def conta_negativa_anterior(row):
        if str(row['CÓDIGO PLANO DE CONTAS']).startswith("1"):
            return row['TIPO ANTERIOR'] == 'C'
        else:
            return row['TIPO ANTERIOR'] == 'D'
    base_balancetes['CONTA NEGATIVA ANTERIOR'] = base_balancetes.apply(conta_negativa_anterior, axis=1)

    # Filtra por TIPO DE CONTA null
    base_balancetes = base_balancetes[base_balancetes["TIPO DE CONTA"].isna()]

    # Substitui valores na coluna "SALDO ANTERIOR"
    def saldo_anterior_novo(row):
        if row["CONTA NEGATIVA ANTERIOR"] == True:
            return row["SALDO ANTERIOR"] *-1
        else:
            return row["SALDO ANTERIOR"]
    base_balancetes["SALDO ANTERIOR"] = base_balancetes.apply(saldo_anterior_novo, axis=1)

    # Substitui valores na coluna "SALDO ATUAL"
    def saldo_atual_novo(row):
        if row["CONTA NEGATIVA"] == True:
            return row["SALDO ATUAL"] *-1
        else:
            return row["SALDO ATUAL"]
    base_balancetes["SALDO ATUAL"] = base_balancetes.apply(saldo_atual_novo, axis=1)

    # Remove colunas desnecessárias
    base_balancetes.drop(columns=["MÊS ANTERIOR"],inplace=True)

    # Reorganizando as colunas
    base_balancetes = base_balancetes[["CÓDIGO", "CLASSIFICAÇÃO", "DESCRIÇÃO", "SALDO ANTERIOR", 
                            "DÉBITO", "CRÉDITO", "SALDO ATUAL", "TIPO", "TIPO ANTERIOR", 
                            "CONTA NEGATIVA","CONTA NEGATIVA ANTERIOR", "MÊS",
                            "TIPO DE CONTA", "CÓDIGO PLANO DE CONTAS"]]

    # Reset index
    base_balancetes.reset_index(drop=True,inplace=True)

    # Classifica os dados
    base_balancetes.sort_values(by=["CLASSIFICAÇÃO", "MÊS"],ascending=[True,True])


    # %% [markdown]
    # ## contas_ajuste

    # %%
    # Baseado na base_balancetes, porém com um filtro na coluna CÓDIGO PLANO DE CONTAS
    contas_ajuste = base_balancetes[base_balancetes['CÓDIGO PLANO DE CONTAS'].str.startswith(("1.02.03.03", "1.01.01"))].copy()

    # Ajusta coluna "CÓDIGO PLANO DE CONTAS"
    condition = contas_ajuste["CÓDIGO PLANO DE CONTAS"].str.startswith("1.01.01")
    contas_ajuste.loc[condition, "CÓDIGO PLANO DE CONTAS"] = "9.02." + contas_ajuste["CÓDIGO"].astype(str)
    contas_ajuste.loc[~condition, "CÓDIGO PLANO DE CONTAS"] = "9.01." + contas_ajuste["CÓDIGO"].astype(str)

    # Inverte colunas de "CRÉDITO" e "DÉBITO"
    contas_ajuste.rename(columns={"CRÉDITO": "DÉBITO",
                                "DÉBITO":"CRÉDITO"},inplace=True)

    # Remove os negativos do "SALDO ANTERIOR"
    contas_ajuste["SALDO ANTERIOR"] = abs(contas_ajuste["SALDO ANTERIOR"])

    # Remove os negativos do "SALDO ATUAL"
    contas_ajuste["SALDO ATUAL"] = abs(contas_ajuste["SALDO ATUAL"])

    # Add coluna "DÉBITO 9.01"
    contas_ajuste["DÉBITO 9.01"] = np.nan
    condition = contas_ajuste["CÓDIGO PLANO DE CONTAS"].str.startswith("9.01")
    contas_ajuste.loc[condition, "DÉBITO 9.01"] = contas_ajuste["CRÉDITO"]
    contas_ajuste.loc[~condition, "DÉBITO 9.01"] = contas_ajuste["DÉBITO"]

    # Add coluna "CRÉDITO 9.01"
    contas_ajuste["CRÉDITO 9.01"] = np.nan
    condition = contas_ajuste["CÓDIGO PLANO DE CONTAS"].str.startswith("9.01")
    contas_ajuste.loc[condition, "CRÉDITO 9.01"] = contas_ajuste["DÉBITO"]
    contas_ajuste.loc[~condition, "CRÉDITO 9.01"] = contas_ajuste["CRÉDITO"]

    # Remove colunas de CRÉDITO e DÉBITO
    contas_ajuste.drop(columns=["CRÉDITO", "DÉBITO"],inplace=True)

    # Renomeia colunas de "CRÉDITO 9.01" e "DÉBITO 9.01"
    contas_ajuste.rename(columns={"DÉBITO 9.01":"DÉBITO",
                                "CRÉDITO 9.01":"CRÉDITO"},inplace=True)

    # Reorganizando as colunas
    contas_ajuste = contas_ajuste[["CÓDIGO", "CLASSIFICAÇÃO", "DESCRIÇÃO", "SALDO ANTERIOR", 
                                "DÉBITO", "CRÉDITO", "SALDO ATUAL", "TIPO", "TIPO ANTERIOR", "CONTA NEGATIVA", 
                                "CONTA NEGATIVA ANTERIOR", "MÊS", "TIPO DE CONTA", "CÓDIGO PLANO DE CONTAS"]]

    # Reset index
    contas_ajuste.reset_index(drop=True,inplace=True)


    # %% [markdown]
    # ## balancetes_ajustados

    # %%
    balancetes_ajustados = pd.concat([base_balancetes, contas_ajuste], ignore_index=True)

    # %% [markdown]
    # ## meses

    # %%
    meses = balancetes_ajustados["MÊS"].copy().drop_duplicates().reset_index(drop=True).sort_values().to_frame()


    # %% [markdown]
    # ## contas_balancete

    # %%
    contas_balancete = balancetes_ajustados[["CÓDIGO PLANO DE CONTAS", "DESCRIÇÃO"]].copy().drop_duplicates().reset_index(drop=True)

    # Capitaliza cada palavra na coluna DESCRIÇÃO
    contas_balancete["DESCRIÇÃO"] = contas_balancete["DESCRIÇÃO"].str.title()

    # Adiciona coluna GRAU
    contas_balancete["GRAU"] = contas_balancete['CÓDIGO PLANO DE CONTAS'].str.count("\.") + 1


    # %% [markdown]
    # ## base_final

    # %%
    # Une plano_contas_base e contas_balancete uma emcima da outra
    base_final = pd.concat([plano_contas_base[["CÓDIGO PLANO DE CONTAS","DESCRIÇÃO","GRAU"]].drop_duplicates(), contas_balancete])

    # Merge com a tabela de meses para cada linha
    base_final = pd.merge(base_final, meses, how='cross')

    # Merge com  BalancetesAjustados
    base_final = pd.merge(base_final, 
                        balancetes_ajustados[["SALDO ANTERIOR", "DÉBITO", "CRÉDITO", "SALDO ATUAL","CÓDIGO PLANO DE CONTAS", 'MÊS']], 
                        how='left', 
                        on=['CÓDIGO PLANO DE CONTAS', 'MÊS'])

    # Adiciona coluna INCLUIR
    base_final["INCLUIR"] = base_final["SALDO ANTERIOR"].isna()

    # Classifica os dados
    base_final.sort_values(by=["MÊS",'CÓDIGO PLANO DE CONTAS'],inplace=True)

    # Tranforma datetime em date
    base_final["MÊS"] = pd.to_datetime(base_final["MÊS"]).dt.date

    # Arredonda os números
    base_final["SALDO ANTERIOR"] = base_final["SALDO ANTERIOR"].round(2)
    base_final["SALDO ATUAL"] = base_final["SALDO ATUAL"].round(2)
    base_final["CRÉDITO"] = base_final["CRÉDITO"].round(2)
    base_final["DÉBITO"] = base_final["DÉBITO"].round(2)

    # Reset index
    base_final.reset_index(drop=True,inplace=True)

    query = """
        SELECT 
            t1.'CÓDIGO PLANO DE CONTAS',
            t1.'DESCRIÇÃO',
            t1.'GRAU',
            t1.'MÊS',
            SUM(t2.'SALDO ANTERIOR') AS 'SALDO ANTERIOR',
            SUM(t2.'DÉBITO') AS 'DÉBITO',
            SUM(t2.'CRÉDITO') AS 'CRÉDITO',
            SUM(t2.'SALDO ATUAL') AS 'SALDO ATUAL'
        FROM base_final as t1
        LEFT JOIN base_final as t2
        ON t1.'MÊS' = T2.'MÊS'
            AND t2.'CÓDIGO PLANO DE CONTAS' LIKE t1.'CÓDIGO PLANO DE CONTAS' || '%'
        GROUP BY 
            t1.'CÓDIGO PLANO DE CONTAS',
            t1.'DESCRIÇÃO',
            t1.'GRAU',
            t1.'MÊS'
        ORDER BY
            t1.'MÊS',
            t1.'CÓDIGO PLANO DE CONTAS'
    """
    base_final = sqldf(query,locals())

    base_final["MÊS"] = pd.to_datetime(base_final["MÊS"])

    base_final.rename(columns={"CÓDIGO PLANO DE CONTAS":"CÓDIGO"},inplace=True)

    # %%
    indicadores = load_csv("Indicadores.csv")
    indicadores_dict = dict(zip(indicadores["DESCRIÇÃO"], indicadores["DESCRIÇÃO"]
                                .str.replace(" - ","_")
                                .str.replace(" ","_")
                                .str.replace("(","")
                                .str.replace(")","")
                                .str.replace("/","_")
                                .str.replace("%","Perc")
                                .str.replace("-","")))

    indicadores = pd.merge(left=indicadores,
                        right=meses,
                        how="cross")

    # %% [markdown]
    # ## Cálculo dos indicadores

    # %%
    busca_valor_dict = {}

    def busca_valor(cod,row,col="SALDO ATUAL"):
        if (cod, row['MÊS'], col) in busca_valor_dict:
            return busca_valor_dict[(cod, row['MÊS'], col)]
        else:
            # Otherwise, calculate the sum and store it in the dictionary
            base_final_subset = base_final[base_final['MÊS'] == row['MÊS']].fillna(0)
            valor = base_final_subset.loc[base_final_subset['CÓDIGO'] == cod, col].sum()
            busca_valor_dict[(cod, row['MÊS'], col)] = valor
            return valor

    def Ativo_Circulante_Operacional(row):
        return (busca_valor("1.01",row) - busca_valor("1.01.01",row))
    def Passivo_Circulante_Operacional(row):
        return (busca_valor("2.01",row) - busca_valor("2.01.01.02",row) - busca_valor("2.01.01.08",row))
    def Necessidade_de_Capital_de_Giro(row):
        return (Ativo_Circulante_Operacional(row) - Passivo_Circulante_Operacional(row))
    def Recursos_Financeiros_de_Curto_Prazo(row):
        return (busca_valor("2.01.01.02",row) + busca_valor("2.01.01.08",row) - busca_valor("1.01.01",row))
    def Capital_de_Giro_Próprio(row):
        return (busca_valor("3",row) - busca_valor("1.02.02",row) - busca_valor("1.02.03",row) - busca_valor("1.02.04",row) - busca_valor("1.02.05",row))
    def Recursos_de_Longo_Prazo(row):
        return (busca_valor("2.02",row) - busca_valor("1.02.01",row))
    def Financiamento_Geral(row):
        return (
            Recursos_Financeiros_de_Curto_Prazo(row) - 
            Capital_de_Giro_Próprio(row) +
            Recursos_de_Longo_Prazo(row)
        )
    def Faturamento_Dia(row):
        return (busca_valor("3.04.01.03.04.01",row,"CRÉDITO") - busca_valor("3.04.01.03.04.01",row,"DÉBITO")) / int(pd.to_datetime(row['MÊS']).day)
    def Necessidade_de_Capital_de_Giro_em_Dias_de_Venda(row):
        return (
            Necessidade_de_Capital_de_Giro(row) / 
            Faturamento_Dia(row)
        )
    def Prazo_Médio_de_Pagamento_PMP(row):
        return (
            (busca_valor("2.01.01.01",row) - busca_valor("2.02.01.02",row)) /
            abs(busca_valor("3.04.01.03.04.03",row,"CRÉDITO") - busca_valor("3.04.01.03.04.03",row,"DÉBITO")) * 
            int(pd.to_datetime(row['MÊS']).day)
        )
    def Prazo_Médio_de_Recebimento_PMR(row):
        return (
            (busca_valor("1.01.02",row) - busca_valor("1.02.01.02",row)) /
            abs(busca_valor("3.04.01.03.04.01",row,"CRÉDITO") - busca_valor("3.04.01.03.04.01",row,"DÉBITO")) * 
            int(pd.to_datetime(row['MÊS']).day)
        )
    def Prazo_Médio_de_Rotação_dos_Estoques_PMRE(row):
        return (
            (busca_valor("1.01.03",row) /
            abs(busca_valor("3.04.01.03.04.03",row,"CRÉDITO") - busca_valor("3.04.01.03.04.03",row,"DÉBITO")) * int(pd.to_datetime(row['MÊS']).day)) *
            int(pd.to_datetime(row['MÊS']).day)
        )
    def Ciclo_Operacional(row):
        return (
            Prazo_Médio_de_Recebimento_PMR(row) +
            Prazo_Médio_de_Rotação_dos_Estoques_PMRE(row)
        )
    def Ciclo_Financeiro(row):
        return (
            Ciclo_Operacional(row) -
            Prazo_Médio_de_Pagamento_PMP(row)
        )
    def EBIT(row):
        return (
            (busca_valor("3.04.01.03.01",row,"CRÉDITO") - busca_valor("3.04.01.03.01",row,"DÉBITO")) +
            (busca_valor("3.04.01.03.02",row,"CRÉDITO") - busca_valor("3.04.01.03.02",row,"DÉBITO")) +
            (busca_valor("3.04.01.03.03",row,"CRÉDITO") - busca_valor("3.04.01.03.03",row,"DÉBITO")) +
            (busca_valor("3.04.01.03.04",row,"CRÉDITO") - busca_valor("3.04.01.03.04",row,"DÉBITO")) +
            (busca_valor("3.04.01.03.05",row,"CRÉDITO") - busca_valor("3.04.01.03.05",row,"DÉBITO"))
        )
    def NOPAT_Resultado_Operacional(row):
            if EBIT(row) < 0:
                return 1
            else:
                return (EBIT(row) * (1-0.34))
    def Margem_Operacional_Sobre_Vendas(row):
        return (
            NOPAT_Resultado_Operacional(row) /
            (busca_valor("3.04.01.03.04.01",row,"CRÉDITO") - busca_valor("3.04.01.03.04.01",row,"DÉBITO"))
        )
    def Margem_Líquida_Sobre_Vendas(row):
        return (
            (busca_valor("3.04",row,"CRÉDITO") - busca_valor("3.04",row,"DÉBITO")) /
            (busca_valor("3.04.01.03.04.01",row,"CRÉDITO") - busca_valor("3.04.01.03.04.01",row,"DÉBITO"))
        )
    def Investimentos_Líquidos(row):
        return (busca_valor("1",row) - busca_valor("2.01",row) - busca_valor("2.01.01.02",row) - busca_valor("2.01.01.08",row))
    def ROI_Retorno_do_Investimento(row):
        return (
            NOPAT_Resultado_Operacional(row) /
            Investimentos_Líquidos(row)
        )
    def ROA_Retorno_Sobre_o_Ativo(row):
        return(
            NOPAT_Resultado_Operacional(row) /
            Investimentos_Totais(row)
        )
    def ROE_Retorno_Sobre_o_Patrimônio(row):
        return (
            (busca_valor("3.04",row,"CRÉDITO") - busca_valor("3.04",row,"DÉBITO")) /
            Capital_Próprios(row)
        )
    def GAFGrau_de_Alavancagem_Financeira(row):
        return(
            ROE_Retorno_Sobre_o_Patrimônio(row) /
            ROI_Retorno_do_Investimento(row)
        )
    def Alavancagem_Financeira(row):
        return(
            Capital_de_Terceiros(row) /
            Capital_Próprios(row)
        )
    def Giro_do_Ativo(row):
        return(
            (busca_valor("3.04.01.03.04.01",row,"CRÉDITO") - busca_valor("3.04.01.03.04.01",row,"DÉBITO")) /
            Investimentos_Líquidos(row)
        )
    def EBITDA(row):
        return(
            EBIT(row) - (busca_valor("3.04.01.03.01.02",row,"CRÉDITO") - busca_valor("3.04.01.03.01.02",row,"DÉBITO"))
        )
    def Margem_EBITDA(row):
        return(
            EBITDA(row) /
            (busca_valor("3.04.01.03.04.01",row,"CRÉDITO") - busca_valor("3.04.01.03.04.01",row,"DÉBITO"))
        )
    def EVA(row):
        return (
            (busca_valor("3.04",row,"CRÉDITO") - busca_valor("3.04",row,"DÉBITO")) -
            (WACC(row) * Investimentos_Líquidos(row))
        )
    def Receita_Bruta(row):
        return (
            (busca_valor("3.04.01.03.04.01",row,"CRÉDITO") - busca_valor("3.04.01.03.04.01",row,"DÉBITO"))
        )
    def CPV_CMV_CSP(row):
        return (
            (busca_valor("3.04.01.03.04.03",row,"CRÉDITO") - busca_valor("3.04.01.03.04.03",row,"DÉBITO"))
        )
    def CPV_CMV_CSP_Perc_da_Receita_Bruta(row):
        return(
            (busca_valor("3.04.01.03.04.03",row,"CRÉDITO") - busca_valor("3.04.01.03.04.03",row,"DÉBITO")) /
            (busca_valor("3.04.01.03.04.01",row,"CRÉDITO") - busca_valor("3.04.01.03.04.01",row,"DÉBITO"))
        )
    def Despesa_Financeira(row):
        return(
            (busca_valor("3.04.01.01.01",row,"CRÉDITO") - busca_valor("3.04.01.01.01",row,"DÉBITO"))
        )
    def Despesa_Financeira_Perc_da_Receita_Bruta(row):
        return(
            (busca_valor("3.04.01.01.01",row,"CRÉDITO") - busca_valor("3.04.01.01.01",row,"DÉBITO")) /
            (busca_valor("3.04.01.03.04.01",row,"CRÉDITO") - busca_valor("3.04.01.03.04.01",row,"DÉBITO"))
        )
    def Spread(row):
        return (
            ROI_Retorno_do_Investimento(row) -
            WACC(row)
        )
    def Margem_de_Contribuição(row):
        return(
            (busca_valor("3.04.01.03.04",row,"CRÉDITO") - busca_valor("3.04.01.03.04",row,"DÉBITO")) /
            (busca_valor("3.04.01.03.04.01",row,"CRÉDITO") - busca_valor("3.04.01.03.04.01",row,"DÉBITO"))
        )
    def Investimentos_Totais(row):
        return (busca_valor("1",row))
    def Passivo_Não_Oneroso(row):
        return (busca_valor("2.01",row) - busca_valor("2.01.01.02",row) - busca_valor("2.01.01.08",row))
    def Investimentos_Líquidos(row):
        return (Investimentos_Totais(row) - Passivo_Não_Oneroso(row))
    def Capital_Próprios(row):
        return (busca_valor("3",row))
    def Capital_de_Terceiros(row):
        return (Investimentos_Líquidos(row) - Capital_Próprios(row))
    def Participação_do_Capital_Próprio_Ke(row):
        return(Capital_Próprios(row) / Investimentos_Líquidos(row))
    def Participação_do_Capital_de_Terceiros_Ki(row):
        return (Capital_de_Terceiros(row) / Investimentos_Líquidos(row))
    def Custo_do_Capital_Próprio_Ke(row):
        return(
            0
            #(
            #    RF_CDI(row) + 
            #    (((RM_SELIC(row) - RF_CDI(row))) * Beta(row)) + 
            #    RP_Risco_País(row)/100/100
            #)
        )
    def Custo_do_Capital_de_Terceiros_Ki(row):
        return(
            (busca_valor("3.04.01.01.01",row,"CRÉDITO") - busca_valor("3.04.01.01.01",row,"DÉBITO")) * (1-0.34) /
            Capital_de_Terceiros(row)
        )
    def RF_CDI(row):
        return(busca_valor("CDI",row))
    def RF_TBond(row):
        return(busca_valor("T-Bond 30Y",row))
    def RM_SELIC(row):
        return(busca_valor("SELIC",row))
    def Beta(row):
        return(1)
    def RP_Risco_País(row):
        return(busca_valor("Risco País",row)/100)
    def WACC(row):
        return (
            Custo_do_Capital_Próprio_Ke(row) * Participação_do_Capital_Próprio_Ke(row) +
            Custo_do_Capital_de_Terceiros_Ki(row) * Participação_do_Capital_de_Terceiros_Ki(row)
        )
    def Garantia_de_Capital_de_Terceiros(row):
        return(busca_valor("3",row) / (busca_valor("2.01",row) + busca_valor("2.02",row)))
    def Composição_do_Endividamento(row):
        return(busca_valor("2.01",row) / (busca_valor("2.01",row) + busca_valor("2.02",row)))
    def Endividamento_Geral(row):
        return((busca_valor("2.01",row) + busca_valor("2.02",row)) / busca_valor("3",row))
    def Endividamento_Financeiro(row):
        return((busca_valor("2.01.01.02",row) + busca_valor("2.02.01.01",row)) / busca_valor("3",row))
    def Grau_de_Endividamento_Tributário(row):
        return(
            (busca_valor("2.01.01.03",row) + busca_valor("2.01.01.08",row) + busca_valor("2.02.01.07",row) + busca_valor("2.02.01.03",row)) / 
            busca_valor("3",row)
        )
    def Grau_de_Imobilizações(row):
        return(busca_valor("1.02.03",row) / busca_valor("3",row))
    def Grau_de_Permanência_do_Ativo(row):
        return(busca_valor("1.02.03",row) / busca_valor("1",row))
    def Liquidez_Circulante(row):
        return(busca_valor("1.01",row) / busca_valor("2.01",row))
    def Liquidez_Seca(row):
        return((busca_valor("1.01",row) - busca_valor("1.01.03",row)) / busca_valor("2.01",row))
    def Liquidez_Imediata(row):
        return(busca_valor("1.01.01",row) / busca_valor("2.01",row))
    def Liquidez_Geral(row):
        return((busca_valor("1.01",row) + busca_valor("1.02.01",row)) / (busca_valor("2.01",row) + busca_valor("2.02",row)))
    def Caixa_Gerado_Nas_Operações(row):
        return(
            (busca_valor("3.04.01",row,"CRÉDITO") - busca_valor("3.04.01",row,"DÉBITO")) +
            (busca_valor("3.04.02",row,"CRÉDITO") - busca_valor("3.04.02",row,"DÉBITO")) +
            (busca_valor("9.01",row,"CRÉDITO") - busca_valor("9.01",row,"DÉBITO")) +
            (busca_valor("3.05",row,"CRÉDITO") - busca_valor("3.05",row,"DÉBITO")) +
            (busca_valor("2.01.01.06",row,"CRÉDITO") - busca_valor("2.01.01.06",row,"DÉBITO")) + 
            (busca_valor("2.02.01.04",row,"CRÉDITO") - busca_valor("2.02.01.04",row,"DÉBITO"))
        )
    def Capacidade_de_Quitação_dos_Empréstimos(row):
        return((busca_valor("2.01.01.02",row) + busca_valor("2.02.01.01",row)) / Caixa_Líquido_das_Atividades_Operacionais(row))
    def Taxa_de_Retorno_de_Caixa(row):
        return(Caixa_Líquido_das_Atividades_Operacionais(row) / Investimentos_Totais(row))
    def Nível_de_Recebimento_de_Vendas(row):
        return(
            Caixa_Líquido_das_Atividades_Operacionais(row) / 
            (busca_valor("3.04.01.03.04.01",row,"CRÉDITO") - busca_valor("3.04.01.03.04.01",row,"DÉBITO"))
        )
    def Geração_Caixa_vs_Capital_Próprio(row):
        return(Caixa_Líquido_das_Atividades_Operacionais(row) / Capital_Próprios(row))
    def Variações_Nos_Ativos_e_Passivos(row):
        return(
            (busca_valor("1.01.02",row,"CRÉDITO") - busca_valor("1.01.02",row,"DÉBITO")) +
            (busca_valor("1.02.01.02",row,"CRÉDITO") - busca_valor("1.02.01.02",row,"DÉBITO")) +
            (busca_valor("1.01.03",row,"CRÉDITO") - busca_valor("1.01.03",row,"DÉBITO")) +
            (busca_valor("1.01.04",row,"CRÉDITO") - busca_valor("1.01.04",row,"DÉBITO")) +
            (busca_valor("1.02.01.06",row,"CRÉDITO") - busca_valor("1.02.01.06",row,"DÉBITO")) + 
            (busca_valor("1.01.05",row,"CRÉDITO") - busca_valor("1.01.05",row,"DÉBITO")) +
            (busca_valor("1.01.06",row,"CRÉDITO") - busca_valor("1.01.06",row,"DÉBITO")) +
            (busca_valor("1.02.01.07",row,"CRÉDITO") - busca_valor("1.02.01.07",row,"DÉBITO")) +
            (busca_valor("1.01.08",row,"CRÉDITO") - busca_valor("1.01.08",row,"DÉBITO")) +
            (busca_valor("1.02.01.08",row,"CRÉDITO") - busca_valor("1.02.01.08",row,"DÉBITO")) +
            (busca_valor("1.01.07",row,"CRÉDITO") - busca_valor("1.01.07",row,"DÉBITO")) +
            (busca_valor("1.02.05",row,"CRÉDITO") - busca_valor("1.02.05",row,"DÉBITO")) +
            (busca_valor("2.01.01.01",row,"CRÉDITO") - busca_valor("2.01.01.01",row,"DÉBITO")) +
            (busca_valor("2.02.01.02",row,"CRÉDITO") - busca_valor("2.02.01.02",row,"DÉBITO")) +
            (busca_valor("2.01.01.03",row,"CRÉDITO") - busca_valor("2.01.01.03",row,"DÉBITO")) +
            (busca_valor("2.02.01.03",row,"CRÉDITO") - busca_valor("2.02.01.03",row,"DÉBITO")) +
            (busca_valor("2.01.01.04",row,"CRÉDITO") - busca_valor("2.01.01.04",row,"DÉBITO")) +
            (busca_valor("2.01.01.07",row,"CRÉDITO") - busca_valor("2.01.01.07",row,"DÉBITO")) +
            (busca_valor("2.01.01.09",row,"CRÉDITO") - busca_valor("2.01.01.09",row,"DÉBITO")) +
            (busca_valor("2.02.01.05",row,"CRÉDITO") - busca_valor("2.02.01.05",row,"DÉBITO")) +
            (busca_valor("2.02.01.06",row,"CRÉDITO") - busca_valor("2.02.01.06",row,"DÉBITO")) +
            (busca_valor("2.01.01.05",row,"CRÉDITO") - busca_valor("2.01.01.05",row,"DÉBITO"))
        )
    def Caixa_Líquido_das_Atividades_Operacionais(row):
        return(Caixa_Gerado_Nas_Operações(row) + Variações_Nos_Ativos_e_Passivos(row))

    for key in indicadores_dict:
        indicadores_dict[key] = locals()[indicadores_dict[key]]

    def calcular_indicador(row):
        func = indicadores_dict.get(row["DESCRIÇÃO"])
        if func:
            return func(row)

    indicadores['VALOR'] = indicadores.apply(calcular_indicador, axis=1)

    indicadores.sort_values(by=["MÊS","GRUPO","ORDEM"],inplace=True)
    indicadores.reset_index(drop=True, inplace=True)

    return base_final, indicadores
