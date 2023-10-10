import pandas as pd
import streamlit as st
from tratamento import calcula_base, balancetes as demo_balancetes, plano_contas_real as demo_plano_contas
pd.options.display.float_format = '{:.2f}'.format
from streamlit_extras.add_vertical_space import add_vertical_space
from streamlit_extras.dataframe_explorer import dataframe_explorer
import plotly.express as px

def main():
    st.set_page_config(layout="wide")

    st.title("GERADOR DE ANA")

    if "placeholder" not in st.session_state:
        st.session_state["placeholder"] = st.empty()

    # UPLOAD DOS DADOS
    if "uploaded" not in st.session_state:
        with st.session_state["placeholder"].container():
            st.write('Por favor, faça o upload de ambos os arquivos em formato CSV, com delimitador ; e enconding UTF-8.')              

            if st.button(label="😒 Não quero carregar dados, me mostre uma demo",type="primary"):
                st.session_state["demo"] = True
                st.session_state['uploaded'] = True

            add_vertical_space(1)

            col1, col2, col3, col4 = st.columns([2,0.2,1,1])
            with col1:
                st.header("Upload de arquivos")
                b = st.file_uploader("Upload Balancetes", type=["csv"])
            with col3:
                st.header("Arquivos modelo")
                st.write("Baixe os arquivos de modelo para saber como os dados devem ser carregados ao sistema.")
                st.download_button(label="Baixar balancete modelo",data=demo_balancetes.to_csv(index=False).encode('utf-8'),mime='text/csv',file_name="Balancetes Modelo.csv")
                st.download_button(label="Baixar plano de contas modelo",data=demo_plano_contas.to_csv(index=False).encode('utf-8'),mime='text/csv',file_name="Plano de contas modelo.csv")

            with col1:
                p = st.file_uploader("Upload Plano de contas", type=["csv"])

            if b is not None and p is not None:
                st.session_state['uploaded'] = True

    # CÁLCULO DOS DADOS
    if "uploaded" in st.session_state and "dados" not in st.session_state:       
        with st.spinner("Calculando os dados, pode levar uns 10 segundos, aguarde..."):
            with st.session_state["placeholder"].container():
                if "demo" in st.session_state:
                    balancetes = demo_balancetes
                    plano_contas = demo_plano_contas
                else:
                    balancetes = pd.read_csv(b,delimiter=";",decimal=",", encoding="utf-8")
                    plano_contas = pd.read_csv(p,delimiter=";",decimal=",", encoding="utf-8")
                st.session_state["dados"], st.session_state["indicadores"] = calcula_base(balancetes, plano_contas)

    # EXIBIÇÃO DOS DADOS
    if "dados" in st.session_state:
        with st.session_state["placeholder"].container():
            if st.button(label="Carregar novos dados"):
                del st.session_state["dados"]
                del st.session_state["uploaded"]
                del st.session_state["demo"]
                st.rerun()

            st.markdown("Criado com 😭 e 💪 e 🥹 por [Rodrigo Ferreira](https://www.linkedin.com/in/rodrigoavf/)")
            st.title("ANALISE - Noiva do Mar 🧜‍♀️")

            st.header("Base de dados")

            col1,col2 = st.columns([1,2.5])
            with col1:
                grau = st.slider("Selecione o grau de granularidade dos dados da tabela abaixo",min_value=1,max_value=st.session_state["dados"]["GRAU"].max(),value=st.session_state["dados"]["GRAU"].max())
            
            col1,col2 = st.columns([3,1])
            with col2:
                dados_filtrados = dataframe_explorer(st.session_state["dados"][st.session_state["dados"]["GRAU"]<=grau], case=False)
            with col1:
                st.dataframe(data=dados_filtrados, use_container_width=True, height=400,column_config={"MÊS":st.column_config.DateColumn(format="MMM YYYY")})
                st.header("Lista de indicadores")
            st.dataframe(data=st.session_state["indicadores"], use_container_width=True, height=400,column_config={"MÊS":st.column_config.DateColumn(format="MMM YYYY")})

            st.header("Ativo vs Passivo")
            st.plotly_chart(px.line(st.session_state["dados"][(st.session_state["dados"]['CÓDIGO']=="1") | (st.session_state["dados"]['CÓDIGO']=="2")], 
                                    x="MÊS",
                                    y="SALDO ATUAL",
                                    color="DESCRIÇÃO",
                                    markers=True,
                                    color_discrete_map={'Ativo': 'blue', 'Passivo': 'red'}
                                    ),
                                    use_container_width=True)

            col1, col2 = st.columns(2)

            with col1:
                st.header("Ativo")
                st.plotly_chart(px.bar(st.session_state["dados"][(st.session_state["dados"]['CÓDIGO']=="1.01") | (st.session_state["dados"]['CÓDIGO']=="1.02")],
                                            x="SALDO ATUAL",
                                            y="DESCRIÇÃO",
                                            color="DESCRIÇÃO",
                                            color_discrete_map={'Ativo Circulante': 'blue', 'Ativo Não Circulante': 'red'}))
            with col2:
                st.header("Passivo")
                st.plotly_chart(px.bar(st.session_state["dados"][(st.session_state["dados"]['CÓDIGO']=="2.01") | (st.session_state["dados"]['CÓDIGO']=="2.02") | (st.session_state["dados"]['CÓDIGO']=="3")],
                                            x="SALDO ATUAL",
                                            y="DESCRIÇÃO",
                                            color="DESCRIÇÃO",
                                            color_discrete_map={'Passivo Circulante': 'orange', 'Passivo Não-Circulante': 'blue', 'Patrimônio Líquido': 'green'}))


if __name__ == "__main__":
    main()
