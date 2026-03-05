from __future__ import annotations

from pathlib import Path
from datetime import datetime
import mysql.connector
import pandas as pd
import streamlit as st
from io import BytesIO


# =========================
# CONFIG DO BANCO
# =========================
DB = dict(
    host="localhost",
    user="root",
    password="123456",
    database="_m1",
)

OUT_XLSX = "resultado.xlsx"
OUT_LOG = "log.txt"
OUT_CONCAT_LOG = "concatenalog.txt"


# =========================
# MYSQL METADATA
# =========================
def list_tables(cur) -> list[str]:
    cur.execute("SHOW TABLES")
    tables = [r[0] for r in cur.fetchall()]
    tables.sort(key=lambda x: str(x).lower())
    return tables

def list_columns(cur, table: str) -> list[str]:
    cur.execute(f"SHOW COLUMNS FROM `{table}`")
    cols = [r[0] for r in cur.fetchall()]
    cols.sort(key=lambda x: str(x).lower())
    return cols


# =========================
# VALIDAÇÃO DE JOINS
# =========================
def validate_join(cur, t_left: str, c_left: str, t_right: str, c_right: str) -> dict:
    """
    Valida uma condição de JOIN e retorna estatísticas.
    Retorna: {
        'total_left': total de registros na tabela esquerda,
        'total_right': total de registros na tabela direita,
        'matched': quantidade de registros que encontram match,
        'unmatched_left': lista de valores em left que não encontram match,
        'unmatched_right': lista de valores em right que não encontram match
    }
    """
    try:
        # Total em ambas as tabelas
        cur.execute(f"SELECT COUNT(*) FROM `{t_left}`")
        total_left = cur.fetchone()[0]
        
        cur.execute(f"SELECT COUNT(*) FROM `{t_right}`")
        total_right = cur.fetchone()[0]
        
        # Quantidade de matches
        sql_matched = f"""
        SELECT COUNT(DISTINCT t1.`{c_left}`)
        FROM `{t_left}` t1
        INNER JOIN `{t_right}` t2 ON t1.`{c_left}` = t2.`{c_right}`
        """
        cur.execute(sql_matched)
        matched = cur.fetchone()[0]
        
        # Valores em left que não encontram match
        sql_unmatched_left = f"""
        SELECT DISTINCT t1.`{c_left}`
        FROM `{t_left}` t1
        LEFT JOIN `{t_right}` t2 ON t1.`{c_left}` = t2.`{c_right}`
        WHERE t2.`{c_right}` IS NULL
        LIMIT 100
        """
        cur.execute(sql_unmatched_left)
        unmatched_left = [row[0] for row in cur.fetchall()]
        
        # Valores em right que não encontram match
        sql_unmatched_right = f"""
        SELECT DISTINCT t2.`{c_right}`
        FROM `{t_right}` t2
        LEFT JOIN `{t_left}` t1 ON t1.`{c_left}` = t2.`{c_right}`
        WHERE t1.`{c_left}` IS NULL
        LIMIT 100
        """
        cur.execute(sql_unmatched_right)
        unmatched_right = [row[0] for row in cur.fetchall()]
        
        return {
            'total_left': total_left,
            'total_right': total_right,
            'matched': matched,
            'unmatched_left': unmatched_left,
            'unmatched_right': unmatched_right
        }
    except Exception as e:
        return {
            'error': str(e),
            'total_left': 0,
            'total_right': 0,
            'matched': 0,
            'unmatched_left': [],
            'unmatched_right': []
        }


# =========================
# MAIN
# =========================
def main():
    st.title("Assistente de JOIN (Múltiplas Tabelas) - Web Interface")
    st.write("Ferramenta para realizar JOINs entre tabelas MySQL de forma interativa.")

    # Conectar ao banco
    conn = mysql.connector.connect(**DB)
    cur = conn.cursor()

    # Listar tabelas
    all_tables = list_tables(cur)
    if not all_tables:
        st.error("Nenhuma tabela encontrada no banco de dados.")
        return

    # Inicializar session state
    if 'tables' not in st.session_state:
        st.session_state.tables = []
    if 'join_conditions' not in st.session_state:
        st.session_state.join_conditions = []
    if 'selected_cols' not in st.session_state:
        st.session_state.selected_cols = {}
    if 'validated' not in st.session_state:
        st.session_state.validated = False

    st.header("1. Escolher Tabelas Iniciais")
    col1, col2 = st.columns(2)
    with col1:
        t1 = st.selectbox("Tabela 1", all_tables, key="t1")
    with col2:
        t2 = st.selectbox("Tabela 2", [t for t in all_tables if t != t1], key="t2")

    st.header("2. Escolher Colunas para JOIN Inicial")
    cols1 = list_columns(cur, t1)
    cols2 = list_columns(cur, t2)
    col1, col2 = st.columns(2)
    with col1:
        c1 = st.selectbox(f"Coluna de {t1}", cols1, key="c1")
    with col2:
        c2 = st.selectbox(f"Coluna de {t2}", cols2, key="c2")

    if st.button("Validar JOIN Inicial"):
        validation = validate_join(cur, t1, c1, t2, c2)
        st.subheader("Resultado da Validação")
        st.write(f"Registros em {t1}: {validation['total_left']}")
        st.write(f"Registros em {t2}: {validation['total_right']}")
        st.write(f"Ligações encontradas: {validation['matched']}")
        if validation['matched'] == 0:
            st.error("Nenhuma ligação encontrada. Escolha outras colunas.")
            st.session_state.validated = False
        else:
            st.success("JOIN válido! Você pode prosseguir.")
            st.session_state.tables = [t1, t2]
            st.session_state.join_conditions = [(t1, c1, t2, c2)]
            st.session_state.selected_cols = {}
            st.session_state.validated = True

    if st.session_state.validated:
        st.header("3. Escolher Colunas para o Relatório")
        col1, col2 = st.columns(2)
        with col1:
            cols1_sel = st.multiselect(f"Colunas de {t1}", cols1, key="cols1")
        with col2:
            cols2_sel = st.multiselect(f"Colunas de {t2}", cols2, key="cols2")
        st.session_state.selected_cols[t1] = cols1_sel
        st.session_state.selected_cols[t2] = cols2_sel

        st.header("4. Adicionar Mais Tabelas (Opcional)")
        if st.button("Adicionar Nova Tabela"):
            st.session_state.adding = True

        if 'adding' in st.session_state and st.session_state.adding:
            remaining = [t for t in all_tables if t not in st.session_state.tables]
            if not remaining:
                st.warning("Todas as tabelas já foram adicionadas.")
                st.session_state.adding = False
            else:
                t_new = st.selectbox("Escolha a nova tabela", remaining, key="t_new")
                base_options = st.session_state.tables
                t_base = st.selectbox("Tabela base para o JOIN", base_options, key="t_base")
                cols_base = list_columns(cur, t_base)
                c_base = st.selectbox(f"Coluna de {t_base}", cols_base, key="c_base")
                cols_new = list_columns(cur, t_new)
                c_new = st.selectbox(f"Coluna de {t_new}", cols_new, key="c_new")

                if st.button("Validar e Adicionar Tabela"):
                    validation_new = validate_join(cur, t_base, c_base, t_new, c_new)
                    st.write(f"Ligações encontradas: {validation_new['matched']}")
                    if validation_new['matched'] > 0:
                        st.session_state.tables.append(t_new)
                        st.session_state.join_conditions.append((t_base, c_base, t_new, c_new))
                        cols_new_sel = st.multiselect(f"Colunas de {t_new}", cols_new, key=f"cols_{t_new}")
                        st.session_state.selected_cols[t_new] = cols_new_sel
                        st.success(f"Tabela {t_new} adicionada!")
                        st.session_state.adding = False
                    else:
                        st.error("Nenhuma ligação. Tente outras colunas.")

        st.header("5. Gerar Relatório")
        if st.button("Executar Consulta e Gerar Relatório"):
            # Montar SQL
            table_aliases = {t: f"t{i+1}" for i, t in enumerate(st.session_state.tables)}
            select_parts = []
            for t in st.session_state.tables:
                alias = table_aliases[t]
                for col in st.session_state.selected_cols.get(t, []):
                    select_parts.append(f"`{alias}`.`{col}` AS `{t}.{col}`")
            from_parts = [f"`{st.session_state.tables[0]}` AS {table_aliases[st.session_state.tables[0]]}"]
            for t_left, c_left, t_right, c_right in st.session_state.join_conditions:
                alias_left = table_aliases[t_left]
                alias_right = table_aliases[t_right]
                from_parts.append(f"JOIN `{t_right}` AS {alias_right} ON {alias_left}.`{c_left}` = {alias_right}.`{c_right}`")
            sql = f"SELECT {', '.join(select_parts)} FROM {' '.join(from_parts)}"
            st.code(sql, language="sql")
            df = pd.read_sql(sql, conn)
            st.write(f"Linhas retornadas: {len(df)}")
            if len(df) > 1000000:
                st.error("Resultado muito grande! Possível cross join.")
            else:
                st.dataframe(df.head(100))
                # Download CSV
                csv = df.to_csv(index=False)
                st.download_button("Baixar CSV", csv, "resultado.csv", "text/csv")
                # Download XLSX
                buffer = BytesIO()
                df.to_excel(buffer, index=False)
                buffer.seek(0)
                st.download_button("Baixar XLSX", buffer.getvalue(), "resultado.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    conn.close()


if __name__ == "__main__":
    main()

