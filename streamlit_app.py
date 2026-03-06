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
)

OUT_XLSX = "resultado.xlsx"
OUT_LOG = "log.txt"
OUT_CONCAT_LOG = "concatenalog.txt"


# =========================
# LOGGING
# =========================
def log_action(action: str, details: str = ""):
    """Registra uma ação no arquivo de log com timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_entry = f"[{timestamp}] {action}"
    if details:
        log_entry += f" | {details}"
    log_entry += "\n"
    
    with open(OUT_LOG, "a", encoding="utf-8") as f:
        f.write(log_entry)
    
    print(log_entry.strip())  # Debug output


def init_log():
    """Inicia o arquivo de log com um cabeçalho."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = f"{'='*80}\nLOG DE CONSULTAS - {timestamp}\n{'='*80}\n"
    with open(OUT_LOG, "w", encoding="utf-8") as f:
        f.write(header)


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

def list_databases(cur) -> list[str]:
    cur.execute("SHOW DATABASES")
    dbs = [r[0] for r in cur.fetchall()]
    blocked = {"information_schema", "mysql", "performance_schema", "sys"}
    dbs = [db for db in dbs if db not in blocked]
    dbs.sort(key=lambda x: str(x).lower())
    return dbs


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
    # Configurar página para usar largura completa
    st.set_page_config(layout="wide", page_title="Assistente de JOIN")
    
    st.title("Assistente de JOIN (Múltiplas Tabelas) - Web Interface")
    st.write("Ferramenta para realizar JOINs entre tabelas MySQL de forma interativa.")

    # Inicializar arquivo de log
    if 'log_initialized' not in st.session_state:
        init_log()
        log_action("APLICAÇÃO INICIADA")
        st.session_state.log_initialized = True

    st.header("0. Configurar Conexão MySQL")
    col_db1, col_db2, col_db3 = st.columns(3)
    with col_db1:
        db_host = st.text_input("Host/IP MySQL", value=DB.get("host", "localhost"))
    with col_db2:
        db_user = st.text_input("Usuário", value=DB.get("user", "root"))
    with col_db3:
        db_password = st.text_input("Senha", value=DB.get("password", ""), type="password")

    if st.button("Conectar e listar bases", key="btn_connect_mysql"):
        try:
            conn_temp = mysql.connector.connect(
                host=db_host,
                user=db_user,
                password=db_password,
            )
            cur_temp = conn_temp.cursor()
            st.session_state.available_databases = list_databases(cur_temp)
            st.session_state.db_host = db_host
            st.session_state.db_user = db_user
            st.session_state.db_password = db_password
            st.session_state.db_connected = True
            conn_temp.close()
            log_action("BANCO DE DADOS", f"Servidor conectado: {db_host}")
        except Exception as e:
            st.session_state.db_connected = False
            st.error(f"Erro ao conectar no MySQL: {e}")
            log_action("ERRO CONEXAO MYSQL", str(e))
            return

    if not st.session_state.get("db_connected"):
        st.info("Informe host/IP, usuário e senha, depois clique em 'Conectar e listar bases'.")
        return

    dbs = st.session_state.get("available_databases", [])
    if not dbs:
        st.error("Nenhuma base disponível encontrada nesse servidor.")
        return

    selected_database = st.selectbox("Base para análise", dbs, key="selected_database")
    if st.session_state.get("last_selected_database") != selected_database:
        keys_to_clear = [
            'tables', 'join_conditions', 'selected_cols', 'validated',
            'adding', 'col_order', 'last_t1', 'last_t2', 'last_c1', 'last_c2',
            't1_select', 't2_select', 'c1_select', 'c2_select',
            't_new_select', 't_base_select', 'c_base_select', 'c_new_select',
            'cols1', 'cols2'
        ]
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        dynamic_keys = [k for k in st.session_state.keys() if k.startswith("cols_") or k.startswith("order_")]
        for key in dynamic_keys:
            del st.session_state[key]
        st.session_state.last_selected_database = selected_database

    # Conectar ao banco selecionado
    conn = mysql.connector.connect(
        host=st.session_state.db_host,
        user=st.session_state.db_user,
        password=st.session_state.db_password,
        database=selected_database,
    )
    cur = conn.cursor()
    log_action("BANCO DE DADOS", f"Conectado ao MySQL | Host: {st.session_state.db_host} | Base: {selected_database}")

    # Listar tabelas
    all_tables = list_tables(cur)
    if not all_tables:
        st.error("Nenhuma tabela encontrada no banco de dados.")
        log_action("ERRO", "Nenhuma tabela encontrada")
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

    def render_selected_columns(tables: list[str]):
        if not tables:
            return

        st.header("3. Escolher Colunas para o Relatório")
        for table in tables:
            cols_table = list_columns(cur, table)
            widget_key = f"cols_{table}"
            if widget_key not in st.session_state:
                st.session_state[widget_key] = st.session_state.selected_cols.get(table, [])
            selected = st.multiselect(
                f"Colunas de {table}",
                cols_table,
                key=widget_key,
            )
            st.session_state.selected_cols[table] = selected

        if any(st.session_state.selected_cols.get(t, []) for t in tables):
            details = " | ".join(
                [f"{t}: {', '.join(st.session_state.selected_cols.get(t, []))}" for t in tables]
            )
            log_action("SELEÇÃO DE COLUNAS", details)

    # Botão para limpar tudo
    col_clear1, col_clear2, col_clear3 = st.columns([1, 8, 1])
    with col_clear3:
        if st.button("Limpar Tudo", key="btn_limpar"):
            # Limpar todas as chaves de session_state
            keys_to_clear = [
                'tables', 'join_conditions', 'selected_cols', 'validated',
                'adding', 'col_order', 'last_t1', 'last_t2', 'last_c1', 'last_c2',
                'log_initialized', 't1_select', 't2_select', 'c1_select', 'c2_select'
            ]
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]
            dynamic_keys = [k for k in st.session_state.keys() if k.startswith("cols_") or k.startswith("order_")]
            for key in dynamic_keys:
                del st.session_state[key]
            
            log_action("LIMPEZA", "Todos os campos foram resetados")
            st.rerun()

    st.header("1. Escolher Tabelas Iniciais")
    col1, col2 = st.columns(2)
    with col1:
        tables_with_empty = ["-- Selecione uma tabela --"] + all_tables
        t1_idx = st.selectbox("Tabela 1", range(len(tables_with_empty)), 
                              format_func=lambda x: tables_with_empty[x], key="t1_select")
        t1 = tables_with_empty[t1_idx] if t1_idx > 0 else None
    with col2:
        available_t2 = ["-- Selecione uma tabela --"] + [t for t in all_tables if t != t1] if t1 else ["-- Selecione primeiro a Tabela 1 --"]
        t2_idx = st.selectbox("Tabela 2", range(len(available_t2)), 
                              format_func=lambda x: available_t2[x], key="t2_select")
        t2 = available_t2[t2_idx] if t2_idx > 0 else None
    
    # Validar se tabelas foram selecionadas
    if not t1 or not t2:
        st.warning("Selecione ambas as tabelas para prosseguir")
        conn.close()
        return
    
    # Log de seleção de tabelas
    if st.session_state.get('last_t1') != t1 or st.session_state.get('last_t2') != t2:
        log_action("SELEÇÃO DE TABELAS", f"Tabela 1: {t1} | Tabela 2: {t2}")
        st.session_state['last_t1'] = t1
        st.session_state['last_t2'] = t2

    st.header("2. Escolher Colunas para JOIN Inicial")
    cols1 = list_columns(cur, t1)
    cols2 = list_columns(cur, t2)
    col1, col2 = st.columns(2)
    with col1:
        cols1_with_empty = ["-- Selecione uma coluna --"] + cols1
        c1_idx = st.selectbox(f"Coluna de {t1}", range(len(cols1_with_empty)), 
                              format_func=lambda x: cols1_with_empty[x], key="c1_select")
        c1 = cols1_with_empty[c1_idx] if c1_idx > 0 else None
    with col2:
        cols2_with_empty = ["-- Selecione uma coluna --"] + cols2
        c2_idx = st.selectbox(f"Coluna de {t2}", range(len(cols2_with_empty)), 
                              format_func=lambda x: cols2_with_empty[x], key="c2_select")
        c2 = cols2_with_empty[c2_idx] if c2_idx > 0 else None
    
    # Validar se colunas foram selecionadas
    if not c1 or not c2:
        st.warning("Selecione as colunas para o JOIN")
        conn.close()
        return
    
    # Log de seleção de colunas para JOIN
    if st.session_state.get('last_c1') != c1 or st.session_state.get('last_c2') != c2:
        log_action("SELEÇÃO DE COLUNAS JOIN", f"{t1}.{c1} = {t2}.{c2}")
        st.session_state['last_c1'] = c1
        st.session_state['last_c2'] = c2

    if st.button("Validar JOIN Inicial"):
        validation = validate_join(cur, t1, c1, t2, c2)
        st.subheader("Resultado da Validação")
        st.write(f"Registros em {t1}: {validation['total_left']}")
        st.write(f"Registros em {t2}: {validation['total_right']}")
        st.write(f"Ligações encontradas: {validation['matched']}")
        if validation['matched'] == 0:
            st.error("Nenhuma ligação encontrada. Escolha outras colunas.")
            st.session_state.validated = False
            log_action("VALIDAÇÃO JOIN", f"FALHOU: {t1}.{c1} = {t2}.{c2} | Nenhuma ligação encontrada")
        else:
            st.success("JOIN válido! Você pode prosseguir.")
            st.session_state.tables = [t1, t2]
            st.session_state.join_conditions = [(t1, c1, t2, c2)]
            st.session_state.selected_cols = {}
            st.session_state.validated = True
            log_action("VALIDAÇÃO JOIN", f"SUCESSO: {t1}.{c1} = {t2}.{c2} | Ligações: {validation['matched']} | Total {t1}: {validation['total_left']} | Total {t2}: {validation['total_right']}")

    if st.session_state.validated:
        render_selected_columns(st.session_state.tables)

        st.header("4. Adicionar Mais Tabelas (Opcional)")
        if st.button("Adicionar Nova Tabela"):
            st.session_state.adding = True

        if 'adding' in st.session_state and st.session_state.adding:
            remaining = [t for t in all_tables if t not in st.session_state.tables]
            if not remaining:
                st.warning("Todas as tabelas já foram adicionadas.")
                st.session_state.adding = False
            else:
                # Padronizar com opção vazia como nos outros selectbox
                tables_with_empty = ["-- Selecione a nova tabela --"] + remaining
                t_new_idx = st.selectbox("Escolha a nova tabela", range(len(tables_with_empty)), 
                                        format_func=lambda x: tables_with_empty[x], key="t_new_select")
                t_new = tables_with_empty[t_new_idx] if t_new_idx > 0 else None
                
                if not t_new:
                    st.warning("Selecione uma tabela")
                else:
                    base_options = st.session_state.tables
                    base_with_empty = ["-- Selecione a tabela base --"] + base_options
                    t_base_idx = st.selectbox("Tabela base para o JOIN", range(len(base_with_empty)), 
                                             format_func=lambda x: base_with_empty[x], key="t_base_select")
                    t_base = base_with_empty[t_base_idx] if t_base_idx > 0 else None
                    
                    if not t_base:
                        st.warning("Selecione uma tabela base")
                    else:
                        cols_base = list_columns(cur, t_base)
                        cols_base_with_empty = ["-- Selecione uma coluna --"] + cols_base
                        c_base_idx = st.selectbox(f"Coluna de {t_base}", range(len(cols_base_with_empty)), 
                                                 format_func=lambda x: cols_base_with_empty[x], key="c_base_select")
                        c_base = cols_base_with_empty[c_base_idx] if c_base_idx > 0 else None
                        
                        if not c_base:
                            st.warning("Selecione uma coluna da tabela base")
                        else:
                            cols_new = list_columns(cur, t_new)
                            cols_new_with_empty = ["-- Selecione uma coluna --"] + cols_new
                            c_new_idx = st.selectbox(f"Coluna de {t_new}", range(len(cols_new_with_empty)), 
                                                    format_func=lambda x: cols_new_with_empty[x], key="c_new_select")
                            c_new = cols_new_with_empty[c_new_idx] if c_new_idx > 0 else None
                            
                            if not c_new:
                                st.warning("Selecione uma coluna da nova tabela")
                            else:
                                if st.button("Validar e Adicionar Tabela"):
                                    # Validar se as colunas realmente existem
                                    if c_base not in cols_base:
                                        st.error(f"Coluna '{c_base}' não encontrada em {t_base}")
                                        log_action("ERRO ADIÇÃO TABELA", f"Coluna '{c_base}' não existe em {t_base}")
                                    elif c_new not in cols_new:
                                        st.error(f"Coluna '{c_new}' não encontrada em {t_new}")
                                        log_action("ERRO ADIÇÃO TABELA", f"Coluna '{c_new}' não existe em {t_new}")
                                    else:
                                        validation_new = validate_join(cur, t_base, c_base, t_new, c_new)
                                        st.write(f"Ligações encontradas: {validation_new['matched']}")
                                        if validation_new['matched'] > 0:
                                            st.session_state.tables.append(t_new)
                                            st.session_state.join_conditions.append((t_base, c_base, t_new, c_new))
                                            st.session_state.selected_cols[t_new] = st.session_state.get(f"cols_{t_new}", [])
                                            log_action("ADIÇÃO DE TABELA", f"Tabela {t_new} adicionada | JOIN: {t_base}.{c_base} = {t_new}.{c_new}")
                                            st.success(f"Tabela {t_new} adicionada!")
                                            st.session_state.adding = False
                                            # Limpar chaves de selectbox para próxima adição
                                            if 't_new_select' in st.session_state:
                                                del st.session_state['t_new_select']
                                            if 't_base_select' in st.session_state:
                                                del st.session_state['t_base_select']
                                            if 'c_base_select' in st.session_state:
                                                del st.session_state['c_base_select']
                                            if 'c_new_select' in st.session_state:
                                                del st.session_state['c_new_select']
                                            st.rerun()
                                        else:
                                            st.error("Nenhuma ligação. Tente outras colunas.")
                                            log_action("ERRO ADIÇÃO TABELA", f"Nenhuma ligação encontrada para {t_base}.{c_base} = {t_new}.{c_new}")

        # Step 5: Organizar Colunas (aparece dinamicamente com qualquer tabela adicionada)
        # Verificar se há alguma coluna selecionada
        total_colunas = sum(len(st.session_state.selected_cols.get(t, [])) for t in st.session_state.tables)
        
        if total_colunas > 0:
            st.header("5. Organizar Colunas do Relatório")
            st.write("Reordene as colunas selecionadas usando os campos de número. (1 = primeira coluna, 2 = segunda, etc)")
            
            # Preparar lista de todas as colunas selecionadas
            todas_cols = {}
            for tabela in st.session_state.tables:
                cols_selecionadas = st.session_state.selected_cols.get(tabela, [])
                if cols_selecionadas:
                    todas_cols[tabela] = cols_selecionadas
            
            # Inicializar ordem das colunas se não existir
            if 'col_order' not in st.session_state:
                st.session_state.col_order = {}
            
            # Interface de reordenação
            col_mapping = {}  # Mapeia (tabela, coluna) -> número de ordem
            idx = 1
            
            for tabela in todas_cols:
                st.subheader(f"Tabela: {tabela}")
                cols_container = st.container()
                
                for col in todas_cols[tabela]:
                    col1, col2, col3 = st.columns([1, 3, 1])
                    with col1:
                        ordem = st.number_input(
                            f"Posição de {col}",
                            min_value=1,
                            value=idx,
                            key=f"order_{tabela}_{col}"
                        )
                        col_mapping[(tabela, col)] = ordem
                    with col2:
                        st.write(f"**{tabela}**.{col}")
                    with col3:
                        st.write(f"#{ordem}")
                    idx += 1
            
            # Ordenar colunas por número
            st.subheader("Preview das Colunas na Ordem Decidida")
            colunas_ordenadas = sorted(col_mapping.items(), key=lambda x: x[1])
            
            with st.container():
                st.markdown("**Ordem Final das Colunas:**")
                col_list = [f"{i+1}. `{tabela}`.`{col}`" for i, ((tabela, col), _) in enumerate(colunas_ordenadas)]
                st.markdown("\n".join(col_list))
            
            # Salvar ordem para executar a consulta
            st.session_state.col_order = col_mapping
            
            # Log de organização de colunas
            ordem_final = " -> ".join([f"{tabela}.{col}(#{ordem})" for (tabela, col), ordem in colunas_ordenadas])
            log_action("ORGANIZAÇÃO DE COLUNAS", ordem_final)

        st.header("6. Gerar Relatório")
        if st.button("Executar Consulta e Gerar Relatório"):
            # Montar SQL respeitando a ordem das colunas
            table_aliases = {t: f"t{i+1}" for i, t in enumerate(st.session_state.tables)}
            select_parts = []
            
            # Se há ordem definida, usar essa ordem
            if st.session_state.get('col_order'):
                colunas_ordenadas = sorted(
                    st.session_state.col_order.items(),
                    key=lambda x: x[1]
                )
                for (tabela, col), _ in colunas_ordenadas:
                    alias = table_aliases.get(tabela)
                    if alias:
                        select_parts.append(f"`{alias}`.`{col}` AS `{tabela}.{col}`")
            else:
                # Fallback para ordem original
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
            
            # Log de execução de consulta
            colunas_str = " | ".join([f"{t}: {', '.join(st.session_state.selected_cols.get(t, []))}" for t in st.session_state.tables])
            log_action("SELEÇÃO DE COLUNAS", colunas_str)
            log_action("EXECUÇÃO DE CONSULTA", f"SQL gerado com {len(select_parts)} colunas")
            
            try:
                df = pd.read_sql(sql, conn)
                st.write(f"Linhas retornadas: {len(df)}")
                log_action("CONSULTA SUCESSO", f"Linhas retornadas: {len(df)}")
                
                if len(df) > 1000000:
                    st.error("Resultado muito grande! Possível cross join.")
                    log_action("AVISO", "Resultado muito grande (>1M linhas) - Possível cross join")
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
                    log_action("EXPORTAÇÃO", "Arquivos CSV e XLSX disponíveis para download")
            except Exception as e:
                st.error(f"Erro ao executar consulta: {e}")
                log_action("ERRO NA CONSULTA", str(e))

    conn.close()


if __name__ == "__main__":
    main()



