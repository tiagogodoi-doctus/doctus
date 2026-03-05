"""
Streamlit Web App para Assistente de JOIN
Execute com: streamlit run streamlit_app.py
"""

# Fix para Windows + Python 3.13
import fix_asyncio

from pathlib import Path
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
    """Valida uma condição de JOIN e retorna estatísticas."""
    try:
        cur.execute(f"SELECT COUNT(*) FROM `{t_left}`")
        total_left = cur.fetchone()[0]
        
        cur.execute(f"SELECT COUNT(*) FROM `{t_right}`")
        total_right = cur.fetchone()[0]
        
        sql_matched = f"""
        SELECT COUNT(DISTINCT t1.`{c_left}`)
        FROM `{t_left}` t1
        INNER JOIN `{t_right}` t2 ON t1.`{c_left}` = t2.`{c_right}`
        """
        cur.execute(sql_matched)
        matched = cur.fetchone()[0]
        
        sql_unmatched_left = f"""
        SELECT DISTINCT t1.`{c_left}`
        FROM `{t_left}` t1
        LEFT JOIN `{t_right}` t2 ON t1.`{c_left}` = t2.`{c_right}`
        WHERE t2.`{c_right}` IS NULL
        LIMIT 100
        """
        cur.execute(sql_unmatched_left)
        unmatched_left = [row[0] for row in cur.fetchall()]
        
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
# FUNÇÕES DE SUGESTÃO
# =========================
def extract_key_name(col: str) -> str:
    """
    Extrai o núcleo do nome de uma coluna de chave.
    Exemplos:
    - 'IdDerivacao' -> 'Derivacao'
    - 'idproduto' -> 'produto'
    - 'CodPro' -> 'Pro'
    - 'Derivacao_Id' -> 'Derivacao'
    """
    col_lower = col.lower()
    
    # Remover prefixos comuns
    for prefix in ['id', 'cod_', 'code_']:
        if col_lower.startswith(prefix):
            return col[len(prefix):]
    
    # Remover sufixos comuns
    for suffix in ['_id', 'id']:
        if col_lower.endswith(suffix):
            return col[:-len(suffix)]
    
    return col


def is_likely_key(col: str) -> bool:
    """Verifica se uma coluna é provavelmente uma chave (ID ou código)."""
    col_lower = col.lower()
    return (
        col_lower.startswith(('id', 'cod', 'code')) or
        col_lower.endswith(('_id', 'id')) or
        'id' in col_lower or
        'cod' in col_lower
    )


def suggest_join_columns(cur, table1: str, table2: str) -> list[tuple[str, str, str]]:
    """Sugere possíveis colunas para fazer JOIN entre duas tabelas."""
    cols1 = list_columns(cur, table1)
    cols2 = list_columns(cur, table2)
    
    suggestions = []
    
    # Filtrar colunas que parecem ser chaves
    key_cols1 = [(c, extract_key_name(c)) for c in cols1 if is_likely_key(c)]
    key_cols2 = [(c, extract_key_name(c)) for c in cols2 if is_likely_key(c)]
    
    # Comparar núcleos dos nomes
    for c1, key1 in key_cols1:
        for c2, key2 in key_cols2:
            key1_lower = key1.lower()
            key2_lower = key2.lower()
            
            # 1. Núcleos idênticos (case-insensitive)
            if key1_lower == key2_lower:
                suggestions.append((c1, c2, "Chave primária/estrangeira"))
            
            # 2. Um contém o outro (para nomes mais longos)
            elif len(key1_lower) > 2 and len(key2_lower) > 2:
                if (key1_lower in key2_lower or key2_lower in key1_lower):
                    # Evitar matches muito genéricos (como "id" em qualquer lugar)
                    if not (key1_lower in ['id', 'cod'] or key2_lower in ['id', 'cod']):
                        suggestions.append((c1, c2, "Correspondência parcial"))
    
    # Remover duplicatas mantendo as de melhor correspondência
    unique_suggestions = {}
    for c1, c2, reason in suggestions:
        key = (c1, c2)
        if key not in unique_suggestions:
            unique_suggestions[key] = reason
        elif "primária" in reason:
            unique_suggestions[key] = reason
    
    # Converter de volta para lista, ordenando por qualidade
    result = [(c1, c2, reason) for (c1, c2), reason in unique_suggestions.items()]
    # Priorizar "primária/estrangeira" antes de "correspondência parcial"
    result.sort(key=lambda x: (x[2] != "Chave primária/estrangeira", c1[:10]))
    
    return result[:8]  # Retorna até 8 sugestões


# =========================
# STREAMLIT APP
# =========================
def main():
    st.set_page_config(page_title="Assistente de JOIN", layout="wide")
    
    # Logo no topo
    col1, col2 = st.columns([1, 5])
    with col1:
        try:
            st.image("logo.png", width=80)
        except:
            st.write("📊")
    with col2:
        st.title("Assistente de JOIN (Múltiplas Tabelas)")
    
    st.write("Ferramenta web para realizar JOINs entre tabelas MySQL de forma interativa.")

    # (Campo de texto será movido para depois do passo 1)

    try:
        conn = mysql.connector.connect(**DB)
        cur = conn.cursor()
    except Exception as e:
        st.error(f"Erro ao conectar ao banco: {e}")
        return

    all_tables = list_tables(cur)
    if not all_tables:
        st.error("Nenhuma tabela encontrada no banco de dados.")
        conn.close()
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

    # Step 1: Escolher tabelas iniciais
    st.header("Step 1: Escolher Tabelas Iniciais")
    col1, col2 = st.columns(2)
    t1_default = all_tables[0]
    t2_default = all_tables[1] if len(all_tables) > 1 else all_tables[0]
    with col1:
        t1 = st.selectbox("Tabela 1", all_tables, key="t1", index=all_tables.index(t1_default))
    with col2:
        t2 = st.selectbox("Tabela 2", [t for t in all_tables if t != t1], key="t2", index=0 if t1==t2_default else ([t for t in all_tables if t != t1].index(t2_default) if t2_default in [t for t in all_tables if t != t1] else 0))

    # ===== CAMPO DE TEXTO E SUGESTÃO DE COLUNAS APÓS JOIN VALIDADO =====

    if st.session_state.validated:
        st.header("Step 3: Escolher Colunas para o Relatório")
        st.markdown("""
        <style>div[data-testid='stTextArea'] textarea {font-family:monospace;}</style>
        """, unsafe_allow_html=True)
        user_query = st.text_area(
            "Descreva as colunas desejadas para o relatório (ex: código, descrição, classificação fiscal...)",
            height=70,
            placeholder="Exemplo: código reduzido, descrição, classificação fiscal, código de barras, código de derivação"
        )
        # Buscar colunas novamente para garantir que existam
        cols1 = list_columns(cur, t1)
        cols2 = list_columns(cur, t2)
        # Botão de sugestão de colunas (controle por session_state)
        if 'last_user_query' not in st.session_state:
            st.session_state['last_user_query'] = ''
        if 'last_t1' not in st.session_state:
            st.session_state['last_t1'] = ''
        if 'last_t2' not in st.session_state:
            st.session_state['last_t2'] = ''
        if 'cols1_auto' not in st.session_state:
            st.session_state['cols1_auto'] = []
        if 'cols2_auto' not in st.session_state:
            st.session_state['cols2_auto'] = []
        if 'sugerir_colunas_flag' not in st.session_state:
            st.session_state['sugerir_colunas_flag'] = False

        if (user_query != st.session_state['last_user_query'] or t1 != st.session_state['last_t1'] or t2 != st.session_state['last_t2']):
            st.session_state['cols1_auto'] = []
            st.session_state['cols2_auto'] = []
            st.session_state['sugerir_colunas_flag'] = False
            st.session_state['last_user_query'] = user_query
            st.session_state['last_t1'] = t1
            st.session_state['last_t2'] = t2

        colunas_sugeridas = None
        if st.button("🔎 Sugerir colunas automaticamente", key="btn_sugerir_colunas"):
            st.session_state['sugerir_colunas_flag'] = True
            # Sempre calcula sugestões e exibe
            colunas_sugeridas = {'t1': [], 't2': []}
            if user_query:
                try:
                    palavras = []
                    import re
                    palavras = [p.strip().lower() for p in re.split(r'[\s,;\.]', user_query) if len(p.strip()) > 2]
                    match_cols1 = []
                    match_cols2 = []
                    # Buscar exemplos de valores das colunas
                    def exemplos_col(cur, tabela, coluna):
                        try:
                            cur.execute(f"SELECT `{coluna}` FROM `{tabela}` WHERE `{coluna}` IS NOT NULL LIMIT 10")
                            return [str(r[0]).lower() for r in cur.fetchall() if r[0] is not None]
                        except Exception:
                            return []
                    # Para tabela 1
                    for c in cols1:
                        c_norm = c.lower().replace('_', '').replace('-', '')
                        exemplos = exemplos_col(cur, t1, c)
                        for p in palavras:
                            if p in c_norm or c_norm in p or p in c.lower():
                                match_cols1.append(c)
                            else:
                                for ex in exemplos:
                                    if p in ex:
                                        match_cols1.append(c)
                                        break
                    # Para tabela 2
                    for c in cols2:
                        c_norm = c.lower().replace('_', '').replace('-', '')
                        exemplos = exemplos_col(cur, t2, c)
                        for p in palavras:
                            if p in c_norm or c_norm in p or p in c.lower():
                                match_cols2.append(c)
                            else:
                                for ex in exemplos:
                                    if p in ex:
                                        match_cols2.append(c)
                                        break
                    st.session_state['cols1_auto'] = list(dict.fromkeys(match_cols1))
                    st.session_state['cols2_auto'] = list(dict.fromkeys(match_cols2))
                    colunas_sugeridas['t1'] = st.session_state['cols1_auto']
                    colunas_sugeridas['t2'] = st.session_state['cols2_auto']
                except Exception as e:
                    st.warning(f"Não foi possível sugerir colunas: {e}")
            else:
                st.session_state['cols1_auto'] = []
                st.session_state['cols2_auto'] = []
                colunas_sugeridas = {'t1': [], 't2': []}

        # Espaço fixo para mostrar sugestões de colunas após o botão
        if st.session_state.get('sugerir_colunas_flag', False):
            colunas_sugeridas = {
                't1': st.session_state.get('cols1_auto', []),
                't2': st.session_state.get('cols2_auto', [])
            }
            st.markdown("""
            <div style='margin-top:10px; margin-bottom:10px; padding:10px; background:#f8f9fa; border-radius:6px;'>
            <b>Colunas sugeridas:</b><br>
            <b>{t1}</b>: {cols1}<br>
            <b>{t2}</b>: {cols2}
            </div>
            """.format(
                t1=t1,
                cols1=(', '.join(colunas_sugeridas['t1']) if colunas_sugeridas['t1'] else '<i>Nenhuma</i>'),
                t2=t2,
                cols2=(', '.join(colunas_sugeridas['t2']) if colunas_sugeridas['t2'] else '<i>Nenhuma</i>')
            ), unsafe_allow_html=True)

        # Multiselect manual das colunas (sempre visível após JOIN validado)
        col1, col2 = st.columns(2)
        cols1_sel = st.session_state.get('cols1_sel', st.session_state.get('cols1_auto', []))
        cols2_sel = st.session_state.get('cols2_sel', st.session_state.get('cols2_auto', []))
        with col1:
            cols1_sel = st.multiselect(f"Colunas de {t1}", cols1, key="cols1_sel", default=st.session_state.get('cols1_auto', []))
        with col2:
            cols2_sel = st.multiselect(f"Colunas de {t2}", cols2, key="cols2_sel", default=st.session_state.get('cols2_auto', []))
        st.session_state.selected_cols[t1] = cols1_sel if cols1_sel is not None else []
        st.session_state.selected_cols[t2] = cols2_sel if cols2_sel is not None else []

    # Mostrar sugestões de JOINs
    if t1 and t2:
        suggestions = suggest_join_columns(cur, t1, t2)
        if suggestions:
            st.info("💡 **Sugestões de JOINs detectadas:**")
            for c1, c2, reason in suggestions:
                st.write(f"• `{t1}.{c1}` = `{t2}.{c2}` — {reason}")

    # Step 2: Escolher colunas para JOIN
    st.header("Step 2: Escolher Colunas para JOIN Inicial")
    cols1 = list_columns(cur, t1)
    cols2 = list_columns(cur, t2)
    col1, col2 = st.columns(2)
    with col1:
        c1 = st.selectbox(f"Coluna de {t1}", cols1, key="c1")
    with col2:
        c2 = st.selectbox(f"Coluna de {t2}", cols2, key="c2")

    # Validar JOIN
    if st.button("✓ Validar JOIN Inicial", key="validate_initial"):
        validation = validate_join(cur, t1, c1, t2, c2)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Registros em " + t1, validation['total_left'])
        with col2:
            st.metric("Registros em " + t2, validation['total_right'])
        with col3:
            st.metric("Ligações encontradas", validation['matched'])
        
        if validation['matched'] == 0:
            st.error("❌ Nenhuma ligação encontrada. Escolha outras colunas.")
            st.session_state.validated = False
        else:
            st.success("✓ JOIN válido! Você pode prosseguir.")
            st.session_state.tables = [t1, t2]
            st.session_state.join_conditions = [(t1, c1, t2, c2)]
            st.session_state.selected_cols = {}
        # Sempre exibe seleção de colunas após validação (mesmo se não validado)
        st.header("Step 3: Escolher Colunas para o Relatório")
        col1, col2 = st.columns(2)
        # Inicializa variáveis para evitar UnboundLocalError
        cols1_sel = st.session_state.get('cols1_sel', [])
        cols2_sel = st.session_state.get('cols2_sel', [])
        try:
            with col1:
                cols1_sel = st.multiselect(f"Colunas de {t1}", cols1, key="cols1_sel", default=st.session_state['cols1_auto'])
            with col2:
                cols2_sel = st.multiselect(f"Colunas de {t2}", cols2, key="cols2_sel", default=st.session_state['cols2_auto'])
        except Exception:
            pass
        st.session_state.selected_cols[t1] = cols1_sel if cols1_sel is not None else []
        st.session_state.selected_cols[t2] = cols2_sel if cols2_sel is not None else []
        st.session_state.selected_cols[t1] = cols1_sel
        st.session_state.selected_cols[t2] = cols2_sel

        # Step 4: Adicionar mais tabelas
        st.header("Step 4: Adicionar Mais Tabelas (Opcional)")
        
        if "add_table_mode" not in st.session_state:
            st.session_state.add_table_mode = False
        
        if st.button("➕ Adicionar Nova Tabela"):
            st.session_state.add_table_mode = True

        if st.session_state.add_table_mode:
            remaining = [t for t in all_tables if t not in st.session_state.tables]
            if not remaining:
                st.warning("Todas as tabelas já foram adicionadas.")
            else:
                st.write("**Adicionar Nova Tabela ao JOIN:**")
                t_new = st.selectbox("Escolha a nova tabela", remaining, key="t_new")
                t_base = st.selectbox("Tabela base para o JOIN", st.session_state.tables, key="t_base")
                
                # Mostrar sugestões
                if t_base and t_new:
                    suggestions = suggest_join_columns(cur, t_base, t_new)
                    if suggestions:
                        st.info(f"💡 **Sugestões para JOIN {t_base} → {t_new}:**")
                        for c1, c2, reason in suggestions:
                            st.write(f"• `{t_base}.{c1}` = `{t_new}.{c2}` — {reason}")
                
                cols_base = list_columns(cur, t_base)
                c_base = st.selectbox(f"Coluna de {t_base}", cols_base, key="c_base")
                
                cols_new = list_columns(cur, t_new)
                c_new = st.selectbox(f"Coluna de {t_new}", cols_new, key="c_new")

                if st.button("✓ Validar e Adicionar Tabela"):
                    validation_new = validate_join(cur, t_base, c_base, t_new, c_new)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Registros em " + t_base, validation_new['total_left'])
                    with col2:
                        st.metric("Registros em " + t_new, validation_new['total_right'])
                    with col3:
                        st.metric("Ligações encontradas", validation_new['matched'])
                    
                    if validation_new['matched'] > 0:
                        st.session_state.tables.append(t_new)
                        st.session_state.join_conditions.append((t_base, c_base, t_new, c_new))
                        
                        cols_new_sel = st.multiselect(f"Colunas de {t_new}", cols_new, key=f"cols_{t_new}")
                        st.session_state.selected_cols[t_new] = cols_new_sel
                        
                        st.success(f"✓ Tabela {t_new} adicionada!")
                        st.session_state.add_table_mode = False
                    else:
                        st.error("❌ Nenhuma ligação. Tente outras colunas.")

        # Step 5: Gerar relatório
        st.header("Step 5: Gerar Relatório")
        
        if st.button("🚀 Executar Consulta e Gerar Relatório"):
            try:
                # Montar SQL
                table_aliases = {t: f"t{i+1}" for i, t in enumerate(st.session_state.tables)}
                select_parts = []
                for t in st.session_state.tables:
                    alias = table_aliases[t]
                    cols = st.session_state.selected_cols.get(t, [])
                    for col in cols:
                        select_parts.append(f"`{alias}`.`{col}` AS `{t}.{col}`")
                
                if not select_parts:
                    st.warning("Selecione ao menos uma coluna para incluir no relatório.")
                else:
                    from_parts = [f"`{st.session_state.tables[0]}` AS {table_aliases[st.session_state.tables[0]]}"]
                    for t_left, c_left, t_right, c_right in st.session_state.join_conditions:
                        alias_left = table_aliases[t_left]
                        alias_right = table_aliases[t_right]
                        from_parts.append(f"JOIN `{t_right}` AS {alias_right} ON {alias_left}.`{c_left}` = {alias_right}.`{c_right}`")
                    
                    sql = f"SELECT {', '.join(select_parts)} FROM {' '.join(from_parts)}"
                    
                    with st.expander("📋 Ver SQL gerado"):
                        st.code(sql, language="sql")
                    
                    df = pd.read_sql(sql, conn)
                    st.success(f"✓ Consulta executada: **{len(df)}** linhas retornadas")
                    
                    if len(df) > 1000000:
                        st.error("⚠️ Resultado muito grande! Possível cross join.")
                    else:
                        # Mostrar dados
                        st.subheader("Primeiras 100 linhas:")
                        st.dataframe(df.head(100), use_container_width=True)
                        
                        # Downloads
                        st.subheader("Baixar Resultados:")
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            csv = df.to_csv(index=False)
                            st.download_button("📥 CSV", csv, "resultado.csv", "text/csv")
                        
                        with col2:
                            buffer = BytesIO()
                            df.to_excel(buffer, index=False, engine='openpyxl')
                            buffer.seek(0)
                            st.download_button("📥 Excel", buffer.getvalue(), "resultado.xlsx", 
                                             "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            except Exception as e:
                st.error(f"Erro ao executar consulta: {e}")

    conn.close()


if __name__ == "__main__":
    main()
