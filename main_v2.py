from __future__ import annotations

from pathlib import Path
from datetime import datetime
import mysql.connector
import pandas as pd


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
# LOG
# =========================
log_lines: list[str] = []

def log(msg: str) -> None:
    print(msg)
    log_lines.append(str(msg))

def save_log(path: Path) -> None:
    path.write_text("\n".join(log_lines), encoding="utf-8")


# =========================
# INPUT HELPERS
# =========================
def pedir_numero(msg: str, minimo: int, maximo: int) -> int:
    while True:
        s = input(msg).strip()
        if not s.isdigit():
            print("Digite um número.")
            continue
        n = int(s)
        if minimo <= n <= maximo:
            return n
        print(f"Digite um número entre {minimo} e {maximo}.")

def pedir_lista_numeros(msg: str, minimo: int, maximo: int) -> list[int]:
    """
    Aceita: 1,2,3  |  1 2 3  |  1;2;3
    """
    while True:
        raw = input(msg).strip()
        if not raw:
            print("Informe ao menos 1 número.")
            continue
        raw = raw.replace(";", ",").replace(" ", ",")
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        if not parts:
            print("Formato inválido.")
            continue

        nums = []
        ok = True
        for p in parts:
            if not p.isdigit():
                ok = False
                break
            n = int(p)
            if not (minimo <= n <= maximo):
                ok = False
                break
            nums.append(n)

        if ok:
            # remove duplicados preservando ordem
            seen = set()
            out = []
            for n in nums:
                if n not in seen:
                    out.append(n)
                    seen.add(n)
            return out

        print(f"Use números entre {minimo} e {maximo}. Ex: 1,2,5")

def escolher_tabela(tables: list[str], msg: str) -> str:
    while True:
        filtro = input(f"{msg} (digite trecho para filtrar, ou vazio para todas): ").strip().lower()
        filtradas = [t for t in tables if filtro in t.lower()] if filtro else tables
        if not filtradas:
            print("Nenhuma tabela encontrada com esse filtro. Tente novamente.")
            continue
        print("Tabelas encontradas (ordem alfabética):")
        for i, t in enumerate(filtradas, start=1):
            print(f"{i:>3}) {t}")
        num = pedir_numero("Escolha o número: ", 1, len(filtradas))
        return filtradas[num - 1]

def escolher_coluna(cols: list[str], table: str, msg: str) -> str:
    while True:
        filtro = input(f"{msg} para {table} (digite trecho para filtrar, ou vazio para todas): ").strip().lower()
        filtradas = [c for c in cols if filtro in c.lower()] if filtro else cols
        if not filtradas:
            print("Nenhuma coluna encontrada com esse filtro. Tente novamente.")
            continue
        print("Colunas encontradas (ordem alfabética):")
        for i, c in enumerate(filtradas, start=1):
            print(f"{i:>3}) {c}")
        num = pedir_numero("Escolha o número: ", 1, len(filtradas))
        return filtradas[num - 1]

def escolher_coluna_anterior(cur, table: str, tables_anteriores: list[str]) -> tuple[str, str]:
    """Permite ao usuário escolher qual coluna de qual tabela anterior usar."""
    print(f"\nEscolha a coluna da tabela base ({table}) que será a chave da junção:")
    cols = list_columns(cur, table)
    while True:
        filtro = input(f"Digite trecho para filtrar, ou vazio para todas: ").strip().lower()
        filtradas = [c for c in cols if filtro in c.lower()] if filtro else cols
        if not filtradas:
            print("Nenhuma coluna encontrada. Tente novamente.")
            continue
        print("Colunas encontradas:")
        for i, c in enumerate(filtradas, start=1):
            print(f"{i:>3}) {c}")
        num = pedir_numero("Escolha o número: ", 1, len(filtradas))
        return table, filtradas[num - 1]

def escolher_colunas(cols: list[str], table: str, msg: str) -> list[str]:
    """Escolhe múltiplas colunas para incluir no relatório (mantém ordem original)."""
    while True:
        filtro = input(f"{msg} para {table} (digite trecho para filtrar, ou vazio para todas): ").strip().lower()
        filtradas = [c for c in cols if filtro in c.lower()] if filtro else cols
        if not filtradas:
            print("Nenhuma coluna encontrada com esse filtro. Tente novamente.")
            continue
        print("Colunas encontradas (ordem original):")
        for i, c in enumerate(filtradas, start=1):
            print(f"{i:>3}) {c}")
        nums = pedir_lista_numeros("Digite os números separados por vírgula: ", 1, len(filtradas))
        return [filtradas[i-1] for i in nums]


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
    base_dir = Path(__file__).resolve().parent
    xlsx_path = base_dir / OUT_XLSX
    log_path = base_dir / OUT_LOG
    concat_log_path = base_dir / OUT_CONCAT_LOG

    log("=== ASSISTENTE DE JOIN (MÚLTIPLAS TABELAS) ===")
    log(f"Data/hora: {datetime.now().isoformat(sep=' ', timespec='seconds')}")
    log(f"Saída XLSX: {xlsx_path}")
    log(f"Log: {log_path}")
    log(f"Log de Concatenação: {concat_log_path}")
    log("")

    conn = mysql.connector.connect(**DB)
    concat_log_lines: list[str] = []
    
    try:
        cur = conn.cursor()

        # 1) listar tabelas
        all_tables = list_tables(cur)
        if not all_tables:
            log("Nenhuma tabela encontrada.")
            return

        # 2) escolher tabela 1 e 2
        t1 = escolher_tabela(all_tables, "Escolha a TABELA 1")
        t2 = escolher_tabela(all_tables, "Escolha a TABELA 2")

        log("")
        log(f"Tabela 1 escolhida: {t1}")
        log(f"Tabela 2 escolhida: {t2}")
        log("")

        # 3) escolher coluna de comparação tabela 1
        cols1 = list_columns(cur, t1)

        # 4) escolher coluna de comparação tabela 2
        cols2 = list_columns(cur, t2)

        while True:
            c1 = escolher_coluna(cols1, t1, "Qual coluna de comparação da TABELA 1")
            c2 = escolher_coluna(cols2, t2, "Qual coluna de comparação da TABELA 2")

            # Validar primeiro JOIN
            validation_1_2 = validate_join(cur, t1, c1, t2, c2)
            print(f"\nValidação do JOIN {t1}.{c1} = {t2}.{c2}:")
            print(f"  Registros em {t1}: {validation_1_2['total_left']}")
            print(f"  Registros em {t2}: {validation_1_2['total_right']}")
            print(f"  Ligações encontradas: {validation_1_2['matched']}")

            if validation_1_2['matched'] == 0:
                voltar = input("A intersecção resultou em zero. Deseja voltar ao passo anterior para escolher outras colunas? (s/n): ").strip().lower()
                if voltar == 's':
                    continue
                else:
                    break
            else:
                break

        log("")
        log(f"Chave de JOIN inicial: {t1}.{c1} = {t2}.{c2}")
        
        if validation_1_2['matched'] > 0:
            log(f"\nValidação do JOIN {t1}.{c1} = {t2}.{c2}:")
            log(f"  Registros em {t1}: {validation_1_2['total_left']}")
            log(f"  Registros em {t2}: {validation_1_2['total_right']}")
            log(f"  Ligações encontradas: {validation_1_2['matched']}")
            
            concat_log_lines.append(f"=== JOIN: {t1}.{c1} = {t2}.{c2} ===")
            concat_log_lines.append(f"Registros em {t1}: {validation_1_2['total_left']}")
            concat_log_lines.append(f"Registros em {t2}: {validation_1_2['total_right']}")
            concat_log_lines.append(f"Ligações encontradas: {validation_1_2['matched']}")
            
            if validation_1_2['unmatched_left']:
                concat_log_lines.append(f"\nValores em {t1}.{c1} sem correspondência em {t2} (amostra):")
                for val in validation_1_2['unmatched_left'][:20]:
                    concat_log_lines.append(f"  - {val}")
            
            if validation_1_2['unmatched_right']:
                concat_log_lines.append(f"\nValores em {t2}.{c2} sem correspondência em {t1} (amostra):")
                for val in validation_1_2['unmatched_right'][:20]:
                    concat_log_lines.append(f"  - {val}")
            concat_log_lines.append("")
        
        log("")

        # 5) escolher colunas para o relatório (tabela 1)
        cols1_sel = escolher_colunas(cols1, t1, "Escolha as colunas do relatório da TABELA 1")

        # 6) escolher colunas para o relatório (tabela 2)
        cols2_sel = escolher_colunas(cols2, t2, "Escolha as colunas do relatório da TABELA 2")

        # Inicializar estruturas
        tables = [t1, t2]
        table_aliases = {t1: "t1", t2: "t2"}
        selected_cols = {t1: cols1_sel, t2: cols2_sel}
        
        # Estrutura de junções: lista de tuplas (tabela_esquerda, coluna_esquerda, tabela_direita, coluna_direita)
        join_conditions = [(t1, c1, t2, c2)]
        join_validations = [(t1, c1, t2, c2, validation_1_2)]

        # Loop para adicionar mais tabelas
        while True:
            resp = input("\nDeseja adicionar outra tabela ao JOIN? (s/n): ").strip().lower()
            if resp != 's':
                break

            # Escolher nova tabela
            t_new = escolher_tabela(all_tables, "Escolha a nova TABELA")

            # Escolher com qual tabela anterior fazer o JOIN
            print("\nTabelas adicionadas até agora:")
            for i, t in enumerate(tables, start=1):
                print(f"{i:>3}) {t}")
            table_idx = pedir_numero("Escolha a tabela base para a junção: ", 1, len(tables))
            t_base = tables[table_idx - 1]

            # Escolher coluna da tabela base
            cols_base = list_columns(cur, t_base)
            print(f"\nEscolha a coluna de {t_base} que será a chave da junção:")
            while True:
                filtro = input("Digite trecho para filtrar, ou vazio para todas: ").strip().lower()
                filtradas_base = [c for c in cols_base if filtro in c.lower()] if filtro else cols_base
                if not filtradas_base:
                    print("Nenhuma coluna encontrada. Tente novamente.")
                    continue
                print("Colunas encontradas:")
                for i, c in enumerate(filtradas_base, start=1):
                    print(f"{i:>3}) {c}")
                col_idx = pedir_numero("Escolha o número: ", 1, len(filtradas_base))
                c_base = filtradas_base[col_idx - 1]
                break

            # Escolher coluna da nova tabela
            cols_new = list_columns(cur, t_new)
            while True:
                c_new = escolher_coluna(cols_new, t_new, f"Qual coluna de {t_new} se iguala a {t_base}.{c_base}")

                # Validar novo JOIN
                validation_new = validate_join(cur, t_base, c_base, t_new, c_new)
                print(f"\nValidação do JOIN {t_base}.{c_base} = {t_new}.{c_new}:")
                print(f"  Registros em {t_base}: {validation_new['total_left']}")
                print(f"  Registros em {t_new}: {validation_new['total_right']}")
                print(f"  Ligações encontradas: {validation_new['matched']}")

                if validation_new['matched'] == 0:
                    voltar = input("A intersecção resultou em zero. Deseja voltar ao passo anterior para escolher outra coluna? (s/n): ").strip().lower()
                    if voltar == 's':
                        continue
                    else:
                        break
                else:
                    break

            if validation_new['matched'] > 0:
                log(f"\nValidação do JOIN {t_base}.{c_base} = {t_new}.{c_new}:")
                log(f"  Registros em {t_base}: {validation_new['total_left']}")
                log(f"  Registros em {t_new}: {validation_new['total_right']}")
                log(f"  Ligações encontradas: {validation_new['matched']}")
                
                concat_log_lines.append(f"=== JOIN: {t_base}.{c_base} = {t_new}.{c_new} ===")
                concat_log_lines.append(f"Registros em {t_base}: {validation_new['total_left']}")
                concat_log_lines.append(f"Registros em {t_new}: {validation_new['total_right']}")
                concat_log_lines.append(f"Ligações encontradas: {validation_new['matched']}")
                
                if validation_new['unmatched_left']:
                    concat_log_lines.append(f"\nValores em {t_base}.{c_base} sem correspondência em {t_new} (amostra):")
                    for val in validation_new['unmatched_left'][:20]:
                        concat_log_lines.append(f"  - {val}")
                
                if validation_new['unmatched_right']:
                    concat_log_lines.append(f"\nValores em {t_new}.{c_new} sem correspondência em {t_base} (amostra):")
                    for val in validation_new['unmatched_right'][:20]:
                        concat_log_lines.append(f"  - {val}")
                concat_log_lines.append("")

                # Escolher colunas da nova tabela para incluir
                cols_new_sel = escolher_colunas(cols_new, t_new, f"Escolha as colunas do relatório da TABELA {t_new}")

                # Adicionar às estruturas
                tables.append(t_new)
                alias_num = len(tables)
                table_aliases[t_new] = f"t{alias_num}"
                selected_cols[t_new] = cols_new_sel
                join_conditions.append((t_base, c_base, t_new, c_new))
                join_validations.append((t_base, c_base, t_new, c_new, validation_new))

                log(f"\nTabela {t_new} adicionada com JOIN em {t_base}.{c_base} = {t_new}.{c_new}")
                log(f"Colunas selecionadas: {cols_new_sel}")

        log("")
        log(f"Total de tabelas no JOIN: {len(tables)}")
        for i, t in enumerate(tables, start=1):
            log(f"  {i}) {t} -> colunas: {selected_cols[t]}")
        log("")
        log("Condições de JOIN:")
        for t_left, c_left, t_right, c_right in join_conditions:
            log(f"  {t_left}.{c_left} = {t_right}.{c_right}")
        log("")

        # 7) montar SELECT com aliases
        # A coluna de interseção é a primeira coluna do primeiro JOIN
        t_intersecao, c_intersecao = join_conditions[0][0], join_conditions[0][1]
        select_parts = [f"`{table_aliases[t_intersecao]}`.`{c_intersecao}` AS `intersecao`"]

        for t in tables:
            alias = table_aliases[t]
            for col in selected_cols[t]:
                select_parts.append(f"`{alias}`.`{col}` AS `{t}.{col}`")

        # Montar FROM e JOINs baseado em join_conditions
        from_parts = [f"`{tables[0]}` AS {table_aliases[tables[0]]}"]
        
        for t_left, c_left, t_right, c_right in join_conditions:
            alias_left = table_aliases[t_left]
            alias_right = table_aliases[t_right]
            from_parts.append(f"JOIN `{t_right}` AS {alias_right} ON {alias_left}.`{c_left}` = {alias_right}.`{c_right}`")

        sql = f"""
        SELECT
            {", ".join(select_parts)}
        FROM {" ".join(from_parts)}
        """

        log("SQL gerado:")
        log(sql.strip())
        log("")

        # 8) executar e exportar
        df = pd.read_sql(sql, conn)
        log(f"Linhas retornadas pelo JOIN: {len(df)}")
        
        # Proteção contra cross join anormal
        if len(df) > 1000000:
            log("")
            log("⚠️ AVISO: Resultado muito grande detectado!")
            log(f"A consulta retornou {len(df)} linhas, o que sugere um produto cartesiano (cross join).")
            log("Possíveis causas:")
            log("- As colunas de junção não contêm valores correspondentes")
            log("- Uma ou mais colunas de junção contêm NULLs")
            log("- As chaves não foram especificadas corretamente")
            log("")
            log("XLSX não foi gerado para proteger sua máquina.")
            log("Verifique o concatenalog.txt para ver as incompatibilidades encontradas.")
            concat_log_lines.append("\n⚠️ ERRO: Produto cartesiano detectado!")
            concat_log_lines.append(f"A consulta gerou {len(df):,} linhas.")
            concat_log_lines.append("Verifique as validações de JOIN acima para identificar o problema.")
        else:
            log(f"Colunas no XLSX: {list(df.columns)}")
            df.to_excel(xlsx_path, index=False)
            log("XLSX gerado com sucesso.")

    finally:
        try:
            conn.close()
        except Exception:
            pass

        save_log(log_path)
        
        # Salvar concatenalog
        concat_log_content = "\n".join(concat_log_lines)
        Path(concat_log_path).write_text(concat_log_content, encoding="utf-8")
        
        print(f"\nLog gerado em: {log_path}")
        print(f"Log de concatenação gerado em: {concat_log_path}")


if __name__ == "__main__":
    main()

