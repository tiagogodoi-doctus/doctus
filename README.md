# consultasql

A Python project for SQL queries with an interactive **web interface** for joining multiple MySQL tables and exporting results to Excel or CSV.

## Features

- 🌐 **Web interface** built with Streamlit - clean and intuitive
- 💡 **Smart JOIN suggestions** - automatically detects column patterns and suggests joins
- 🔗 Supports flexible JOIN of multiple tables (2 or more)
- Each table can join with ANY previously selected table on ANY column
- 📊 Select columns for each table independently
- ✓ Validates JOIN conditions before execution
- 🔄 Generates dynamic SQL JOIN queries automatically
- 📥 Exports results to **XLSX** and **CSV** formats
- 📋 Real-time validation with match counts

## Quick Start

### Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

### Running the Web App

```bash
streamlit run streamlit_app.py
```

Then open your browser:
```
http://localhost:8501
```

## Usage Steps

1. **Select Tables**: Choose the two tables you want to join
2. **Smart Suggestions**: The app will suggest possible JOIN columns based on:
   - Identical column names
   - Common naming patterns (IDs, codes, etc.)
3. **Validate JOIN**: Check the match count before proceeding
4. **Select Columns**: Choose which columns to include in the report
5. **Add More Tables** (optional): Extend your JOIN with additional tables
6. **Generate Report**: Execute the query and download results in CSV or Excel

## Example

If you have tables `produtos`, `derivacaoproduto`, and `produtofornecedor`:
- Initial JOIN: `produtos.Id = derivacaoproduto.CodPro`
- Add third table: `produtos.codigobarras = produtofornecedor.codigobarrafornecedor`

The app automatically suggests these JOINs and handles the SQL generation.

## Configuration

Edit the `DB` dictionary in `streamlit_app.py` to match your MySQL database settings:

```python
DB = dict(
    host="localhost",
    user="root",
    password="123456",
    database="_m1",
)
```

## File Structure

- `streamlit_app.py` - **Main web application** (use this!)
- `main_cli.py` - Original command-line version (optional, for advanced users)
- `fix_asyncio.py` - Fixes for Python 3.13 + Windows compatibility
- `.streamlit/config.toml` - Streamlit configuration

## Troubleshooting

**App not starting?**
```bash
pip install --upgrade streamlit
```

**Database connection issues?**
- Verify your MySQL server is running
- Check credentials in `streamlit_app.py`
- Ensure database exists