# backend/app.py
from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
import pandas as pd
import google.generativeai as genai
import os
from dotenv import load_dotenv
import tempfile
import io
import chardet
import csv 


# Increase max CSV field size to avoid buffer overflow
csv.field_size_limit(2**31 - 1)

# Load env variables
load_dotenv()

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Setup Gemini
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
model = genai.GenerativeModel(model_name="models/gemini-1.5-flash")

@app.route("/upload", methods=["POST"])
def handle_csv():
    if 'files' not in request.files or 'question' not in request.form:
        return jsonify({"error": "CSV files and question required"}), 400

    files = request.files.getlist('files')
    question = request.form['question']
    database_type = request.form.get('database_type', 'SQL')

    # Save to temporary SQLite DB
    with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp_db:
        db_path = tmp_db.name

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    schema_strings = []

    try:
        # Load each CSV as a separate table
        for file in files:
            if file.filename.endswith('.csv'):
                raw_bytes = file.read()
                detected = chardet.detect(raw_bytes)
                encoding = detected['encoding'] or 'utf-8'
                print(f"Detected encoding for {file.filename}: {encoding}")

                # Decode bytes
                decoded = raw_bytes.decode(encoding, errors='replace')

                # Try detecting delimiter
                try:
                    sample = decoded[:2048]
                    dialect = csv.Sniffer().sniff(sample)
                    delimiter = dialect.delimiter
                except Exception:
                    delimiter = ','  # fallback

                # Read CSV using detected encoding + delimiter
                try:
                    df = pd.read_csv(io.StringIO(decoded), on_bad_lines='skip', delimiter=delimiter)
                except Exception:
                    df = pd.read_csv(io.StringIO(decoded), on_bad_lines='skip', delimiter=delimiter, engine='python')

                table_name = os.path.splitext(file.filename)[0].replace(" ", "_")
                df.to_sql(table_name, conn, index=False, if_exists='replace')

                # Generate schema for Gemini
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [col[1] for col in cursor.fetchall()]
                schema_strings.append(f"Table: {table_name}\nColumns:\n" + "\n".join([f"- {col}" for col in columns]))

        full_schema = "\n".join(schema_strings)

        # Prompt Gemini for query
        prompt = f"""
You are an expert ***{database_type}*** developer.
Given the table schemas below, extract exact data schemas like rows and columns strictly as mentioned in csv file generate a ***{database_type}*** query based on the schema to answer the question.
Only output the ***{database_type}*** query — no explanation or markdown.
*******Do not assume or guess column/table names. Use names exactly as in schema.************
{full_schema}
Question: {question}
Query:
"""
        
        response = model.generate_content(prompt)
        sql_query = response.text.strip().replace("```sql", "").replace("```", "").strip()

        # Prompt Gemini for SQLite-compatible version
        prompt2 = f"""
You are an expert SQL developer.
Convert the following ***{database_type}*** SQL query into a valid **SQLite-compatible SQL query** using the given schema.
Only output the final SQLite SQL query — no explanation, no markdown.

Query: {sql_query}
Schema:
{full_schema}
"""
        sql_response = model.generate_content(prompt2)
        sql_final = sql_response.text.strip().replace("```sql", "").replace("```", "").strip()

        # Execute the SQLite query
        cursor.execute(sql_final)
        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        result = [dict(zip(headers, row)) for row in rows]

        return jsonify({
            "sql": sql_query,
            "result": result
        })

    except Exception as e:
        return jsonify({"sql": sql_query if 'sql_query' in locals() else None, "error": str(e)})

    finally:
        conn.close()
        os.remove(db_path)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
