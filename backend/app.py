# backend/app.py
from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
import pandas as pd
import google.generativeai as genai
import os
from dotenv import load_dotenv
import tempfile

# Load env variables
load_dotenv()

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Setup Gemini
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
model = genai.GenerativeModel(model_name="models/gemini-1.5-flash")

@app.route("/upload", methods=["POST"])
def handle_csv():
    if 'file' not in request.files or 'question' not in request.form:
        return jsonify({"error": "CSV file and question required"}), 400

    csv_file = request.files['file']
    question = request.form['question']
    database_type = request.form.get('database_type')
    # Save CSV to a temp file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp_db:
        db_path = tmp_db.name

    # Load CSV to 
    df = pd.read_csv(csv_file)
    table_name = "data"
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, index=False, if_exists="replace")

    # Extract schema
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cursor.fetchall()]
    schema = f"Table: {table_name}({', '.join(columns)})"

    # Gemini prompt
    prompt = f"""
    You are an expert ***{database_type}*** developer.
Given the table schema below, generate a ***{database_type}*** query for the question.
Only output the ***{database_type}*** query — no explanation or markdown.

    {schema}
    Question: {question}
    database type: {database_type}
    query:
    """

    try:
       response = model.generate_content(prompt)
       if response.text is None:
            
            return jsonify({"error": "Gemini did not return any SQL. Please try a different question.", "sql": ""})
       

       sql_query = response.text.replace("```query", "").replace("```", "").strip()


       prompt2 = f"""
      You are an expert SQL developer.
Convert the following ***{database_type}*** SQL query to a valid **SQLite-compatible SQL query** using the given schema.
Only output the final SQLite SQL query — no extra explanation, no markdown
       query:{sql_query}
       Schema:{schema}
       """
       sql_response =model.generate_content(prompt2)
       sql_response_final = sql_response.text.replace("```query", "").replace("```", "").strip()


        # Execute the generated SQL
       cursor.execute(sql_response_final)
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
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port)

