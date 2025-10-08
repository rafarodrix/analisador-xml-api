import os
import uuid
import shutil
from pathlib import Path
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from werkzeug.utils import secure_filename
import analysis_engine

app = Flask(__name__)
CORS(app)

# Define pastas para uploads e resultados (o Render usa um sistema de arquivos efêmero)
UPLOAD_FOLDER = Path('/tmp/uploads')
RESULTS_FOLDER = Path('/tmp/results')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)

@app.route('/api/analyze', methods=['POST'])
def analyze_files():
    if 'files' not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado"}), 400

    files = request.files.getlist('files')
    numeros_raw = request.form.get('numerosParaCopiar', '')
    
    job_id = str(uuid.uuid4())
    source_path = UPLOAD_FOLDER / job_id
    dest_path = RESULTS_FOLDER / job_id
    os.makedirs(source_path, exist_ok=True)

    try:
        for file in files:
            file.save(source_path / secure_filename(file.filename))
            
        numeros_para_copiar = analysis_engine.parse_numeros(numeros_raw)
        result_paths = analysis_engine.run_analysis(source_path, dest_path, numeros_para_copiar)

        with open(result_paths["summary_path"], 'r', encoding='utf-8') as f:
            summary_content = f.read()

        zip_path = result_paths['zip_path']
        
        return jsonify({
            "summary": summary_content,
            "downloadUrl": f"/api/download/{job_id}/{zip_path.name}",
        })
    except Exception as e:
        return jsonify({"error": f"Ocorreu um erro durante a análise: {str(e)}"}), 500
    finally:
        # Limpa as pastas temporárias para não ocupar espaço no servidor
        if source_path.exists():
            shutil.rmtree(source_path)
        if dest_path.exists():
            shutil.rmtree(dest_path)


@app.route('/api/download/<job_id>/<filename>', methods=['GET'])
def download_file(job_id, filename):
    directory = RESULTS_FOLDER / secure_filename(job_id)
    # Adicionando um bloco try-except para depuração
    try:
        return send_from_directory(directory, filename, as_attachment=True)
    except FileNotFoundError:
        return jsonify({"error": "Arquivo não encontrado. Pode ter sido limpo pelo servidor."}), 404


# Adiciona uma rota raiz para verificar se a API está no ar
@app.route('/', methods=['GET'])
def index():
    return "API do Analisador de XML está no ar."

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False)