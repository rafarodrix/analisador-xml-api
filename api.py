import os
import uuid
import shutil
import logging
import zipfile
from pathlib import Path
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from werkzeug.utils import secure_filename
import analysis_engine

# Nota: O import 'after_this_request' não é mais necessário
# from flask import after_this_request 

app = Flask(__name__)
CORS(app)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100 MB

# As pastas de resultados precisam persistir entre as requisições
RESULTS_FOLDER = Path('/tmp/results')
os.makedirs(RESULTS_FOLDER, exist_ok=True)


@app.route('/api/analyze', methods=['POST'])
def analyze_files():
    job_id = str(uuid.uuid4())
    result_dir = RESULTS_FOLDER / job_id
    result_dir.mkdir(parents=True, exist_ok=True)
    
    xml_files_in_memory = {}

    try:
        # Lógica para popular o dicionário em memória
        if 'file' in request.files: # Upload de ZIP
            zip_file = request.files['file']
            if not zip_file or not zip_file.filename.lower().endswith('.zip'):
                return jsonify({"error": "Envie um arquivo .zip válido."}), 400
            
            with zipfile.ZipFile(zip_file, 'r') as zf:
                for filename in zf.namelist():
                    if not filename.endswith('/') and '__MACOSX' not in filename and filename.lower().endswith('.xml'):
                        xml_files_in_memory[os.path.basename(filename)] = zf.read(filename)

        elif 'files' in request.files: # Upload de pasta
            files = request.files.getlist('files')
            for file in files:
                if file and file.filename and file.filename.lower().endswith('.xml'):
                    xml_files_in_memory[secure_filename(file.filename)] = file.read()
        else:
            return jsonify({"error": "Nenhum arquivo enviado."}), 400

        numeros_raw = request.form.get('numerosParaCopiar', '')
        numeros_para_copiar = analysis_engine.parse_numeros(numeros_raw)
        
        result_paths = analysis_engine.run_analysis(xml_files_in_memory, result_dir, numeros_para_copiar)

        with open(result_paths["summary_path"], 'r', encoding='utf-8') as f:
            summary_content = f.read()

        result_zip = result_paths['zip_path']
        
        return jsonify({
            "jobId": job_id,
            "summary": summary_content,
            "downloadUrl": f"/api/download/{job_id}/{result_zip.name}",
        })

    except ValueError as ve:
        logging.warning(f"Erro do usuário no job {job_id}: {ve}")
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        logging.exception(f"Erro interno no job {job_id}")
        return jsonify({"error": "Erro interno no servidor durante a análise."}), 500


@app.route('/api/download/<job_id>/<filename>', methods=['GET'])
def download_file(job_id, filename):
    directory = RESULTS_FOLDER / secure_filename(job_id)
    try:
        # ??? CORREÇÃO APLICADA AQUI ???
        # O bloco @after_this_request foi removido para evitar que o arquivo
        # seja deletado antes do download terminar.
        # ??? FIM DA CORREÇÃO ???
        return send_from_directory(directory, filename, as_attachment=True)
    except FileNotFoundError:
        return jsonify({"error": "Arquivo não encontrado ou expirado."}), 404


@app.route('/', methods=['GET'])
def index():
    return jsonify({"status": "API do Analisador de XML está no ar."})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False)