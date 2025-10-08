import os
import uuid
import shutil
import logging
from pathlib import Path
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from werkzeug.utils import secure_filename
import analysis_engine

app = Flask(__name__)
CORS(app)

# ??? CORREÇÃO APLICADA AQUI ???
# Em ambientes de nuvem, devemos usar o diretório /tmp para escrita de arquivos temporários.
UPLOAD_FOLDER = Path('/tmp/uploads')
RESULTS_FOLDER = Path('/tmp/results')
# ??? FIM DA CORREÇÃO ???

# Garante que as pastas base existem ao iniciar a aplicação
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)

# Define o limite máximo de upload (opcional, mas bom ter)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB

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
        # Adiciona logging para vermos o erro exato no log do Render
        logging.error(f"Erro no job {job_id}: {e}", exc_info=True)
        return jsonify({"error": f"Ocorreu um erro interno durante a análise."}), 500
    finally:
        # Limpa as pastas temporárias
        if source_path.exists():
            shutil.rmtree(source_path)
        # A pasta de resultados é mantida até o download ser solicitado

@app.route('/api/download/<job_id>/<filename>', methods=['GET'])
def download_file(job_id, filename):
    directory = RESULTS_FOLDER / secure_filename(job_id)
    try:
        # Após o download, podemos limpar a pasta de resultados
        @after_this_request
        def cleanup(response):
            if directory.exists():
                shutil.rmtree(directory)
            return response
        return send_from_directory(directory, filename, as_attachment=True)
    except FileNotFoundError:
        return jsonify({"error": "Arquivo não encontrado. O processo pode ter expirado."}), 404

# Rota para verificar se a API está no ar
@app.route('/', methods=['GET'])
def index():
    return "API do Analisador de XML está no ar."

if __name__ == '__main__':
    # Para desenvolvimento local, o Render ignora isso e usa o Gunicorn
    app.run(host='0.0.0.0', port=5001, debug=False)