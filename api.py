import os
import uuid
import shutil
import logging
from pathlib import Path
from flask import Flask, request, jsonify, send_from_directory, after_this_request
from flask_cors import CORS
from werkzeug.utils import secure_filename
import analysis_engine

app = Flask(__name__)
CORS(app)

UPLOAD_FOLDER = Path('/tmp/uploads')
RESULTS_FOLDER = Path('/tmp/results')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB

@app.route('/api/analyze', methods=['POST'])
def analyze_files():
    logging.info("="*50)
    logging.info("Nova requisição recebida em /api/analyze")
    
    # Verifica se a chave 'files' está na requisição
    if 'files' not in request.files:
        logging.error("A requisição chegou, mas a chave 'files' não foi encontrada em request.files.")
        return jsonify({"error": "Estrutura da requisição inválida: chave 'files' ausente."}), 400

    files = request.files.getlist('files')
    logging.info(f"request.files.getlist('files') retornou uma lista com {len(files)} arquivos.")

    if not files or (len(files) == 1 and files[0].filename == ''):
        logging.error("A lista de arquivos está vazia. O frontend pode não ter enviado os arquivos corretamente.")
        return jsonify({"error": "Nenhum arquivo selecionado foi recebido pelo servidor."}), 400
        
    numeros_raw = request.form.get('numerosParaCopiar', '')
    
    job_id = str(uuid.uuid4())
    source_path = UPLOAD_FOLDER / job_id
    dest_path = RESULTS_FOLDER / job_id
    os.makedirs(source_path, exist_ok=True)

    try:
        logging.info(f"Tentando salvar {len(files)} arquivos na pasta {source_path}...")
        file_count = 0
        for file in files:
            if file and file.filename:
                filename = secure_filename(file.filename)
                # Converte o caminho para string para máxima compatibilidade
                save_path = os.path.join(str(source_path), filename)
                file.save(save_path)
                file_count += 1
        logging.info(f"{file_count} arquivos foram salvos com sucesso.")

        if file_count == 0:
            raise ValueError("Apesar de receber a requisição, nenhum arquivo válido foi salvo.")
            
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
        logging.error(f"Erro no job {job_id}: {e}", exc_info=True)
        return jsonify({"error": f"Ocorreu um erro interno durante a análise."}), 500
    finally:
        if source_path.exists():
            shutil.rmtree(source_path)

@app.route('/api/download/<job_id>/<filename>', methods=['GET'])
def download_file(job_id, filename):
    directory = RESULTS_FOLDER / secure_filename(job_id)
    try:
        @after_this_request
        def cleanup(response):
            # Limpa a pasta de resultados após o download ser concluído
            if directory.exists():
                shutil.rmtree(directory)
            return response
        return send_from_directory(directory, filename, as_attachment=True)
    except FileNotFoundError:
        return jsonify({"error": "Arquivo não encontrado. O processo pode ter expirado."}), 404

@app.route('/', methods=['GET'])
def index():
    return "API do Analisador de XML está no ar."

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False)