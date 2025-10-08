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
    # ??? NOVOS LOGS PARA DEPURAÇÃO ???
    logging.info("="*50)
    logging.info("Nova requisição recebida em /api/analyze")
    logging.info(f"Cabeçalhos da Requisição: {request.headers}")
    logging.info(f"Dados do Formulário (request.form): {request.form}")
    logging.info(f"Chaves dos Arquivos (request.files.keys()): {list(request.files.keys())}")
    # ??? FIM DOS NOVOS LOGS ???

    if 'files' not in request.files:
        logging.error("Chave 'files' não encontrada na requisição.")
        return jsonify({"error": "Estrutura da requisição inválida: chave 'files' ausente."}), 400

    files = request.files.getlist('files')
    # NOVO LOG para verificar se a lista de arquivos está vazia
    logging.info(f"Número de arquivos recebidos na lista 'files': {len(files)}")
    if not files or files[0].filename == '':
        logging.error("A lista de arquivos recebida está vazia.")
        return jsonify({"error": "Nenhum arquivo selecionado foi recebido pelo servidor."}), 400
        
    numeros_raw = request.form.get('numerosParaCopiar', '')
    
    job_id = str(uuid.uuid4())
    source_path = UPLOAD_FOLDER / job_id
    dest_path = RESULTS_FOLDER / job_id
    os.makedirs(source_path, exist_ok=True)

    try:
        logging.info(f"Salvando {len(files)} arquivos na pasta {source_path}...")
        for file in files:
            filename = secure_filename(file.filename)
            file.save(source_path / filename)
        logging.info("Arquivos salvos com sucesso.")
            
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