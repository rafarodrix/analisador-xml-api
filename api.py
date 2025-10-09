import os
import uuid
import shutil
import logging
import zipfile
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
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100 MB

@app.route('/api/analyze', methods=['POST'])
def analyze_files():
    logging.info("="*50)
    logging.info("Nova requisição recebida em /api/analyze")
    
    numeros_raw = request.form.get('numerosParaCopiar', '')
    job_id = str(uuid.uuid4())
    source_path = UPLOAD_FOLDER / job_id
    dest_path = RESULTS_FOLDER / job_id
    os.makedirs(source_path, exist_ok=True)

    try:
        # ??? LÓGICA DINÂMICA PARA LIDAR COM ZIP OU PASTA ???

        # CASO 1: Um único arquivo 'file' foi enviado (é um ZIP)
        if 'file' in request.files:
            zip_file = request.files['file']
            if zip_file.filename == '' or not zip_file.filename.endswith('.zip'):
                return jsonify({"error": "Por favor, envie um arquivo .zip válido."}), 400
            
            zip_filename = secure_filename(zip_file.filename)
            zip_filepath = source_path / zip_filename
            zip_file.save(zip_filepath)
            logging.info(f"Arquivo {zip_filename} salvo, iniciando extração...")

            with zipfile.ZipFile(zip_filepath, 'r') as zf:
                zf.extractall(source_path)
            os.remove(zip_filepath)
            logging.info("Arquivos extraídos com sucesso.")

        # CASO 2: Múltiplos arquivos 'files' foram enviados (é uma pasta)
        elif 'files' in request.files:
            files = request.files.getlist('files')
            if not files or (len(files) == 1 and files[0].filename == ''):
                 return jsonify({"error": "Nenhum arquivo da pasta foi recebido."}), 400
            
            logging.info(f"Recebidos {len(files)} arquivos de uma pasta. Salvando...")
            for file in files:
                if file and file.filename:
                    filename = secure_filename(file.filename)
                    file.save(source_path / filename)
            logging.info("Arquivos da pasta salvos com sucesso.")
        
        # CASO 3: Nenhum dos formatos esperados foi enviado
        else:
            return jsonify({"error": "Nenhum arquivo ou pasta foi enviado."}), 400

        # ??? FIM DA LÓGICA DINÂMICA ???
            
        # A partir daqui, o resto do código funciona igual, pois a source_path já contém os XMLs
        numeros_para_copiar = analysis_engine.parse_numeros(numeros_raw)
        result_paths = analysis_engine.run_analysis(source_path, dest_path, numeros_para_copiar)

        with open(result_paths["summary_path"], 'r', encoding='utf-8') as f:
            summary_content = f.read()

        zip_path_resultado = result_paths['zip_path']
        
        return jsonify({
            "summary": summary_content,
            "downloadUrl": f"/api/download/{job_id}/{zip_path_resultado.name}",
        })
    except Exception as e:
        logging.error(f"Erro no job {job_id}: {e}", exc_info=True)
        return jsonify({"error": "Ocorreu um erro interno durante a análise."}), 500
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