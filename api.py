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
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100 MB

# As pastas de resultados precisam persistir entre as requisições
RESULTS_FOLDER = Path('/tmp/results')
os.makedirs(RESULTS_FOLDER, exist_ok=True)


@app.route('/api/analyze', methods=['POST'])
def analyze_files():
    job_id = str(uuid.uuid4())
    # A única pasta que usamos no disco é para os resultados FINAIS
    result_dir = RESULTS_FOLDER / job_id
    result_dir.mkdir(parents=True, exist_ok=True)
    
    # Dicionário para guardar os arquivos em memória: {nome_do_arquivo: conteudo_em_bytes}
    xml_files_in_memory = {}

    try:
        # --- LÓGICA ATUALIZADA PARA TRABALHAR EM MEMÓRIA ---
        
        # Caso 1: Upload de um arquivo ZIP
        if 'file' in request.files:
            zip_file = request.files['file']
            if not zip_file or not zip_file.filename.lower().endswith('.zip'):
                return jsonify({"error": "Envie um arquivo .zip válido."}), 400
            
            # Lê o zip em memória e extrai o conteúdo dos XMLs para o dicionário
            with zipfile.ZipFile(zip_file, 'r') as zf:
                for filename in zf.namelist():
                    # Ignora pastas e arquivos de metadados (ex: do macOS)
                    if not filename.endswith('/') and '__MACOSX' not in filename and filename.lower().endswith('.xml'):
                        xml_files_in_memory[os.path.basename(filename)] = zf.read(filename)

        # Caso 2: Upload de uma pasta (múltiplos arquivos)
        elif 'files' in request.files:
            files = request.files.getlist('files')
            for file in files:
                if file and file.filename and file.filename.lower().endswith('.xml'):
                    # Lê o conteúdo de cada arquivo para o dicionário
                    xml_files_in_memory[secure_filename(file.filename)] = file.read()
        else:
            return jsonify({"error": "Nenhum arquivo enviado."}), 400

        numeros_raw = request.form.get('numerosParaCopiar', '')
        numeros_para_copiar = analysis_engine.parse_numeros(numeros_raw)
        
        # --- CHAMADA CORRETA ---
        # Agora passamos o dicionário em memória, como o analysis_engine.py espera
        result_paths = analysis_engine.run_analysis(xml_files_in_memory, result_dir, numeros_para_copiar)

        # A partir daqui, a lógica para retornar o resultado é diferente.
        # O frontend espera um JSON, não um download direto.
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
        @after_this_request
        def cleanup(response):
            # Limpa a pasta de resultados após a requisição de download ser concluída
            try:
                if directory.exists():
                    shutil.rmtree(directory)
            except Exception as e:
                logging.error(f"Erro ao limpar a pasta temporária {directory}: {e}")
            return response

        return send_from_directory(directory, filename, as_attachment=True)
    except FileNotFoundError:
        return jsonify({"error": "Arquivo não encontrado ou expirado."}), 404


@app.route('/', methods=['GET'])
def index():
    return jsonify({"status": "API do Analisador de XML está no ar."})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False)