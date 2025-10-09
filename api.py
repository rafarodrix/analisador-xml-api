import os
import uuid
import shutil
import logging
import zipfile
import io 
from pathlib import Path
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS 
from werkzeug.utils import secure_filename
import analysis_engine

app = Flask(__name__)
CORS(app) 

logging.basicConfig(level=logging.INFO)

# Define o diretório base para todos os jobs
BASE_DIR = Path("/tmp/xml_analysis_jobs")
BASE_DIR.mkdir(parents=True, exist_ok=True)


@app.route("/api/analyze", methods=["POST"])
def analyze_files():
    """
    Endpoint principal para análise de XMLs, agora com upload dinâmico e limpeza segura.
    """
    job_id = os.urandom(8).hex()
    job_dir = BASE_DIR / f"job_{job_id}"
    job_dir.mkdir()

    try:
        logging.info(f"Iniciando job {job_id}")
        xml_files_in_memory = {}

        # Caso 1: Upload de um arquivo ZIP
        if 'file' in request.files:
            zip_file = request.files['file']
            if zip_file and zip_file.filename.lower().endswith('.zip'):
                logging.info(f"Job {job_id}: Recebido arquivo ZIP: {zip_file.filename}")
                with zipfile.ZipFile(zip_file, 'r') as zf:
                    for filename in zf.namelist():
                        if not filename.endswith('/') and '__MACOSX' not in filename and filename.lower().endswith('.xml'):
                            xml_files_in_memory[os.path.basename(filename)] = zf.read(filename)
            else:
                return jsonify({"error": "Envie um arquivo .zip válido."}), 400

        # Caso 2: Upload de uma pasta (múltiplos arquivos)
        elif 'files' in request.files:
            uploaded_files = request.files.getlist("files")
            logging.info(f"Job {job_id}: Recebidos {len(uploaded_files)} arquivos de uma pasta.")
            for file in uploaded_files:
                if file and file.filename and file.filename.lower().endswith('.xml'):
                    xml_files_in_memory[secure_filename(file.filename)] = file.read()
        else:
            return jsonify({"error": "Nenhum arquivo ou pasta foi enviado."}), 400

        if not xml_files_in_memory:
            return jsonify({"error": "Nenhum arquivo XML válido foi encontrado no envio."}), 400

        # --- MELHORIA: CORREÇÃO DO NOME DO CAMPO ---
        numeros_str = request.form.get("numerosParaCopiar", "")
        numeros_para_copiar = analysis_engine.parse_numeros(numeros_str)

        # --- Executa a análise ---
        logging.info(f"Job {job_id}: Iniciando análise de {len(xml_files_in_memory)} arquivos.")
        result = analysis_engine.run_analysis(xml_files_in_memory, job_dir, numeros_para_copiar)
        zip_path = result["zip_path"]

        if not zip_path.exists():
            raise IOError("Arquivo ZIP final não foi gerado.")

        # 1. Lê todo o arquivo ZIP para a memória
        zip_in_memory = io.BytesIO()
        with open(zip_path, 'rb') as f:
            zip_in_memory.write(f.read())
        zip_in_memory.seek(0) # Volta para o início do buffer

        # 2. Com o arquivo seguro na memória, a pasta temporária já pode ser deletada
        shutil.rmtree(job_dir)
        logging.info(f"Job {job_id}: Pasta temporária {job_dir} limpa com sucesso.")

        # 3. Envia o arquivo que está na memória
        logging.info(f"Job {job_id}: Análise concluída, enviando arquivo ZIP.")
        return send_file(
            zip_in_memory,
            as_attachment=True,
            download_name=zip_path.name, # Usa o nome original do arquivo
            mimetype="application/zip"
        )

    except Exception as e:
        logging.exception(f"Erro interno no job {job_id}")
        # Limpa a pasta do job em caso de erro também
        if job_dir.exists():
            shutil.rmtree(job_dir)
        return jsonify({"error": "Ocorreu um erro interno no servidor."}), 500


@app.route("/")
def index():
    return "Servidor de análise de XMLs ativo."


if __name__ == "__main__":
    # Esta parte é ignorada pelo Gunicorn no Render, serve apenas para teste local
    app.run(host="0.0.0.0", port=5001, debug=True)