import os
import logging
from flask import Flask, request, jsonify, send_file
from pathlib import Path
from werkzeug.utils import secure_filename

from analysis_engine import run_analysis, parse_numeros

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Diretório temporário para salvar os uploads e resultados
BASE_DIR = Path("/tmp/analyze_files")
BASE_DIR.mkdir(parents=True, exist_ok=True)


@app.route("/api/analyze", methods=["POST"])
def analyze_files():
    """
    Endpoint principal para análise de XMLs enviados via upload.
    Retorna o arquivo ZIP final para download.
    """
    try:
        logging.info("Recebendo requisição de análise...")

        # --- 1. Lê os arquivos enviados ---
        if 'files' not in request.files:
            return jsonify({"error": "Nenhum arquivo foi enviado."}), 400

        uploaded_files = request.files.getlist("files")
        if not uploaded_files:
            return jsonify({"error": "Lista de arquivos vazia."}), 400

        xml_files_in_memory = {}
        for file in uploaded_files:
            filename = secure_filename(file.filename)
            if not filename.lower().endswith(".xml"):
                logging.warning(f"Ignorado arquivo não-XML: {filename}")
                continue
            xml_files_in_memory[filename] = file.read()

        if not xml_files_in_memory:
            return jsonify({"error": "Nenhum arquivo XML válido foi enviado."}), 400

        # --- 2. Lê os números opcionais informados ---
        numeros_str = request.form.get("numeros", "")
        numeros_para_copiar = parse_numeros(numeros_str)

        # --- 3. Define diretório de trabalho ---
        job_id = os.urandom(8).hex()
        result_dir = BASE_DIR / f"job_{job_id}"
        result_dir.mkdir(parents=True, exist_ok=True)

        # --- 4. Executa a análise ---
        logging.info(f"Iniciando análise no diretório {result_dir}")
        result = run_analysis(xml_files_in_memory, result_dir, numeros_para_copiar)
        zip_path = result["zip_path"]

        # --- 5. Retorna o ZIP para download direto ---
        if not zip_path.exists():
            return jsonify({"error": "Falha ao gerar o arquivo ZIP."}), 500

        logging.info(f"Análise concluída, enviando arquivo: {zip_path}")
        return send_file(
            zip_path,
            as_attachment=True,
            download_name="resultados.zip",
            mimetype="application/zip"
        )

    except Exception as e:
        logging.exception("Erro interno no job")
        return jsonify({"error": str(e)}), 500


@app.route("/")
def index():
    return "Servidor de análise de XMLs ativo."


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
