import os
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
CORS(app, supports_credentials=True)

logging.basicConfig(level=logging.INFO)

BASE_DIR = Path("/tmp/xml_analysis_jobs")
BASE_DIR.mkdir(parents=True, exist_ok=True)

MAX_XML_FILES = 20000  # proteção contra uploads gigantes

@app.route("/api/analyze", methods=["POST"])
def analyze_files():
    job_id = os.urandom(8).hex()
    job_dir = BASE_DIR / f"job_{job_id}"
    job_dir.mkdir(exist_ok=True)

    try:
        logging.info(f"[{job_id}] Iniciando análise")
        xml_files_in_memory: dict[str, bytes] = {}

        # ==============================================================
        # ✅ Caso: upload ZIP
        # ==============================================================
        if "file" in request.files:
            uploaded_zip = request.files["file"]

            if not uploaded_zip.filename.lower().endswith(".zip"):
                return jsonify({"error": "Envie um arquivo .zip válido."}), 400

            logging.info(f"[{job_id}] ZIP recebido: {uploaded_zip.filename}")

            try:
                with zipfile.ZipFile(uploaded_zip, "r") as zf:
                    for filename in zf.namelist():
                        filename = filename.strip()

                        if filename.endswith("/") or "__MACOSX" in filename:
                            continue

                        if filename.lower().endswith(".xml"):
                            safe_name = secure_filename(os.path.basename(filename))
                            xml_files_in_memory[safe_name] = zf.read(filename)

                            if len(xml_files_in_memory) > MAX_XML_FILES:
                                return jsonify({
                                    "error": f"ZIP contém mais de {MAX_XML_FILES} XMLs."
                                }), 400
            except zipfile.BadZipFile:
                return jsonify({"error": "Arquivo ZIP corrompido ou inválido."}), 400

        # ==============================================================
        # ✅ Caso: upload múltiplos arquivos (pasta)
        # ==============================================================
        elif "files" in request.files:
            files = request.files.getlist("files")
            logging.info(f"[{job_id}] Recebidos {len(files)} arquivos")

            for file in files:
                if file and file.filename.lower().endswith(".xml"):
                    safe_name = secure_filename(file.filename.strip())
                    xml_files_in_memory[safe_name] = file.read()

                if len(xml_files_in_memory) > MAX_XML_FILES:
                    return jsonify({
                        "error": f"Upload contém mais de {MAX_XML_FILES} XMLs."
                    }), 400

        else:
            return jsonify({"error": "Nenhum arquivo foi enviado."}), 400

        # ==============================================================
        # ✅ Validação
        # ==============================================================
        if not xml_files_in_memory:
            return jsonify({"error": "Nenhum XML válido encontrado."}), 400

        numeros_str = request.form.get("numerosParaCopiar", "")
        numeros_para_copiar = analysis_engine.parse_numeros(numeros_str)

        logging.info(f"[{job_id}] Iniciando processamento ({len(xml_files_in_memory)} XMLs)")

        # ==============================================================
        # ✅ EXECUTA ANÁLISE
        # ==============================================================
        result = analysis_engine.run_analysis(
            xml_files_in_memory,
            job_dir,
            numeros_para_copiar
        )

        zip_path = result["zip_path"]
        if not zip_path.exists():
            raise IOError("Arquivo ZIP não foi gerado.")

        # ==============================================================
        # ✅ Lê ZIP na memória (frontend baixa automaticamente)
        # ==============================================================
        zip_buffer = io.BytesIO()
        with open(zip_path, "rb") as f:
            zip_buffer.write(f.read())
        zip_buffer.seek(0)

        # ==============================================================
        # ✅ REMOVE pasta temporária com segurança
        # ==============================================================
        shutil.rmtree(job_dir, ignore_errors=True)
        logging.info(f"[{job_id}] Finalizado e limpo com sucesso.")

        # ==============================================================
        # ✅ ENVIA O ZIP COMO RESPOSTA
        # ==============================================================
        return send_file(
            zip_buffer,
            as_attachment=True,
            download_name=zip_path.name,
            mimetype="application/zip"
        )

    except Exception as e:
        logging.exception(f"[{job_id}] Erro interno inesperado: {e}")
        shutil.rmtree(job_dir, ignore_errors=True)
        return jsonify({"error": "Falha interna no servidor.", "job_id": job_id}), 500


@app.route("/")
def index():
    return "Servidor de análise de XMLs ativo."


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
