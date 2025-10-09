import os
import uuid
import shutil
import logging
import zipfile
import tempfile
from pathlib import Path
from flask import Flask, request, jsonify, send_from_directory, after_this_request
from flask_cors import CORS
from werkzeug.utils import secure_filename
import analysis_engine

app = Flask(__name__)
CORS(app)

app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100 MB

logging.basicConfig(level=logging.INFO)


def extract_zip_safely(zip_path: Path, extract_to: Path):
    """Extrai arquivos ZIP com segurança, evitando path traversal e bombas de zip."""
    with zipfile.ZipFile(zip_path, 'r') as zf:
        for member in zf.infolist():
            filename = Path(member.filename)
            if filename.is_absolute() or ".." in filename.parts:
                logging.warning(f"Arquivo suspeito no ZIP: {filename}")
                continue
            if member.file_size > 50 * 1024 * 1024:  # limite de 50 MB por arquivo
                logging.warning(f"Arquivo grande ignorado: {filename}")
                continue
            zf.extract(member, extract_to)


def find_xml_root(path: Path) -> Path:
    """Encontra a pasta contendo arquivos XML, mesmo em subpastas."""
    for root, _, files in os.walk(path):
        if any(f.lower().endswith('.xml') for f in files):
            return Path(root)
    raise ValueError("Nenhum arquivo .xml encontrado na pasta enviada.")


@app.route('/api/analyze', methods=['POST'])
def analyze_files():
    job_id = str(uuid.uuid4())

    with tempfile.TemporaryDirectory() as tmpdir:
        upload_dir = Path(tmpdir) / "upload"
        result_dir = Path(tmpdir) / "result"
        upload_dir.mkdir(parents=True, exist_ok=True)
        result_dir.mkdir(parents=True, exist_ok=True)

        try:
            numeros_raw = request.form.get('numerosParaCopiar', '')
            numeros_para_copiar = analysis_engine.parse_numeros(numeros_raw)

            # Upload ZIP ou múltiplos arquivos
            if 'file' in request.files:
                zip_file = request.files['file']
                if not zip_file.filename.endswith('.zip'):
                    return jsonify({"error": "Envie um arquivo .zip válido."}), 400
                zip_path = upload_dir / secure_filename(zip_file.filename)
                zip_file.save(zip_path)
                extract_zip_safely(zip_path, upload_dir)
            elif 'files' in request.files:
                files = request.files.getlist('files')
                if not files or files[0].filename == '':
                    return jsonify({"error": "Nenhum arquivo foi recebido."}), 400
                for file in files:
                    if file and file.filename.lower().endswith('.xml'):
                        file.save(upload_dir / secure_filename(file.filename))
            else:
                return jsonify({"error": "Nenhum arquivo enviado."}), 400

            analysis_root = find_xml_root(upload_dir)
            result_paths = analysis_engine.run_analysis(analysis_root, result_dir, numeros_para_copiar)

            with open(result_paths["summary_path"], 'r', encoding='utf-8') as f:
                summary = f.read()

            result_zip = result_paths["zip_path"]
            return jsonify({
                "jobId": job_id,
                "summary": summary,
                "downloadUrl": f"/api/download/{job_id}/{result_zip.name}",
            })

        except ValueError as ve:
            logging.warning(f"Erro do usuário: {ve}")
            return jsonify({"error": str(ve)}), 400
        except Exception as e:
            logging.exception(f"Erro interno no job {job_id}")
            return jsonify({"error": "Erro interno no servidor."}), 500


@app.route('/api/download/<job_id>/<filename>', methods=['GET'])
def download_file(job_id, filename):
    results_base = Path('/tmp/results') / secure_filename(job_id)
    filepath = results_base / secure_filename(filename)
    if not filepath.exists():
        return jsonify({"error": "Arquivo não encontrado ou expirado."}), 404
    return send_from_directory(results_base, filepath.name, as_attachment=True)


@app.route('/', methods=['GET'])
def index():
    return jsonify({"status": "API do Analisador de XML está no ar."})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False)
