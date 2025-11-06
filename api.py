import os
import uuid
import shutil
import logging
import zipfile
import io
from pathlib import Path
from flask import Flask, request, jsonify, send_from_directory # send_file foi trocado por send_from_directory
from flask_cors import CORS
from werkzeug.utils import secure_filename

# Importe seu módulo de análise (que tem a função com o filtro de CNPJ)
import analysis_engine 

app = Flask(__name__)
CORS(app, supports_credentials=True)

logging.basicConfig(level=logging.INFO)

# --- MELHORIA: Esta pasta agora guarda os *resultados* para download ---
# Ela precisa ser persistente entre as requisições
RESULTS_DIR = Path("/tmp/xml_analysis_jobs")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

MAX_XML_FILES = 20000  # Limite de segurança

@app.route("/api/analyze", methods=["POST"])
def analyze_files():
    # --- 1. PREPARAÇÃO DO JOB ---
    # O job_id agora é usado para criar a URL de download
    job_id = os.urandom(8).hex()
    result_dir = RESULTS_DIR / f"job_{job_id}"
    result_dir.mkdir()

    try:
        logging.info(f"Iniciando job {job_id}")
        xml_files_in_memory = {} # ATENÇÃO: Risco de memória, veja nota no final

        # --- 2. LÓGICA DE UPLOAD (sem alteração) ---
        # (Esta lógica de carregar tudo na memória vai falhar com 6.000+ arquivos)
        if 'file' in request.files:
            zip_file = request.files['file']
            if not zip_file.filename.lower().endswith('.zip'):
                return jsonify({"error": "Envie um arquivo .zip válido."}), 400

            logging.info(f"Job {job_id}: Recebido arquivo ZIP: {zip_file.filename}")
            with zipfile.ZipFile(zip_file, 'r') as zf:
                for filename in zf.namelist():
                    filename = filename.strip()
                    if filename.endswith('/') or '__MACOSX' in filename:
                        continue
                    if filename.lower().endswith('.xml'):
                        safe = secure_filename(os.path.basename(filename))
                        xml_files_in_memory[safe] = zf.read(filename)
                        if len(xml_files_in_memory) > MAX_XML_FILES:
                            return jsonify({"error": f"ZIP contém mais de {MAX_XML_FILES} XMLs."}), 400
        
        elif 'files' in request.files:
            files = request.files.getlist("files")
            logging.info(f"Job {job_id}: Recebidos {len(files)} arquivos.")
            for file in files:
                if file and file.filename.lower().endswith('.xml'):
                    safe = secure_filename(file.filename.strip())
                    xml_files_in_memory[safe] = file.read()
        else:
            return jsonify({"error": "Nenhum arquivo foi enviado."}), 400

        if not xml_files_in_memory:
            return jsonify({"error": "Nenhum XML válido encontrado."}), 400

        # --- 3. [REFATORADO] Captura dos dados do formulário ---
        numeros_str = request.form.get("numerosParaCopiar", "")
        numeros_para_copiar = analysis_engine.parse_numeros(numeros_str)
        
        # Captura o CNPJ enviado pelo frontend
        cnpj_empresa = request.form.get("cnpjEmpresa", "")
        if not cnpj_empresa:
            logging.warning(f"Job {job_id}: CNPJ não fornecido.")
            return jsonify({"error": "O CNPJ da empresa é obrigatório para a análise."}), 400

        # --- 4. EXECUÇÃO DA ANÁLISE ---
        logging.info(f"Job {job_id}: Iniciando análise de {len(xml_files_in_memory)} arquivos para o CNPJ: {cnpj_empresa}")
        
        # Passa o 'cnpj_empresa' para sua engine
        result = analysis_engine.run_analysis(
            xml_files_in_memory, 
            result_dir,  # Salva os resultados na pasta do job
            numeros_para_copiar,
            cnpj_empresa=cnpj_empresa # Passa o novo parâmetro
        )

        zip_path = result.get("zip_path")
        summary_path = result.get("summary_path")

        if not zip_path or not zip_path.exists() or not summary_path:
            raise IOError("Resultado da análise (zip ou resumo) não foi gerado.")

        # --- 5. [REFATORADO] Resposta em JSON ---
        # Em vez de enviar o arquivo, lemos o resumo e criamos uma URL de download
        
        summary_content = summary_path.read_text(encoding="utf-8")
        
        # Cria a URL relativa que o frontend usará
        download_url = f"/api/download/{job_id}/{zip_path.name}"
        
        logging.info(f"Job {job_id}: Análise concluída. Enviando JSON de sucesso.")

        # A pasta do job (result_dir) NÃO é deletada, pois os arquivos
        # precisam estar lá para o endpoint de download.

        return jsonify({
            "summary": summary_content,
            "downloadUrl": download_url
        })

    except Exception as e:
        logging.exception(f"Erro interno no job {job_id}")
        # Se der erro, limpamos a pasta do job
        shutil.rmtree(result_dir, ignore_errors=True)
        # Retorna o erro como string para o frontend exibir
        return jsonify({"error": f"Falha interna no servidor: {str(e)}", "job_id": job_id}), 500

# --- 6. [NOVO ENDPOINT] Para o Download ---
# O frontend chamará esta rota quando o usuário clicar em "Baixar Resultados"
@app.route("/api/download/<job_id>/<path:filename>", methods=["GET"])
def download_file(job_id, filename):
    """
    Envia o arquivo de resultado de um job específico.
    (NOTA: Em produção, você deve adicionar um mecanismo para limpar
    pastas de jobs antigos, por exemplo, após 24 horas)
    """
    try:
        logging.info(f"Download solicitado: Job {job_id}, Arquivo {filename}")
        job_dir = RESULTS_DIR / f"job_{job_id}"
        
        # send_from_directory é a forma segura de enviar arquivos
        return send_from_directory(
            job_dir,
            filename,
            as_attachment=True
        )
    except FileNotFoundError:
        logging.error(f"Arquivo não encontrado para download: {job_dir / filename}")
        return jsonify({"error": "Arquivo não encontrado ou expirado."}), 404
    except Exception as e:
        logging.exception(f"Erro no download do job {job_id}")
        return jsonify({"error": str(e)}), 500


@app.route("/")
def index():
    return "Servidor de análise de XMLs ativo."


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)