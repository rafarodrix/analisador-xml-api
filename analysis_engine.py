import os
import shutil
import logging
import csv
import time
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from collections import Counter
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- ESTRUTURA DE DADOS ---
@dataclass
class DadosNota:
    """Estrutura para armazenar de forma organizada os dados extraídos de um XML."""
    arquivo_path: Path
    tipo_documento: str = "Desconhecido"
    chave_acesso: str = ""
    status_code: str = "N/A"
    status_text: str = "N/A"
    modelo: str = ""
    serie: str = ""
    numero_inicial: str = ""
    numero_final: str = ""
    data_emissao: str = ""
    foi_copiado: bool = False
    erros: list[str] = field(default_factory=list)

# --- FUNÇÕES AUXILIARES ---

def parse_numeros(raw_str: str) -> set[int]:
    """Converte string de números separados por vírgula em conjunto de inteiros válidos."""
    if not raw_str:
        return set()
    numeros = set()
    for n in raw_str.split(","):
        n = n.strip()
        if n.isdigit():
            numeros.add(int(n))
        elif n:
            logging.warning(f"Valor inválido ignorado: {n}")
    return numeros

def _mapear_cstat_para_tipo(cstat: str) -> str:
    """Mapeia código cStat em tipo de documento."""
    mapping = {
        '100': "NFe Autorizada",
        '101': "NFe Cancelada",
        '135': "NFe Cancelada",
        '102': "NFe Inutilizada",
    }
    if cstat in mapping:
        return mapping[cstat]
    if cstat and cstat.startswith(('2', '3')):
        return f"NFe com Rejeição ({cstat})"
    return f"Status Desconhecido ({cstat})"

def obter_dados_xml_de_conteudo(filename: str, file_content: bytes) -> DadosNota:
    """Extrai dados essenciais de um XML (NFe, evento ou inutilização) a partir do seu conteúdo em bytes."""
    nota = DadosNota(arquivo_path=Path(filename))
    try:
        # Tentativa de decodificação segura
        content_str = file_content.decode('utf-8', errors='replace')

        root = ET.fromstring(content_str)
        ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

        # Busca genérica — cobre diferentes tipos de XMLs da SEFAZ
        nota.status_code = (root.findtext('.//nfe:cStat', '', ns) or '').strip()
        nota.status_text = (root.findtext('.//nfe:xMotivo', '', ns) or '').strip()
        nota.tipo_documento = _mapear_cstat_para_tipo(nota.status_code)

        ide_node = root.find('.//nfe:ide', ns)
        if ide_node is not None:
            nota.modelo = ide_node.findtext('nfe:mod', '', ns)
            nota.serie = ide_node.findtext('nfe:serie', '', ns)
            nota.numero_inicial = ide_node.findtext('nfe:nNF', '', ns)
            nota.numero_final = nota.numero_inicial
            nota.data_emissao = ide_node.findtext('nfe:dhEmi', '', ns)

        nota.chave_acesso = (root.findtext('.//nfe:chNFe', '', ns) or '').strip()

        # Inutilização de faixa de numeração
        if nota.tipo_documento == "NFe Inutilizada":
            infInut_node = root.find('.//nfe:infInut', ns)
            if infInut_node is not None:
                nota.numero_inicial = infInut_node.findtext('nfe:nNFIni', nota.numero_inicial, ns)
                nota.numero_final = infInut_node.findtext('nfe:nNFFin', nota.numero_final, ns)

    except ET.ParseError as e:
        nota.erros.append(f"XML inválido: {e}")
    except Exception as e:
        nota.erros.append(f"Erro inesperado: {e}")

    return nota

def gerar_relatorios(lista_dados_notas: list[DadosNota], pasta_destino: Path) -> tuple[Path, Path]:
    """Gera arquivos de relatório (resumo.txt) e CSV detalhado."""
    logging.info("Iniciando geração de relatórios...")
    resumo_path = pasta_destino / "resumo_analise.txt"
    csv_path = pasta_destino / "relatorio_detalhado.csv"

    contagem_status = Counter(d.tipo_documento for d in lista_dados_notas)
    com_erro = sum(1 for n in lista_dados_notas if n.erros)

    with resumo_path.open('w', encoding='utf-8') as f:
        f.write(f"Resumo da Análise - {datetime.now():%d/%m/%Y %H:%M:%S}\n")
        f.write(f"Total de XMLs processados: {len(lista_dados_notas)}\n")
        f.write(f"XMLs com erro de leitura: {com_erro}\n\n")
        f.write("--- Sumário de Status dos Documentos ---\n")
        for status, contagem in sorted(contagem_status.items()):
            f.write(f"- {status:<30}: {contagem}\n")

    headers = [
        "arquivo_origem", "tipo_documento", "status_sefaz_cod", "status_sefaz_motivo",
        "numero_inicial", "numero_final", "serie", "modelo", "data_emissao",
        "chave_acesso", "foi_copiado", "erros"
    ]
    with csv_path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(headers)
        for nota in sorted(lista_dados_notas, key=lambda n: n.arquivo_path.name):
            writer.writerow([
                nota.arquivo_path.name, nota.tipo_documento, nota.status_code,
                nota.status_text, nota.numero_inicial, nota.numero_final,
                nota.serie, nota.modelo, nota.data_emissao, nota.chave_acesso,
                "Sim" if nota.foi_copiado else "Não", "; ".join(nota.erros),
            ])
    logging.info(f"Relatórios gerados em: {resumo_path} e {csv_path}")
    return resumo_path, csv_path

# --- FUNÇÃO PRINCIPAL ---

def run_analysis(xml_files_in_memory: dict[str, bytes], pasta_destino: Path, numeros_para_copiar: set[int]) -> dict:
    """Executa análise de XMLs (em memória), gera relatórios e compacta o resultado."""
    start_time = time.time()
    total_arquivos = len(xml_files_in_memory)
    logging.info(f"Iniciando análise de {total_arquivos} XMLs...")

    if total_arquivos == 0:
        raise ValueError("Nenhum arquivo XML foi enviado para análise.")

    pasta_destino.mkdir(parents=True, exist_ok=True)
    pasta_copiados = pasta_destino / "xmls_copiados"
    pasta_copiados.mkdir(exist_ok=True)

    lista_dados_notas = []
    
    # --- MELHORIA: PROCESSAMENTO PARALELO ---
    logging.info("Processando XMLs em paralelo...")
    # Usa ThreadPoolExecutor para analisar múltiplos arquivos ao mesmo tempo
    with ThreadPoolExecutor() as executor:
        # Submete cada arquivo para análise em uma thread separada
        futures = {executor.submit(obter_dados_xml_de_conteudo, filename, content): (filename, content)
                   for filename, content in xml_files_in_memory.items()}
        
        # Coleta os resultados conforme eles ficam prontos
        for future in as_completed(futures):
            nota = future.result()
            lista_dados_notas.append(nota)
    logging.info("Análise de todos os XMLs concluída.")

    # --- Cópia dos arquivos (sequencial, pois envolve escrita em disco) ---
    logging.info("Verificando arquivos para cópia...")
    copiados = 0
    for nota in lista_dados_notas:
        try:
            numero = int(nota.numero_inicial) if nota.numero_inicial and nota.numero_inicial.isdigit() else None
            if numero and numero in numeros_para_copiar:
                filename, file_content = next((name, content) for name, content in xml_files_in_memory.items() if name == nota.arquivo_path.name)
                destino = pasta_copiados / filename
                destino.write_bytes(file_content)
                nota.foi_copiado = True
                copiados += 1
        except Exception as e:
            nota.erros.append(f"Falha ao copiar XML: {e}")

    resumo_path, _ = gerar_relatorios(lista_dados_notas, pasta_destino)

    logging.info("Compactando resultados...")
    zip_filename = f"resultados_{datetime.now():%Y%m%d_%H%M%S}"
    zip_filepath = shutil.make_archive(
        base_name=str(pasta_destino / zip_filename),
        format="zip",
        root_dir=pasta_destino
    )

    elapsed = round(time.time() - start_time, 2)
    logging.info(f"Análise finalizada: {len(lista_dados_notas)} XMLs processados, {copiados} copiados ({elapsed}s).")

    return {
        "zip_path": Path(zip_filepath),
        "summary_path": resumo_path,
    }