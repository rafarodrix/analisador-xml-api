# backend/analysis_engine.py
import os
import shutil
import logging
import csv
import time
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict, Counter
import xml.etree.ElementTree as ET

# ... (dataclass DadosNota permanece a mesma) ...

def parse_numeros(raw_str: str) -> set[int]:
    # ... (função parse_numeros permanece a mesma) ...
    pass

def obter_dados_xml_de_conteudo(filename: str, file_content: bytes) -> DadosNota:
    """Lê e extrai dados do CONTEÚDO de um arquivo XML."""
    nota = DadosNota(arquivo_path=Path(filename)) # Armazena apenas o nome do arquivo
    try:
        # ET.fromstring espera bytes ou uma string decodificada
        # Tenta decodificar como utf-8, com fallback para latin-1
        try:
            content_str = file_content.decode('utf-8')
        except UnicodeDecodeError:
            content_str = file_content.decode('latin-1')

        root = ET.fromstring(content_str)
        
        # Usa namespaces para uma busca mais robusta em diferentes tipos de XML
        ns = {
            'nfe': 'http://www.portalfiscal.inf.br/nfe',
            'ds': 'http://www.w3.org/2000/09/xmldsig#'
        }
        
        # Tenta encontrar o cStat em qualquer protocolo de resposta
        cstat_node = root.find('.//nfe:cStat', ns)
        xmotivo_node = root.find('.//nfe:xMotivo', ns)
        ide_node = root.find('.//nfe:ide', ns)
        
        if cstat_node is not None:
            nota.status_code = cstat_node.text
            nota.tipo_documento = _mapear_cstat_para_tipo(nota.status_code)
        if xmotivo_node is not None:
            nota.status_text = xmotivo_node.text

        if ide_node is not None:
            nota.modelo = ide_node.findtext('nfe:mod', '', ns)
            nota.serie = ide_node.findtext('nfe:serie', '', ns)
            nota.numero_inicial = ide_node.findtext('nfe:nNF', '', ns)
            nota.data_emissao = ide_node.findtext('nfe:dhEmi', '', ns)
        
    except ET.ParseError as e:
        nota.erros.append(f"XML inválido: {e}")
    except Exception as e:
        nota.erros.append(f"Erro inesperado: {e}")
    return nota


def _mapear_cstat_para_tipo(cstat: str) -> str:
    # ... (sua função _mapear_cstat_para_tipo permanece a mesma) ...
    pass

def gerar_relatorios(lista_dados_notas: list[DadosNota], pasta_destino: Path):
    # ... (sua função gerar_relatorios, mas sem o parâmetro pasta_origem) ...
    pass


def run_analysis(xml_files_in_memory: dict, pasta_destino: Path, numeros_para_copiar: set[int]) -> dict:
    """Executa análise a partir de um dicionário de arquivos em memória."""
    start = time.time()
    logging.info(f"Iniciando análise para {len(xml_files_in_memory)} arquivos em memória.")

    if not xml_files_in_memory:
        raise ValueError("Nenhum arquivo .xml válido foi recebido.")

    pasta_copiados = pasta_destino / "xmls_copiados"
    pasta_copiados.mkdir(parents=True, exist_ok=True)

    lista_dados_notas: list[DadosNota] = []
    copiados = 0

    for filename, file_content in xml_files_in_memory.items():
        nota = obter_dados_xml_de_conteudo(filename, file_content)

        try:
            numero = int(nota.numero_inicial) if nota.numero_inicial.isdigit() else None
            if numero and numero in numeros_para_copiar:
                # Se precisa copiar, escreve o conteúdo em memória para um arquivo
                with open(pasta_copiados / filename, 'wb') as f:
                    f.write(file_content)
                nota.foi_copiado = True
                copiados += 1
        except Exception as e:
            nota.erros.append(f"Falha ao copiar arquivo: {e}")

        lista_dados_notas.append(nota)

    gerar_relatorios(lista_dados_notas, pasta_destino)

    zip_filename = f"resultados_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    zip_filepath = shutil.make_archive(
        base_name=str(pasta_destino / zip_filename), format="zip", root_dir=pasta_destino
    )
    
    elapsed = round(time.time() - start, 2)
    logging.info(f"Análise concluída em {elapsed}s.")

    return { "zip_path": Path(zip_filepath), "summary_path": pasta_destino / "resumo.txt" }