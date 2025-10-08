import os
import shutil
import logging
import csv
from pathlib import Path
from collections import defaultdict, Counter
from dataclasses import dataclass, field
import xml.etree.ElementTree as ET
from datetime import datetime

@dataclass
class DadosNota:
    # ... (cole a dataclass DadosNota aqui)
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

def parse_numeros(raw_str: str) -> set[int]:
    # ... (cole a função parse_numeros aqui)
    numeros = set()
    for n in raw_str.split(","):
        n = n.strip()
        if n.isdigit():
            numeros.add(int(n))
        elif n:
            logging.warning(f"Valor inválido ignorado: {n}")
    return numeros

# ... (cole aqui as outras funções auxiliares: agrupar_lacunas, _mapear_cstat_para_tipo, obter_dados_xml, gerar_relatorios)

def run_analysis(pasta_origem: Path, pasta_destino: Path, numeros_para_copiar: set[int]) -> dict:
    logging.info(f"Iniciando análise para a pasta: {pasta_origem}")
    pasta_copiados = pasta_destino / "xmls_copiados"
    os.makedirs(pasta_copiados, exist_ok=True)
    
    arquivos_xml_encontrados = list(pasta_origem.rglob('*.xml'))
    if not arquivos_xml_encontrados:
        raise ValueError("Nenhum arquivo .xml encontrado na pasta enviada.")
    
    lista_dados_notas: list[DadosNota] = []
    # (Omitido por brevidade, mas cole o loop de análise dos arquivos aqui)
    for arquivo in arquivos_xml_encontrados:
        # ... lógica de análise ...
        pass
    
    caminho_sumario, caminho_csv = gerar_relatorios(lista_dados_notas, pasta_destino, pasta_origem)
    
    zip_filename = f"resultados_analise_{pasta_destino.name}"
    zip_filepath = shutil.make_archive(
        base_name=pasta_destino / zip_filename,
        format='zip',
        root_dir=pasta_destino
    )
    
    return {
        "zip_path": Path(zip_filepath),
        "summary_path": caminho_sumario,
    }