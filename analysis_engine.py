import logging
import csv
import time
import zipfile
import textwrap
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict, Counter
from concurrent.futures import ProcessPoolExecutor, as_completed

from lxml import etree
from tqdm import tqdm

# --- ESTRUTURA DE DADOS ---
@dataclass
class DadosNota:
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

# --- AUXILIARES ---
def _formatar_data(iso_str: str) -> str:
    if not iso_str or "T" not in iso_str:
        return iso_str
    try:
        if iso_str.endswith("Z"):
            iso_str = iso_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso_str)
        return dt.strftime("%d/%m/%Y %H:%M")
    except:
        return iso_str

def _mapear_cstat_para_tipo(cstat: str) -> str:
    mapping = {
        '100': "NFe Autorizada",
        '101': "NFe Cancelada",
        '135': "NFe Cancelada",
        '102': "NFe Inutilizada",
    }
    if cstat in mapping:
        return mapping[cstat]
    if cstat and cstat[:1] in ('2', '3'):
        return f"NFe com Rejeição ({cstat})"
    return f"Status Desconhecido ({cstat})"

# ✅ versao com LXML
def obter_dados_xml_de_conteudo(filename: str, file_content: bytes) -> DadosNota:
    nota = DadosNota(arquivo_path=Path(filename))

    try:
        parser = etree.XMLParser(recover=True, encoding="utf-8")
        root = etree.fromstring(file_content, parser=parser)

        ns = {"nfe": "http://www.portalfiscal.inf.br/nfe"}

        # cStat e motivo
        cstat = root.xpath("string(.//nfe:cStat)", namespaces=ns).strip()
        motivo = root.xpath("string(.//nfe:xMotivo)", namespaces=ns).strip()

        nota.status_code = cstat or "N/A"
        nota.status_text = motivo or "N/A"
        nota.tipo_documento = _mapear_cstat_para_tipo(cstat)

        # chave
        chave = root.xpath("string(.//nfe:chNFe)", namespaces=ns).strip()
        if chave:
            nota.chave_acesso = chave

        # ide → modelo, serie, numero
        ide = root.xpath(".//nfe:ide", namespaces=ns)
        if ide:
            ide = ide[0]
            nota.modelo = ide.xpath("string(nfe:mod)", namespaces=ns)
            nota.serie = ide.xpath("string(nfe:serie)", namespaces=ns)
            nota.numero_inicial = ide.xpath("string(nfe:nNF)", namespaces=ns)
            nota.numero_final = nota.numero_inicial
            nota.data_emissao = ide.xpath("string(nfe:dhEmi)", namespaces=ns)

        # inutilizadas
        if nota.tipo_documento == "NFe Inutilizada":
            infinut = root.xpath(".//nfe:infInut", namespaces=ns)
            if infinut:
                infinut = infinut[0]
                nota.numero_inicial = infinut.xpath("string(nfe:nNFIni)", namespaces=ns) or nota.numero_inicial
                nota.numero_final = infinut.xpath("string(nfe:nNFFin)", namespaces=ns) or nota.numero_final

        # ✅ Detectar evento de cancelamento
        inf_evento = root.xpath(".//nfe:infEvento", namespaces=ns)
        if inf_evento:
            tp = inf_evento[0].xpath("string(nfe:tpEvento)", namespaces=ns)
            if tp == "110111":
                nota.tipo_documento = "NFe Cancelada (Evento)"

    except Exception as e:
        nota.erros.append(f"Erro XML: {e}")

    return nota

# --- DEMAIS FUNÇÕES (idênticas, sem alteração lógica) ---
def agrupar_lacunas(numeros: list[int]) -> str:
    if not numeros: return ""
    numeros = sorted(numeros)
    resultado, inicio = [], numeros[0]
    for i in range(1, len(numeros)):
        if numeros[i] != numeros[i-1] + 1:
            fim = numeros[i-1]
            resultado.append(str(inicio) if inicio == fim else f"{inicio}-{fim}")
            inicio = numeros[i]
    fim = numeros[-1]
    resultado.append(str(inicio) if inicio == fim else f"{inicio}-{fim}")
    return ", ".join(resultado)

def gerar_relatorios(lista_dados_notas, pasta_destino, tempo_execucao):
    # (seu código original, sem mudanças, apenas omitido aqui para encurtar)
    ...
    return resumo_path, csv_path

# ✅ FUNÇÃO PRINCIPAL COM TQDM + ProcessPoolExecutor
def run_analysis(xml_files_in_memory: dict[str, bytes], pasta_destino: Path, numeros_para_copiar: set[int]) -> dict:

    start = time.time()
    total = len(xml_files_in_memory)

    if total == 0:
        raise ValueError("Nenhum XML enviado.")

    pasta_destino.mkdir(parents=True, exist_ok=True)
    pasta_copiados = pasta_destino / "xmls_copiados"
    pasta_copiados.mkdir(exist_ok=True)

    lista_dados_notas = []

    # ✅ uso de ProcessPool (mais rápido para parsing)
    with ProcessPoolExecutor() as executor:
        futures = {
            executor.submit(obter_dados_xml_de_conteudo, filename, content): filename
            for filename, content in xml_files_in_memory.items()
        }

        # ✅ tqdm para barra de progresso
        for future in tqdm(as_completed(futures), total=total, desc="Processando XMLs"):
            lista_dados_notas.append(future.result())

    # ✅ resto do código igual:
    copiados = 0
    for nota in lista_dados_notas:
        try:
            if nota.numero_inicial and nota.numero_inicial.isdigit():
                num = int(nota.numero_inicial)
                if num in numeros_para_copiar:
                    destino = pasta_copiados / nota.arquivo_path.name
                    destino.write_bytes(xml_files_in_memory[nota.arquivo_path.name])
                    nota.foi_copiado = True
                    copiados += 1
        except Exception as e:
            nota.erros.append(f"Erro ao copiar: {e}")

    resumo_path, csv_path = gerar_relatorios(
        lista_dados_notas, pasta_destino, round(time.time() - start, 2)
    )

    zip_path = pasta_destino / f"resultados_{datetime.now():%Y%m%d_%H%M%S}.zip"
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(resumo_path, resumo_path.name)
        zf.write(csv_path, csv_path.name)
        for file_path in pasta_copiados.glob("*"):
            zf.write(file_path, f"xmls_copiados/{file_path.name}")

    return {
        "zip_path": zip_path,
        "summary_path": resumo_path,
    }
