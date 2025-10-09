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


def parse_numeros(raw_str: str) -> set[int]:
    """Converte string de números separados por vírgula em set de inteiros válidos."""
    numeros = set()
    for n in raw_str.split(","):
        n = n.strip()
        if n.isdigit():
            numeros.add(int(n))
        elif n:
            logging.warning(f"Valor inválido ignorado: {n}")
    return numeros


def obter_dados_xml(arquivo: Path) -> DadosNota:
    """Lê e extrai dados de um XML de NFe. Retorna DadosNota com metadados básicos."""
    nota = DadosNota(arquivo_path=arquivo)
    try:
        tree = ET.parse(arquivo)
        root = tree.getroot()

        # Lê campos básicos (ajuste conforme estrutura)
        nota.chave_acesso = root.findtext(".//infNFe/@Id", "")[-44:]
        nota.modelo = root.findtext(".//mod", "")
        nota.serie = root.findtext(".//serie", "")
        nota.numero_inicial = root.findtext(".//nNF", "")
        nota.data_emissao = root.findtext(".//dhEmi", "") or root.findtext(".//dEmi", "")
        nota.status_code = root.findtext(".//cStat", "")
        nota.status_text = root.findtext(".//xMotivo", "")
        nota.tipo_documento = _mapear_cstat_para_tipo(nota.status_code)

    except ET.ParseError as e:
        nota.erros.append(f"XML inválido: {e}")
        logging.warning(f"Falha ao ler {arquivo.name}: {e}")
    except Exception as e:
        nota.erros.append(f"Erro inesperado: {e}")
        logging.error(f"Erro ao processar {arquivo}: {e}", exc_info=True)
    return nota


def _mapear_cstat_para_tipo(cstat: str) -> str:
    """Mapeia código cStat em tipo de documento (Emitida, Cancelada, Denegada, etc)."""
    mapa = {
        "100": "Autorizada",
        "135": "Cancelada",
        "301": "Denegada",
        "302": "Denegada",
        "110": "Complementar",
    }
    return mapa.get(cstat, "Outros")


def gerar_relatorios(lista_dados_notas: list[DadosNota], pasta_destino: Path, pasta_origem: Path) -> tuple[Path, Path]:
    """Gera relatório CSV e resumo de análise."""
    csv_path = pasta_destino / "relatorio.csv"
    resumo_path = pasta_destino / "resumo.txt"

    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Arquivo", "Chave", "Tipo", "Modelo", "Série", "Número", "Data", "Status", "Motivo", "Erros"])
        for n in lista_dados_notas:
            writer.writerow([
                n.arquivo_path.name,
                n.chave_acesso,
                n.tipo_documento,
                n.modelo,
                n.serie,
                n.numero_inicial,
                n.data_emissao,
                n.status_code,
                n.status_text,
                "; ".join(n.erros),
            ])

    total = len(lista_dados_notas)
    com_erro = sum(1 for n in lista_dados_notas if n.erros)
    tipos = Counter(n.tipo_documento for n in lista_dados_notas)

    with open(resumo_path, "w", encoding="utf-8") as f:
        f.write(f"Resumo da Análise - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
        f.write(f"Pasta analisada: {pasta_origem}\n\n")
        f.write(f"Total de XMLs analisados: {total}\n")
        f.write(f"Com erro: {com_erro}\n\n")
        f.write("Distribuição por tipo:\n")
        for tipo, count in tipos.items():
            f.write(f" - {tipo}: {count}\n")

    return resumo_path, csv_path


def run_analysis(pasta_origem: Path, pasta_destino: Path, numeros_para_copiar: set[int]) -> dict:
    """Executa análise completa dos XMLs e gera relatórios."""
    start = time.time()
    logging.info(f"Iniciando análise para a pasta: {pasta_origem}")

    pasta_copiados = pasta_destino / "xmls_copiados"
    pasta_copiados.mkdir(parents=True, exist_ok=True)

    arquivos_xml = [f for f in pasta_origem.rglob("*.xml") if f.suffix.lower() == ".xml"]
    total_arquivos = len(arquivos_xml)
    if total_arquivos == 0:
        raise ValueError("Nenhum arquivo .xml encontrado na pasta enviada.")

    logging.info(f"Foram encontrados {total_arquivos} arquivos XML.")

    lista_dados_notas: list[DadosNota] = []
    copiados = 0

    for idx, arquivo in enumerate(arquivos_xml, 1):
        nota = obter_dados_xml(arquivo)

        # Verifica se deve copiar
        try:
            numero = int(nota.numero_inicial) if nota.numero_inicial.isdigit() else None
            if numero and numero in numeros_para_copiar:
                destino = pasta_copiados / arquivo.name
                shutil.copy2(arquivo, destino)
                nota.foi_copiado = True
                copiados += 1
        except Exception as e:
            nota.erros.append(f"Falha ao copiar arquivo: {e}")

        lista_dados_notas.append(nota)
        if idx % 100 == 0 or idx == total_arquivos:
            logging.info(f"Processados {idx}/{total_arquivos} XMLs...")

    caminho_sumario, caminho_csv = gerar_relatorios(lista_dados_notas, pasta_destino, pasta_origem)

    zip_filename = f"resultados_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    zip_filepath = shutil.make_archive(
        base_name=str(pasta_destino / zip_filename),
        format="zip",
        root_dir=pasta_destino
    )

    elapsed = round(time.time() - start, 2)
    logging.info(f"Análise concluída em {elapsed}s - {copiados} arquivos copiados.")

    return {
        "zip_path": Path(zip_filepath),
        "summary_path": caminho_sumario,
        "total_xmls": total_arquivos,
        "copiados": copiados,
        "tempo_execucao": elapsed,
    }
