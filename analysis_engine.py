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

# ============================================
#  ESTRUTURA DE DADOS
# ============================================

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


# ============================================
#  FUNÇÕES AUXILIARES
# ============================================

def parse_numeros(raw_str: str) -> set[int]:
    """
    Converte "1,2,5-8, 10" em {1,2,5,6,7,8,10}.
    Ignora inválidos.
    """
    if not raw_str:
        return set()

    result = set()
    partes = raw_str.split(",")

    for parte in partes:
        parte = parte.strip()

        if "-" in parte:  # intervalo
            try:
                ini, fim = parte.split("-")
                ini, fim = int(ini), int(fim)
                result.update(range(ini, fim + 1))
            except:
                logging.warning(f"Valor inválido ignorado: {parte}")
        elif parte.isdigit():
            result.add(int(parte))
        elif parte:
            logging.warning(f"Valor inválido ignorado: {parte}")

    return result


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
        "100": "NFe Autorizada",
        "101": "NFe Cancelada",
        "135": "NFe Cancelada",
        "102": "NFe Inutilizada",
    }
    if cstat in mapping:
        return mapping[cstat]
    if cstat and cstat[:1] in ("2", "3"):
        return f"NFe com Rejeição ({cstat})"
    return f"Status Desconhecido ({cstat})"


# ============================================
#  PARSER DE XML (LXML)
# ============================================

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

        # chave de acesso
        chave = root.xpath("string(.//nfe:chNFe)", namespaces=ns).strip()
        if chave:
            nota.chave_acesso = chave

        # ide
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
                nota.numero_inicial = (
                    infinut.xpath("string(nfe:nNFIni)", namespaces=ns)
                    or nota.numero_inicial
                )
                nota.numero_final = (
                    infinut.xpath("string(nfe:nNFFin)", namespaces=ns)
                    or nota.numero_final
                )

        # evento de cancelamento
        inf_evento = root.xpath(".//nfe:infEvento", namespaces=ns)
        if inf_evento:
            tp = inf_evento[0].xpath("string(nfe:tpEvento)", namespaces=ns)
            if tp == "110111":
                nota.tipo_documento = "NFe Cancelada (Evento)"

    except Exception as e:
        nota.erros.append(f"Erro XML: {e}")

    return nota


# ============================================
#  AGRUPAMENTO DE LACUNAS
# ============================================

def agrupar_lacunas(numeros: list[int]) -> str:
    if not numeros:
        return ""
    numeros = sorted(numeros)
    resultado, inicio = [], numeros[0]

    for i in range(1, len(numeros)):
        if numeros[i] != numeros[i - 1] + 1:
            fim = numeros[i - 1]
            resultado.append(
                str(inicio) if inicio == fim else f"{inicio}-{fim}"
            )
            inicio = numeros[i]

    fim = numeros[-1]
    resultado.append(str(inicio) if inicio == fim else f"{inicio}-{fim}")
    return ", ".join(resultado)


# ============================================
#  GERAÇÃO DE RELATÓRIOS
# ============================================

def gerar_relatorios(lista_dados_notas, pasta_destino, tempo_execucao):
    logging.info("Iniciando geração de relatórios")

    resumo_path = pasta_destino / "resumo_analise.txt"
    csv_path = pasta_destino / "relatorio_detalhado.csv"

    total_xmls = len(lista_dados_notas)
    if total_xmls == 0:
        resumo_path.touch()
        csv_path.touch()
        return resumo_path, csv_path

    com_erro = sum(1 for n in lista_dados_notas if n.erros)
    contagem_status = Counter(n.tipo_documento for n in lista_dados_notas)

    # grupo por modelo/série
    dados_por_serie = defaultdict(list)
    outras_notas = []

    for nota in lista_dados_notas:
        if (
            nota.tipo_documento in ("NFe Autorizada", "NFe Cancelada", "NFe Inutilizada")
            and nota.modelo
            and nota.serie
            and nota.numero_inicial
            and nota.numero_inicial.isdigit()
        ):
            chave = (nota.modelo, nota.serie)
            dados_por_serie[chave].append(nota)
        else:
            outras_notas.append(nota)

    # --- resumo txt ---
    with resumo_path.open("w", encoding="utf-8") as f:
        f.write("=" * 100 + "\n")
        f.write(f"{'RELATÓRIO DE ANÁLISE DE DOCUMENTOS FISCAIS':^100}\n")
        f.write("=" * 100 + "\n")
        f.write(f"Data da Análise: {datetime.now():%d/%m/%Y %H:%M:%S}\n")
        f.write(f"Tempo de Execução: {tempo_execucao:.2f} segundos\n")
        f.write(f"Total de Arquivos Processados: {total_xmls}\n")
        f.write(f"XMLs com Erro de Leitura: {com_erro}\n\n")

        f.write("-" * 100 + "\n")
        f.write(f"{'STATUS DOS DOCUMENTOS':^100}\n")
        f.write("-" * 100 + "\n")
        for status, qtd in sorted(contagem_status.items()):
            perc = (qtd / total_xmls * 100)
            f.write(f"- {status:<35}: {qtd:<6} ({perc:.2f}%)\n")
        f.write("\n")

    # --- csv detalhado ---
    headers = [
        "modelo", "serie", "numero_nota", "data_emissao", "tipo_documento",
        "status_sefaz_cod", "status_sefaz_motivo", "chave_acesso",
        "arquivo_origem", "foi_copiado", "situacao_numeracao", "erros"
    ]

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(headers)

        for (modelo, serie), notas in sorted(dados_por_serie.items()):
            notas_dict = {int(n.numero_inicial): n for n in notas}
            numeros = sorted(notas_dict.keys())
            min_n, max_n = numeros[0], numeros[-1]

            for num in range(min_n, max_n + 1):
                nota = notas_dict.get(num)

                if nota:
                    writer.writerow([
                        nota.modelo, nota.serie, nota.numero_inicial,
                        _formatar_data(nota.data_emissao), nota.tipo_documento,
                        nota.status_code, nota.status_text, nota.chave_acesso,
                        nota.arquivo_path.name, "Sim" if nota.foi_copiado else "Não",
                        "Presente", "; ".join(nota.erros)
                    ])
                else:
                    writer.writerow([
                        modelo, serie, num, "",
                        "Ausente", "", "", "", "", "Não", "Faltante", ""
                    ])

        for nota in sorted(outras_notas, key=lambda n: (n.modelo, n.serie, n.numero_inicial)):
            writer.writerow([
                nota.modelo, nota.serie, nota.numero_inicial,
                _formatar_data(nota.data_emissao), nota.tipo_documento,
                nota.status_code, nota.status_text, nota.chave_acesso,
                nota.arquivo_path.name, "Não", "N/A", "; ".join(nota.erros)
            ])

    logging.info("Relatórios gerados com sucesso.")
    return resumo_path, csv_path


# ============================================
#  FUNÇÃO PRINCIPAL
# ============================================

def run_analysis(xml_files_in_memory: dict[str, bytes], pasta_destino: Path, numeros_para_copiar: set[int]) -> dict:
    start = time.time()
    total = len(xml_files_in_memory)
    logging.info(f"Iniciando análise de {total} XMLs...")

    if total == 0:
        raise ValueError("Nenhum XML foi enviado para análise.")

    pasta_destino.mkdir(parents=True, exist_ok=True)
    pasta_copiados = pasta_destino / "xmls_copiados"
    pasta_copiados.mkdir(exist_ok=True)

    lista_dados_notas = []

    # processamento paralelo + tqdm
    with ProcessPoolExecutor() as executor:
        futures = {
            executor.submit(obter_dados_xml_de_conteudo, filename, content): filename
            for filename, content in xml_files_in_memory.items()
        }

        for future in tqdm(as_completed(futures), total=total, desc="Processando XMLs"):
            lista_dados_notas.append(future.result())

    # cópia dos XMLs escolhidos
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
            nota.erros.append(f"Erro ao copiar XML: {e}")

    resumo_path, csv_path = gerar_relatorios(
        lista_dados_notas, pasta_destino, round(time.time() - start, 2)
    )

    # compactar
    zip_path = pasta_destino / f"resultados_{datetime.now():%Y%m%d_%H%M%S}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(resumo_path, resumo_path.name)
        zf.write(csv_path, csv_path.name)
        for file_path in pasta_copiados.glob("*"):
            zf.write(file_path, f"xmls_copiados/{file_path.name}")

    return {
        "zip_path": zip_path,
        "summary_path": resumo_path,
    }
