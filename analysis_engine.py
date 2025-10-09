import logging
import csv
import time
import zipfile
import textwrap
import datetime
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict, Counter
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
        content_str = file_content.decode('utf-8', errors='replace')
        root = ET.fromstring(content_str)
        ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

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

def agrupar_lacunas(numeros: list[int]) -> str:
    """Agrupa uma lista de números em intervalos para melhor legibilidade."""
    if not numeros: return ""
    numeros = sorted(numeros)
    resultado, inicio_intervalo = [], numeros[0]
    for i in range(1, len(numeros)):
        if numeros[i] != numeros[i-1] + 1:
            fim_intervalo = numeros[i-1]
            resultado.append(str(inicio_intervalo) if inicio_intervalo == fim_intervalo else f"{inicio_intervalo}-{fim_intervalo}")
            inicio_intervalo = numeros[i]
    fim_intervalo = numeros[-1]
    resultado.append(str(inicio_intervalo) if inicio_intervalo == fim_intervalo else f"{inicio_intervalo}-{fim_intervalo}")
    return ", ".join(resultado)

ef gerar_relatorios(lista_dados_notas: list[DadosNota], pasta_destino: Path, tempo_execucao: float | None = None) -> tuple[Path, Path]:
    """Gera relatórios de resumo (.txt) e detalhado (.csv) com análise de sequência."""
    logging.info("Iniciando geração de relatórios aprimorados...")

    resumo_path = pasta_destino / "resumo_analise.txt"
    csv_path = pasta_destino / "relatorio_detalhado.csv"
    
    total_xmls = len(lista_dados_notas)
    if total_xmls == 0:
        logging.warning("Nenhuma nota para processar nos relatórios.")
        # Cria relatórios vazios para evitar erros
        resumo_path.touch()
        csv_path.touch()
        return resumo_path, csv_path

    com_erro = sum(1 for n in lista_dados_notas if n.erros)
    contagem_status = Counter(n.tipo_documento for n in lista_dados_notas)

    # Agrupa notas AUTORIZADAS por (modelo, serie) para análise de sequência
    dados_por_serie = defaultdict(list)
    outras_notas = []
    for nota in lista_dados_notas:
        if nota.tipo_documento == "NFe Autorizada" and nota.modelo and nota.serie and nota.numero_inicial and nota.numero_inicial.isdigit():
            chave = (nota.modelo, nota.serie)
            dados_por_serie[chave].append(nota)
        else:
            outras_notas.append(nota)

    # --- 1. GERAÇÃO DO RESUMO EM TEXTO (.txt) ---
    with resumo_path.open('w', encoding='utf-8') as f:
        f.write("=" * 100 + "\n")
        f.write(f"{'RELATÓRIO DE ANÁLISE DE DOCUMENTOS FISCAIS':^100}\n")
        f.write("=" * 100 + "\n")
        f.write(f"Data da Análise: {datetime.now():%d/%m/%Y %H:%M:%S}\n")
        if tempo_execucao is not None:
            f.write(f"Tempo total de execução: {tempo_execucao:.2f} segundos\n")
        f.write(f"Total de Arquivos Processados: {total_xmls}\n")
        f.write(f"XMLs com Erro de Leitura: {com_erro}\n\n")

        f.write("-" * 100 + "\n")
        f.write(f"{'SUMÁRIO DE STATUS DOS DOCUMENTOS':^100}\n")
        f.write("-" * 100 + "\n")
        for status, qtd in sorted(contagem_status.items()):
            percentual = (qtd / total_xmls * 100)
            f.write(f"- {status:<35}: {qtd:<6} ({percentual:.2f}%)\n")
        f.write("\n")

        f.write("=" * 100 + "\n")
        f.write(f"{'ANÁLISE DE SEQUÊNCIA NUMÉRICA (NF-e AUTORIZADAS)':^100}\n")
        f.write("=" * 100 + "\n\n")

        if not dados_por_serie:
            f.write("Nenhuma NF-e autorizada encontrada para análise de sequência.\n")
        else:
            hdr = f"{'Modelo':<7} {'Série':<6} {'Intervalo':<22} {'Qtde Encontrada':>16} {'Pulos':>7} {'% Pulos':>9}  {'Situação'}"
            f.write(hdr + "\n")
            f.write("-" * 100 + "\n")

            for (modelo, serie), notas in sorted(dados_por_serie.items()):
                numeros = sorted([int(n.numero_inicial) for n in notas])
                min_n, max_n = numeros[0], numeros[-1]
                
                intervalo_total = set(range(min_n, max_n + 1))
                numeros_encontrados_set = set(numeros)
                
                faltantes = sorted(list(intervalo_total - numeros_encontrados_set))
                qtd_faltantes = len(faltantes)
                
                total_no_intervalo = len(intervalo_total)
                percentual_pulos = (qtd_faltantes / total_no_intervalo * 100) if total_no_intervalo > 0 else 0.0

                situacao = "COMPLETA" if qtd_faltantes == 0 else "INCOMPLETA"

                row = (f"{str(modelo):<7} {str(serie):<6} {f'{min_n} a {max_n}':<22} "
                       f"{len(numeros):>16} {qtd_faltantes:>7} {f'{percentual_pulos:.2f}%':>8}  {situacao}")
                f.write(row + "\n")
                
                if qtd_faltantes > 0:
                    lacunas_formatadas = agrupar_lacunas(faltantes)
                    linhas_quebradas = textwrap.wrap(f"   +- Números Faltantes: {lacunas_formatadas}", width=98, subsequent_indent='      ')
                    for linha in linhas_quebradas:
                        f.write(linha + "\n")
                f.write("\n")

    # --- 2. GERAÇÃO DO CSV DETALHADO ---
    headers = [
        "modelo", "serie", "numero_nota", "data_emissao", "tipo_documento",
        "status_sefaz_cod", "status_sefaz_motivo", "chave_acesso",
        "arquivo_origem", "foi_copiado", "situacao_numeracao", "erros"
    ]
    
    with csv_path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(headers)

        # Processa e analisa a sequência de notas autorizadas
        for (modelo, serie), notas in sorted(dados_por_serie.items()):
            notas_dict = {int(n.numero_inicial): n for n in notas}
            numeros = sorted(notas_dict.keys())
            min_n, max_n = numeros[0], numeros[-1]

            for numero_atual in range(min_n, max_n + 1):
                nota = notas_dict.get(numero_atual)
                if nota: # A nota para este número existe
                    writer.writerow([
                        nota.modelo, nota.serie, nota.numero_inicial,
                        _formatar_data(nota.data_emissao), nota.tipo_documento,
                        nota.status_code, nota.status_text, nota.chave_acesso,
                        nota.arquivo_path.name, "Sim" if nota.foi_copiado else "Não",
                        "Presente", "; ".join(nota.erros)
                    ])
                else: # A nota para este número está faltando
                    writer.writerow([
                        modelo, serie, numero_atual, "", "Ausente",
                        "", "", "", "", "Não", "Faltante", ""
                    ])
        
        # Adiciona as outras notas (Canceladas, Inutilizadas, etc.) no final
        for nota in sorted(outras_notas, key=lambda n: (n.modelo, n.serie, n.numero_inicial)):
             writer.writerow([
                nota.modelo, nota.serie, nota.numero_inicial,
                _formatar_data(nota.data_emissao), nota.tipo_documento,
                nota.status_code, nota.status_text, nota.chave_acesso,
                nota.arquivo_path.name, "Sim" if nota.foi_copiado else "Não",
                "N/A", "; ".join(nota.erros)
            ])

    logging.info("Relatórios gerados com sucesso.")
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
    
    logging.info("Processando XMLs em paralelo...")
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(obter_dados_xml_de_conteudo, filename, content): (filename, content)
                   for filename, content in xml_files_in_memory.items()}
        for future in as_completed(futures):
            nota = future.result()
            lista_dados_notas.append(nota)
    logging.info("Análise de todos os XMLs concluída.")

    logging.info("Verificando arquivos para cópia...")
    copiados = 0
    for nota in lista_dados_notas:
        try:
            numero = int(nota.numero_inicial) if nota.numero_inicial and nota.numero_inicial.isdigit() else None
            if numero and numero in numeros_para_copiar:
                filename_original = nota.arquivo_path.name
                if filename_original in xml_files_in_memory:
                    file_content = xml_files_in_memory[filename_original]
                    destino = pasta_copiados / filename_original
                    destino.write_bytes(file_content)
                    nota.foi_copiado = True
                    copiados += 1
        except Exception as e:
            nota.erros.append(f"Falha ao copiar XML: {e}")

    resumo_path, csv_path = gerar_relatorios(lista_dados_notas, pasta_destino, tempo_execucao=round(time.time() - start_time, 2))

    logging.info("Compactando resultados manualmente para maior robustez...")
    zip_filename = f"resultados_{datetime.now():%Y%m%d_%H%M%S}.zip"
    zip_filepath = pasta_destino / zip_filename
    
    with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(resumo_path, arcname=resumo_path.name)
        zf.write(csv_path, arcname=csv_path.name)

        arquivos_a_copiar = list(pasta_copiados.glob('*'))
        if arquivos_a_copiar:
            for file_path in arquivos_a_copiar:
                zf.write(file_path, arcname=f"xmls_copiados/{file_path.name}")

    elapsed = round(time.time() - start_time, 2)
    logging.info(f"Análise finalizada: {len(lista_dados_notas)} XMLs processados, {copiados} copiados ({elapsed}s).")

    return {
        "zip_path": zip_filepath,
        "summary_path": resumo_path,
    }