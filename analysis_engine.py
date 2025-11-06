import logging
import csv
import time
import zipfile
import textwrap
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from lxml import etree

NFE_NAMESPACE = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

STATUS_AUTORIZADA = "NFe Autorizada"
STATUS_CANCELADA = "NFe Cancelada"
STATUS_INUTILIZADA = "NFe Inutilizada"
STATUS_REJEITADA_PREFIX = "NFe com Rejeição"
STATUS_DESCONHECIDO_PREFIX = "Status Desconhecido"


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

def _formatar_data(iso_str: str) -> str:
    """Converte uma string de data ISO 8601 para DD/MM/AAAA HH:MM de forma robusta."""
    if not iso_str or "T" not in iso_str:
        return iso_str
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.strftime("%d/%m/%Y %H:%M")
    except (ValueError, TypeError):
        logging.warning(f"Não foi possível formatar a data: {iso_str}")
        return iso_str

def parse_numeros(raw_str: str) -> set[int]:
    """
    Converte "1,2,5-8, 10" em {1,2,5,6,7,8,10}.
    Suporta intervalos, valores repetidos e ignora inválidos.
    """
    if not raw_str:
        return set()
    
    result = set()
    partes = raw_str.split(",")

    for parte in partes:
        parte = parte.strip()

        if "-" in parte:  
            try:
                ini_str, fim_str = parte.split("-")
                ini, fim = int(ini_str), int(fim_str)
                if ini <= fim:
                    result.update(range(ini, fim + 1))
                else:
                    logging.warning(f"Intervalo invertido ignorado: {parte}")
            except (ValueError, TypeError):
                logging.warning(f"Valor de intervalo inválido ignorado: {parte}")
        
        elif parte.isdigit():
            result.add(int(parte))
        
        elif parte:
            logging.warning(f"Valor numérico inválido ignorado: {parte}")
            
    return result


def _mapear_cstat_para_tipo(cstat: str) -> str:
    """Mapeia o cStat para um tipo de documento (usando constantes)."""
    mapping = {
        '100': STATUS_AUTORIZADA,
        '101': STATUS_CANCELADA,
        '135': STATUS_CANCELADA, 
        '102': STATUS_INUTILIZADA,
    }
    if cstat in mapping:
        return mapping[cstat]
    if cstat and cstat.startswith(('2', '3')):
        return f"{STATUS_REJEITADA_PREFIX} ({cstat})"
    return f"{STATUS_DESCONHECIDO_PREFIX} ({cstat})"


def obter_dados_xml_de_conteudo(filename: str, file_content: bytes) -> DadosNota:
    """
    Extrai dados do XML usando lxml para performance e robustez.
    O 'recover=True' é crucial para XMLs com pequenos erros.
    """
    nota = DadosNota(arquivo_path=Path(filename))
    try:
        # 1. 'recover=True' tenta corrigir erros de XML
        parser = etree.XMLParser(recover=True, encoding="utf-8")
        root = etree.fromstring(file_content, parser=parser)
        
        # 2. xpath com string() é mais limpo e seguro que .findtext
        cstat = root.xpath("string(.//nfe:cStat)", namespaces=NFE_NAMESPACE).strip()
        motivo = root.xpath("string(.//nfe:xMotivo)", namespaces=NFE_NAMESPACE).strip()
        
        nota.status_code = cstat or "N/A"
        nota.status_text = motivo or "N/A"
        nota.tipo_documento = _mapear_cstat_para_tipo(cstat)

        # 3. Busca pela chave de acesso (funciona em NFe, CTe, etc.)
        nota.chave_acesso = root.xpath("string(.//nfe:chNFe)", namespaces=NFE_NAMESPACE).strip()
        
        # 4. Dados da <ide>
        ide = root.xpath(".//nfe:ide", namespaces=NFE_NAMESPACE)
        if ide:
            ide = ide[0]
            nota.modelo = ide.xpath("string(nfe:mod)", namespaces=NFE_NAMESPACE)
            nota.serie = ide.xpath("string(nfe:serie)", namespaces=NFE_NAMESPACE)
            nota.numero_inicial = ide.xpath("string(nfe:nNF)", namespaces=NFE_NAMESPACE)
            nota.numero_final = nota.numero_inicial
            nota.data_emissao = ide.xpath("string(nfe:dhEmi)", namespaces=NFE_NAMESPACE)

        # 5. Sobrescreve dados se for Inutilização
        if nota.tipo_documento == STATUS_INUTILIZADA:
            infinut = root.xpath(".//nfe:infInut", namespaces=NFE_NAMESPACE)
            if infinut:
                infinut = infinut[0]
                nota.numero_inicial = (
                    infinut.xpath("string(nfe:nNFIni)", namespaces=NFE_NAMESPACE)
                    or nota.numero_inicial
                )
                nota.numero_final = (
                    infinut.xpath("string(nfe:nNFFin)", namespaces=NFE_NAMESPACE)
                    or nota.numero_final
                )

        # 6. Identifica Evento de Cancelamento (mais robusto)
        inf_evento = root.xpath(".//nfe:infEvento", namespaces=NFE_NAMESPACE)
        if inf_evento:
            tp = inf_evento[0].xpath("string(nfe:tpEvento)", namespaces=NFE_NAMESPACE)
            if tp == "110111": # Código do evento de cancelamento
                nota.tipo_documento = f"{STATUS_CANCELADA} (Evento)"

    except Exception as e:
        nota.erros.append(f"Erro ao ler XML: {e}")
    return nota


def agrupar_lacunas(numeros: list[int]) -> str:
    """(Sem alterações) Agrupa uma lista de números em intervalos."""
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

def _preparar_dados_relatorio(lista_dados_notas: list[DadosNota]) -> tuple[dict, list]:
    """Separa as notas em 'dados_por_serie' (para análise) e 'outras_notas'."""
    dados_por_serie = defaultdict(list)
    outras_notas = []
    
    # Usamos as constantes para o filtro
    tipos_validos_para_sequencia = {
        STATUS_AUTORIZADA, 
        STATUS_CANCELADA, 
        STATUS_INUTILIZADA
    }

    for nota in lista_dados_notas:
        if nota.tipo_documento in tipos_validos_para_sequencia \
           and nota.modelo and nota.serie and nota.numero_inicial and nota.numero_inicial.isdigit():
            
            chave = (nota.modelo, nota.serie)
            dados_por_serie[chave].append(nota)
        else:
            outras_notas.append(nota)
            
    return dados_por_serie, outras_notas


def _gerar_relatorio_txt(
    resumo_path: Path, 
    lista_dados_notas: list[DadosNota], 
    dados_por_serie: dict,
    tempo_execucao: float | None
):
    """Gera o arquivo de resumo .txt."""
    total_xmls = len(lista_dados_notas)
    com_erro = sum(1 for n in lista_dados_notas if n.erros)
    contagem_status = Counter(n.tipo_documento for n in lista_dados_notas)
    
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
            percentual = (qtd / total_xmls * 100) if total_xmls > 0 else 0
            f.write(f"- {status:<35}: {qtd:<6} ({percentual:.2f}%)\n")
        f.write("\n")

        f.write("=" * 100 + "\n")
        f.write(f"{'ANÁLISE DE SEQUÊNCIA NUMÉRICA (NF-e)':^100}\n")
        f.write("=" * 100 + "\n\n")

        if not dados_por_serie:
            f.write("Nenhuma nota encontrada para análise de sequência.\n")
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

                percentual_pulos = (qtd_faltantes / len(intervalo_total) * 100) if intervalo_total else 0.0
                situacao = "COMPLETA" if qtd_faltantes == 0 else "INCOMPLETA"

                row = f"{modelo:<7} {serie:<6} {f'{min_n} a {max_n}':<22} {len(numeros):>16} {qtd_faltantes:>7} {percentual_pulos:>8.2f}%  {situacao}"
                f.write(row + "\n")

                if qtd_faltantes > 0:
                    lacunas_formatadas = agrupar_lacunas(faltantes)
                    linhas_quebradas = textwrap.wrap(f"   +- Números Faltantes: {lacunas_formatadas}", width=98, subsequent_indent='      ')
                    for linha in linhas_quebradas:
                        f.write(linha + "\n")
                f.write("\n")


def _gerar_relatorio_csv(csv_path: Path, dados_por_serie: dict, outras_notas: list[DadosNota]):
    """Gera o arquivo de relatório detalhado .csv."""
    headers = [
        "modelo", "serie", "numero_nota", "data_emissao", "tipo_documento",
        "status_sefaz_cod", "status_sefaz_motivo", "chave_acesso",
        "arquivo_origem", "foi_copiado", "situacao_numeracao", "erros"
    ]

    with csv_path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(headers)

        # 1. Notas da análise de sequência
        for (modelo, serie), notas in sorted(dados_por_serie.items()):
            notas_dict = {int(n.numero_inicial): n for n in notas}
            numeros = sorted(notas_dict.keys())
            min_n, max_n = numeros[0], numeros[-1]

            for num in range(min_n, max_n + 1):
                nota = notas_dict.get(num)
                
                if nota:  # Nota PRESENTE (Autorizada, Cancelada ou Inutilizada)
                    writer.writerow([
                        nota.modelo, nota.serie, nota.numero_inicial,
                        _formatar_data(nota.data_emissao), nota.tipo_documento,
                        nota.status_code, nota.status_text, nota.chave_acesso,
                        nota.arquivo_path.name, "Sim" if nota.foi_copiado else "Não",
                        "Presente", "; ".join(nota.erros)
                    ])
                else:  # Nota FALTANTE (lacuna)
                    writer.writerow([
                        modelo, serie, num, "", "Ausente",
                        "", "", "", "", "Não", "Faltante", ""
                    ])

        # 2. Outras notas (Rejeitadas, erros de parse, etc.)
        for nota in sorted(outras_notas, key=lambda n: (n.modelo, n.serie, n.numero_inicial)):
            writer.writerow([
                nota.modelo, nota.serie, nota.numero_inicial,
                _formatar_data(nota.data_emissao), nota.tipo_documento,
                nota.status_code, nota.status_text, nota.chave_acesso,
                nota.arquivo_path.name, "Sim" if nota.foi_copiado else "Não",
                "N/A", "; ".join(nota.erros)
            ])


def gerar_relatorios(lista_dados_notas: list[DadosNota], pasta_destino: Path, tempo_execucao: float | None = None) -> tuple[Path, Path]:
    """
    Função "Gerente": coordena a preparação dos dados e a
    geração dos relatórios TXT e CSV.
    """
    logging.info("Iniciando geração de relatórios")

    resumo_path = pasta_destino / "resumo_analise.txt"
    csv_path = pasta_destino / "relatorio_detalhado.csv"

    if not lista_dados_notas:
        logging.warning("Nenhuma nota para gerar relatórios. Arquivos vazios serão criados.")
        resumo_path.touch()
        csv_path.touch()
        return resumo_path, csv_path

    # 1. Preparar dados
    dados_por_serie, outras_notas = _preparar_dados_relatorio(lista_dados_notas)

    # 2. Delegar geração do TXT
    _gerar_relatorio_txt(
        resumo_path, 
        lista_dados_notas, 
        dados_por_serie, 
        tempo_execucao
    )

    # 3. Delegar geração do CSV
    _gerar_relatorio_csv(
        csv_path,
        dados_por_serie,
        outras_notas
    )

    logging.info("Relatórios gerados com sucesso.")
    return resumo_path, csv_path


# --- FUNÇÃO PRINCIPAL (Sem grandes alterações) ---
def run_analysis(xml_files_in_memory: dict[str, bytes], pasta_destino: Path, numeros_para_copiar: set[int]) -> dict:
    start_time = time.time()
    total_arquivos = len(xml_files_in_memory)
    logging.info(f"Iniciando análise de {total_arquivos} XMLs...")

    if total_arquivos == 0:
        raise ValueError("Nenhum arquivo XML foi enviado para análise.")

    pasta_destino.mkdir(parents=True, exist_ok=True)
    pasta_copiados = pasta_destino / "xmls_copiados"
    pasta_copiados.mkdir(exist_ok=True)

    lista_dados_notas = []


    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(obter_dados_xml_de_conteudo, filename, content): (filename, content)
                   for filename, content in xml_files_in_memory.items()}
        
            
        for future in as_completed(futures):
            lista_dados_notas.append(future.result())


    logging.info("Fase de parsing concluída. Copiando arquivos selecionados...")
    copiados = 0
    for nota in lista_dados_notas:
        try:
            numero = int(nota.numero_inicial) if nota.numero_inicial and nota.numero_inicial.isdigit() else None
            if numero and numero in numeros_para_copiar:
                filename_original = nota.arquivo_path.name
                if filename_original in xml_files_in_memory:
                    destino = pasta_copiados / filename_original
                    destino.write_bytes(xml_files_in_memory[filename_original])
                    nota.foi_copiado = True
                    copiados += 1
                else:
                    nota.erros.append(f"Falha ao copiar: Arquivo '{filename_original}' não encontrado na memória.")
        except Exception as e:
            nota.erros.append(f"Falha ao copiar XML: {e}")

    logging.info(f"{copiados} arquivos copiados. Gerando relatórios...")
    
    # Chama a função "gerente" refatorada
    resumo_path, csv_path = gerar_relatorios(
        lista_dados_notas, 
        pasta_destino, 
        tempo_execucao=round(time.time() - start_time, 2)
    )

    logging.info("Compactando resultados...")
    zip_filepath = pasta_destino / f"resultados_{datetime.now():%Y%m%d_%H%M%S}.zip"
    with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(resumo_path, arcname=resumo_path.name)
        zf.write(csv_path, arcname=csv_path.name)

        for file_path in pasta_copiados.glob('*'):
            zf.write(file_path, arcname=f"xmls_copiados/{file_path.name}")

    elapsed = round(time.time() - start_time, 2)
    logging.info(f"Análise finalizada: {len(lista_dados_notas)} XMLs processados, {copiados} copiados ({elapsed}s).")

    return {
        "zip_path": zip_filepath,
        "summary_path": resumo_path,
    }