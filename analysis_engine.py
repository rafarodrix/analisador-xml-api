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
    # [MELHORIA 1] Novo campo para armazenar o CNPJ de quem emitiu a nota.
    cnpj_emitente: str = "" 
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

# [MELHORIA BÔNUS]
# Esta função 'parse_numeros' que você colou é uma versão
# mais simples que não aceita intervalos (ex: "5-8").
# Estou restaurando a versão mais robusta (que você usou no script lxml)
# que aceita intervalos.
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

        if "-" in parte:  # Suporte a intervalo (ex: 5-9)
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
        
        # [MELHORIA 2] Extrair o CNPJ ou CPF do emitente
        # Isso nos permite filtrar pela *sua* empresa.
        cnpj_emit = (root.findtext('.//nfe:emit/nfe:CNPJ', '', ns) or '').strip()
        cpf_emit = (root.findtext('.//nfe:emit/nfe:CPF', '', ns) or '').strip()
        # Armazena apenas os dígitos para comparação fácil
        id_emitente = cnpj_emit or cpf_emit
        nota.cnpj_emitente = "".join(filter(str.isdigit, id_emitente))

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
    # ... (código sem alteração)
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

def gerar_relatorios(lista_dados_notas: list[DadosNota], pasta_destino: Path, tempo_execucao: float | None = None) -> tuple[Path, Path]:
    # ... (código sem alteração)
    # Esta função já funciona corretamente, pois ela vai receber
    # apenas a 'lista_dados_notas' JÁ FILTRADA.
    logging.info("Iniciando geração de relatórios")

    resumo_path = pasta_destino / "resumo_analise.txt"
    csv_path = pasta_destino / "relatorio_detalhado.csv"

    total_xmls = len(lista_dados_notas)
    if total_xmls == 0:
        # [MELHORIA] Adiciona um log e um resumo mais claro se nada for encontrado
        logging.warning("Nenhum XML correspondeu ao filtro da empresa. Relatórios vazios gerados.")
        with resumo_path.open('w', encoding='utf-8') as f:
            f.write("RELATÓRIO DE ANÁLISE\n")
            f.write("=" * 100 + "\n")
            f.write(f"Data da Análise: {datetime.now():%d/%m/%Y %H:%M:%S}\n")
            f.write("\nNENHUM XML ENCONTRADO.\n")
            f.write("Verifique se o CNPJ da empresa foi digitado corretamente ou se os XMLs enviados são de emitentes terceiros.\n")
        
        with csv_path.open('w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(["Nenhum dado encontrado"])
            
        return resumo_path, csv_path

    com_erro = sum(1 for n in lista_dados_notas if n.erros)
    contagem_status = Counter(n.tipo_documento for n in lista_dados_notas)

    dados_por_serie = defaultdict(list)
    outras_notas = []
    # ... (resto da função sem alteração) ...
    for nota in lista_dados_notas:
        if nota.tipo_documento in ("NFe Autorizada", "NFe Cancelada", "NFe Inutilizada") \
            and nota.modelo and nota.serie and nota.numero_inicial and nota.numero_inicial.isdigit():
            chave = (nota.modelo, nota.serie)
            dados_por_serie[chave].append(nota)
        else:
            outras_notas.append(nota)
    
    with resumo_path.open('w', encoding='utf-8') as f:
        f.write("=" * 100 + "\n")
        f.write(f"{'RELATÓRIO DE ANÁLISE DE DOCUMENTOS FISCAIS':^100}\n")
        f.write("=" * 100 + "\n")
        f.write(f"Data da Análise: {datetime.now():%d/%m/%Y %H:%M:%S}\n")
        if tempo_execucao is not None:
            f.write(f"Tempo total de execução: {tempo_execucao:.2f} segundos\n")
        f.write(f"Total de Arquivos Processados (após filtro): {total_xmls}\n")
        f.write(f"XMLs com Erro de Leitura: {com_erro}\n\n")

        f.write("-" * 100 + "\n")
        f.write(f"{'SUMÁRIO DE STATUS DOS DOCUMENTOS':^100}\n")
        f.write("-" * 100 + "\n")
        for status, qtd in sorted(contagem_status.items()):
            percentual = (qtd / total_xmls * 100)
            f.write(f"- {status:<35}: {qtd:<6} ({percentual:.2f}%)\n")
        f.write("\n")
        # ... (resto da função sem alteração) ...
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
    
    headers = [
        "modelo", "serie", "numero_nota", "data_emissao", "tipo_documento",
        "status_sefaz_cod", "status_sefaz_motivo", "chave_acesso",
        "cnpj_emitente", # Adicionado para verificação
        "arquivo_origem", "foi_copiado", "situacao_numeracao", "erros"
    ]
    with csv_path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter=';')
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
                        nota.cnpj_emitente, # Adicionado
                        nota.arquivo_path.name, "Sim" if nota.foi_copiado else "Não",
                        "Presente", "; ".join(nota.erros)
                    ])
                else:
                    writer.writerow([
                        modelo, serie, num, "", "Ausente",
                        "", "", "", "", "", "Não", "Faltante", "" # Adicionado "" para cnpj_emitente
                    ])
        for nota in sorted(outras_notas, key=lambda n: (n.modelo, n.serie, n.numero_inicial)):
            writer.writerow([
                nota.modelo, nota.serie, nota.numero_inicial,
                _formatar_data(nota.data_emissao), nota.tipo_documento,
                nota.status_code, nota.status_text, nota.chave_acesso,
                nota.cnpj_emitente, # Adicionado
                nota.arquivo_path.name, "Sim" if nota.foi_copiado else "Não",
                "N/A", "; ".join(nota.erros)
            ])

    logging.info("Relatórios gerados com sucesso.")
    return resumo_path, csv_path


# --- FUNÇÃO PRINCIPAL ---
def run_analysis(
    xml_files_in_memory: dict[str, bytes], 
    pasta_destino: Path, 
    numeros_para_copiar: set[int],
    # [MELHORIA 3] Novo parâmetro obrigatório
    cnpj_empresa: str 
) -> dict:
    
    start_time = time.time()
    total_arquivos_recebidos = len(xml_files_in_memory)
    logging.info(f"Recebidos {total_arquivos_recebidos} XMLs. Iniciando parsing...")

    if total_arquivos_recebidos == 0:
        raise ValueError("Nenhum arquivo XML foi enviado para análise.")

    pasta_destino.mkdir(parents=True, exist_ok=True)
    pasta_copiados = pasta_destino / "xmls_copiados"
    pasta_copiados.mkdir(exist_ok=True)

    lista_dados_notas_bruta = []

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(obter_dados_xml_de_conteudo, filename, content): (filename, content)
                   for filename, content in xml_files_in_memory.items()}
        for future in as_completed(futures):
            lista_dados_notas_bruta.append(future.result())

    logging.info("Parsing concluído. Filtrando XMLs pela empresa...")

    # [MELHORIA 3] Etapa de Filtro
    # Limpa o CNPJ alvo (removendo pontos, barras, etc.)
    cnpj_alvo = "".join(filter(str.isdigit, cnpj_empresa))
    
    if not cnpj_alvo:
        logging.warning("CNPJ da empresa não fornecido. Analisando TODOS os XMLs.")
        notas_filtradas = lista_dados_notas_bruta
    else:
        notas_filtradas = [
            nota for nota in lista_dados_notas_bruta 
            if nota.cnpj_emitente == cnpj_alvo
        ]
        logging.info(f"Filtro aplicado: {len(notas_filtradas)} de {total_arquivos_recebidos} XMLs correspondem ao CNPJ {cnpj_alvo}.")

    
    # --- Daqui para baixo, usamos 'notas_filtradas' ---
    
    copiados = 0
    # Loop de cópia agora usa a lista filtrada
    for nota in notas_filtradas:
        try:
            numero = int(nota.numero_inicial) if nota.numero_inicial and nota.numero_inicial.isdigit() else None
            if numero and numero in numeros_para_copiar:
                filename_original = nota.arquivo_path.name
                if filename_original in xml_files_in_memory:
                    destino = pasta_copiados / filename_original
                    destino.write_bytes(xml_files_in_memory[filename_original])
                    nota.foi_copiado = True
                    copiados += 1
        except Exception as e:
            nota.erros.append(f"Falha ao copiar XML: {e}")

    # Geração de relatórios usa a lista filtrada
    resumo_path, csv_path = gerar_relatorios(
        notas_filtradas, 
        pasta_destino, 
        tempo_execucao=round(time.time() - start_time, 2)
    )

    zip_filepath = pasta_destino / f"resultados_{datetime.now():%Y%m%d_%H%M%S}.zip"
    with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(resumo_path, arcname=resumo_path.name)
        zf.write(csv_path, arcname=csv_path.name)

        for file_path in pasta_copiados.glob('*'):
            zf.write(file_path, arcname=f"xmls_copiados/{file_path.name}")

    elapsed = round(time.time() - start_time, 2)
    logging.info(f"Análise finalizada: {len(notas_filtradas)} XMLs processados (de {total_arquivos_recebidos}), {copiados} copiados ({elapsed}s).")

    return {
        "zip_path": zip_filepath,
        "summary_path": resumo_path,
    }