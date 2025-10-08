"""
Analisador de Documentos Fiscais Eletrônicos (NF-e, NFC-e)

Este script realiza uma varredura em um diretório de arquivos XML, extrai
informações detalhadas de cada documento, identifica o status real (Autorizado,
Cancelado, Inutilizado) com base no código cStat da SEFAZ, e gera relatórios
completos de análise de sequência e de operações de cópia.
"""
import os
import sys
import shutil
import logging
import csv
from pathlib import Path
from collections import defaultdict, Counter
from dataclasses import dataclass, field
import xml.etree.ElementTree as ET
from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm

# --- CONFIGURAÇÃO E CONSTANTES GLOBAIS ---
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("analise_xml.log", mode='w', encoding='utf-8')
    ]
)

PASTA_ORIGEM = Path(os.getenv("PASTA_ORIGEM", "")).expanduser()
PASTA_DESTINO = Path(os.getenv("PASTA_DESTINO", "")).expanduser()
NUMEROS_RAW = os.getenv("NUMEROS_PARA_COPIAR", "")
NOME_RELATORIO_SUMARIO = "relatorio_sumario.txt"
NOME_RELATORIO_CSV = "relatorio_detalhado.csv"

RESET = '\033[0m'
VERDE = '\033[92m'
VERMELHO = '\033[91m'

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

# --- FUNÇÕES AUXILIARES E DE CONFIGURAÇÃO ---

def parse_numeros(raw_str: str) -> set[int]:
    numeros = set()
    for n in raw_str.split(","):
        n = n.strip()
        if n.isdigit():
            numeros.add(int(n))
        elif n:
            logging.warning(f"Valor inválido ignorado na lista de números: {n}")
    return numeros

NUMEROS_PARA_COPIAR = parse_numeros(NUMEROS_RAW)

def validar_configuracao():
    erros = []
    if not PASTA_ORIGEM or not PASTA_ORIGEM.is_dir():
        erros.append(f"PASTA_ORIGEM '{PASTA_ORIGEM}' é inválida ou inexistente.")
    if not PASTA_DESTINO:
        erros.append("PASTA_DESTINO não foi definida no arquivo .env.")
    
    if erros:
        for e in erros: logging.error(e)
        logging.critical("Encerrando execução devido a erros de configuração.")
        sys.exit(1)

def criar_pasta_destino():
    try:
        PASTA_DESTINO.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logging.error(f"Não foi possível criar a pasta de destino {PASTA_DESTINO}: {e}")
        sys.exit(1)

def agrupar_lacunas(numeros: list[int]) -> str:
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

def _mapear_cstat_para_tipo(cstat: str) -> str:
    if cstat == '100': return "NFe Autorizada"
    if cstat in ['101', '135']: return "NFe Cancelada"
    if cstat == '102': return "NFe Inutilizada"
    if cstat.startswith('2') or cstat.startswith('3'): return f"NFe com Rejeição ({cstat})"
    return f"Status Desconhecido ({cstat})"

# --- LÓGICA PRINCIPAL DE PROCESSAMENTO ---

def obter_dados_xml(caminho_xml: Path) -> DadosNota:
    dados = DadosNota(arquivo_path=caminho_xml)
    try:
        tree = ET.parse(caminho_xml)
        root = tree.getroot()
        ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        
        # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼ CORREÇÃO APLICADA AQUI ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
        # Converte a tag para minúsculas para uma comparação robusta
        tag_raiz = root.tag.split('}')[-1].lower() 
        
        # Compara com a versão em minúsculas
        if tag_raiz == 'procinutnfe':
        # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲ FIM DA CORREÇÃO ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
            infInutRetorno = root.find('nfe:retInutNFe/nfe:infInut', ns)
            
            if infInutRetorno is not None:
                cstat = infInutRetorno.findtext('nfe:cStat', 'N/A', ns).strip()
                dados.tipo_documento = _mapear_cstat_para_tipo(cstat)
                dados.status_code = cstat
                dados.status_text = infInutRetorno.findtext('nfe:xMotivo', 'N/A', ns).strip()
                dados.modelo = infInutRetorno.findtext('nfe:mod', '?', ns).strip()
                dados.serie = infInutRetorno.findtext('nfe:serie', '?', ns).strip()
                dados.numero_inicial = infInutRetorno.findtext('nfe:nNFIni', '?', ns).strip()
                dados.numero_final = infInutRetorno.findtext('nfe:nNFFin', '?', ns).strip()
                dados.data_emissao = infInutRetorno.findtext('nfe:dhRecbto', '?', ns).strip()
            else:
                infInutPedido = root.find('nfe:inutNFe/nfe:infInut', ns)
                if infInutPedido is not None:
                    dados.tipo_documento = "Inutilização (Sem Protocolo)"
                    dados.status_text = infInutPedido.findtext('nfe:xJust', 'Justificativa não lida', ns).strip()
                    dados.modelo = infInutPedido.findtext('nfe:mod', '?', ns).strip()
                    dados.serie = infInutPedido.findtext('nfe:serie', '?', ns).strip()
                    dados.numero_inicial = infInutPedido.findtext('nfe:nNFIni', '?', ns).strip()
                    dados.numero_final = infInutPedido.findtext('nfe:nNFFin', '?', ns).strip()
                    dados.erros.append("Bloco de retorno <retInutNFe> não encontrado.")
                else:
                    dados.erros.append("Estrutura do XML de Inutilização é inválida.")

        # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼ CORREÇÃO APLICADA AQUI ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
        # Compara com a versão em minúsculas
        elif tag_raiz == 'nfeproc':
        # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲ FIM DA CORREÇÃO ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
            protocolos = root.findall('.//nfe:infProt', ns)
            if protocolos:
                ultimo_protocolo = protocolos[-1]
                cstat = ultimo_protocolo.findtext('nfe:cStat', 'N/A', ns).strip()
                dados.tipo_documento = _mapear_cstat_para_tipo(cstat)
                dados.status_code = cstat
                dados.status_text = ultimo_protocolo.findtext('nfe:xMotivo', 'N/A', ns).strip()
                dados.chave_acesso = ultimo_protocolo.findtext('nfe:chNFe', '?', ns).strip()
            else:
                dados.tipo_documento = "NFe (Sem Protocolo)"
                dados.erros.append("Nenhum protocolo de autorização encontrado.")

            ide = root.find('.//nfe:ide', ns)
            if ide is not None:
                num = ide.findtext('nfe:nNF', '?', ns).strip()
                dados.numero_inicial = num
                dados.numero_final = num
                dados.modelo = ide.findtext('nfe:mod', '?', ns).strip()
                dados.serie = ide.findtext('nfe:serie', '?', ns).strip()
                dados.data_emissao = ide.findtext('nfe:dhEmi', '?', ns).strip()

        else:
            # A tag_raiz original é mais informativa no erro
            tag_original = root.tag.split('}')[-1]
            dados.erros.append(f"Tipo de XML não suportado: <{tag_original}>")

    except ET.ParseError:
        dados.erros.append("Arquivo XML malformado ou corrompido.")
    except Exception as e:
        dados.erros.append(f"Erro inesperado: {e}")

    return dados


def analisar_e_copiar_xmls():
    logging.info("Iniciando processo de análise de documentos fiscais.")
    validar_configuracao()
    criar_pasta_destino()

    arquivos_xml_encontrados = list(PASTA_ORIGEM.rglob('*.xml'))
    if not arquivos_xml_encontrados:
        logging.warning("Nenhum arquivo .xml encontrado na pasta de origem. Encerrando.")
        return

    logging.info(f"Encontrados {len(arquivos_xml_encontrados)} arquivos .xml. Processando...")

    lista_dados_notas: list[DadosNota] = []
    with tqdm(total=len(arquivos_xml_encontrados), desc="Analisando XMLs") as pbar:
        for arquivo in arquivos_xml_encontrados:
            dados = obter_dados_xml(arquivo)
            
            if dados.numero_inicial.isdigit():
                num_ini = int(dados.numero_inicial)
                num_fim = int(dados.numero_final) if dados.numero_final.isdigit() else num_ini
                numeros_no_arquivo = set(range(num_ini, num_fim + 1))
                
                if any(n in numeros_no_arquivo for n in NUMEROS_PARA_COPIAR):
                    try:
                        shutil.copy2(arquivo, PASTA_DESTINO / arquivo.name)
                        dados.foi_copiado = True
                    except Exception as e:
                        logging.error(f"Erro ao copiar {arquivo.name}: {e}")
            
            lista_dados_notas.append(dados)
            pbar.update(1)

    logging.info(f"Análise concluída. {len(lista_dados_notas)} arquivos processados.")
    gerar_relatorios(lista_dados_notas)
    logging.info("Processo finalizado com sucesso.")


def gerar_relatorios(lista_dados: list[DadosNota]):
    """Gera o relatório de sumário (.txt) e o detalhado (.csv) com as novas melhorias."""
    logging.info("Gerando relatórios...")
    
    # --- 1. Prepara os dados para os relatórios ---
    contagem_status = Counter(d.tipo_documento for d in lista_dados)
    
    dados_por_serie = defaultdict(list)
    for nota in lista_dados:
        if nota.tipo_documento == "NFe Autorizada" and nota.numero_inicial.isdigit():
            dados_por_serie[(nota.modelo, nota.serie)].append(int(nota.numero_inicial))

    # NOVO: Lista de arquivos que não puderam ser identificados
    arquivos_desconhecidos = [d for d in lista_dados if d.tipo_documento == "Desconhecido"]

    # --- 2. Geração do Relatório de Sumário (.txt) ---
    caminho_sumario = PASTA_DESTINO / NOME_RELATORIO_SUMARIO
    with caminho_sumario.open('w', encoding='utf-8') as f:
        # --- Cabeçalho ---
        f.write("="*80 + "\n")
        f.write(f"{'Relatório Sumário de Análise e Cópia de XMLs':^80}\n")
        f.write("="*80 + "\n\n")
        f.write(f"Data e Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
        f.write(f"Pasta de Origem: {PASTA_ORIGEM.resolve()}\n")
        f.write(f"Total de XMLs Processados: {len(lista_dados)}\n\n")

        # --- Sumário de Status (com detalhe dos desconhecidos) ---
        f.write("--- Sumário de Status dos Documentos ---\n")
        for status, contagem in sorted(contagem_status.items()):
            f.write(f"- {status:<25}: {contagem}\n")
        
        # NOVO: Se houver arquivos desconhecidos, lista quais são eles
        if arquivos_desconhecidos:
            f.write("\n  --- Arquivos com Status Desconhecido ---\n")
            for nota in arquivos_desconhecidos:
                erros_str = "; ".join(nota.erros)
                f.write(f"  - {nota.arquivo_path.name} (Motivo: {erros_str})\n")
        f.write("\n")

        # --- Análise de Sequência ---
        f.write("-" * 80 + "\n")
        f.write(f"{'Análise de Sequência de Notas Fiscais Autorizadas':^80}\n")
        f.write("-" * 80 + "\n\n")
        if not dados_por_serie:
            f.write("Nenhuma NF-e autorizada encontrada para análise de sequência.\n\n")
        else:
            for (modelo, serie), numeros in sorted(dados_por_serie.items()):
                numeros.sort()
                min_n, max_n = numeros[0], numeros[-1]
                f.write(f"Modelo: {modelo} | Série: {serie} | Documentos: {len(numeros)} | Intervalo: {min_n}-{max_n}\n")
                esperado = set(range(min_n, max_n + 1))
                faltantes = sorted(list(esperado - set(numeros)))
                if faltantes:
                    f.write(f"  └─ Status: Incompleta. Lacunas ({len(faltantes)}): {agrupar_lacunas(faltantes)}\n\n")
                else:
                    f.write(f"  └─ Status: Sequência completa!\n\n")
       
        # --- Resumo dos Arquivos Copiados ---
        f.write("=" * 80 + "\n")
        f.write(f"{'Resumo dos Arquivos Copiados':^80}\n")
        f.write("=" * 80 + "\n\n")
        
        arquivos_copiados = [d for d in lista_dados if d.foi_copiado]
        if not arquivos_copiados:
            f.write("Nenhum arquivo correspondente aos critérios foi copiado.\n")
        else:
            f.write(f"{'Modelo':<7} {'Série':<7} {'Número(s)':<15} {'Data Emissão':<12} {'Status'}\n")
            f.write(f"{'-'*7:<7} {'-'*7:<7} {'-'*15:<15} {'-'*12:<12} {'-'*30}\n")
            
            def sort_key(nota):
                try:
                    num_int = int(nota.numero_inicial)
                except (ValueError, TypeError):
                    num_int = 0
                return (nota.modelo, nota.serie, num_int)

            for nota in sorted(arquivos_copiados, key=sort_key):
                numero_str = nota.numero_inicial
                if nota.numero_final != nota.numero_inicial:
                    numero_str = f"{nota.numero_inicial}-{nota.numero_final}"
                
                status_str = f"{nota.status_code} - {nota.status_text}"
                
                data_formatada = "N/A"
                if nota.data_emissao and 'T' in nota.data_emissao:
                    try:
                        data_obj = datetime.fromisoformat(nota.data_emissao)
                        data_formatada = data_obj.strftime('%d/%m/%Y')
                    except ValueError:
                        data_formatada = nota.data_emissao.split('T')[0] 
                
                f.write(f"{nota.modelo:<7} {nota.serie:<7} {numero_str:<15} {data_formatada:<12} {status_str}\n")
    
    logging.info(f"Relatório de sumário gerado em: {caminho_sumario}")

    # --- Geração do Relatório Detalhado (.csv) ---
    caminho_csv = PASTA_DESTINO / NOME_RELATORIO_CSV
    headers = ["arquivo_origem", "tipo_documento", "status_sefaz_cod", "status_sefaz_motivo",
               "numero_inicial", "numero_final", "serie", "modelo", "data_emissao", "chave_acesso",
               "foi_copiado", "erros"]
    with caminho_csv.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(headers)
        for nota in sorted(lista_dados, key=lambda d: d.arquivo_path.name):
            writer.writerow([
                nota.arquivo_path.name, nota.tipo_documento, nota.status_code,
                nota.status_text, nota.numero_inicial, nota.numero_final,
                nota.serie, nota.modelo, nota.data_emissao, nota.chave_acesso,
                "Sim" if nota.foi_copiado else "Não", "; ".join(nota.erros)
            ])
    logging.info(f"Relatório detalhado CSV gerado em: {caminho_csv}")

# --- EXECUÇÃO PRINCIPAL ---

if __name__ == "__main__":
    try:
        analisar_e_copiar_xmls()
        print(f"\n{VERDE}✔ Processo finalizado com sucesso!{RESET}")
        print(f"  Relatórios salvos em: {PASTA_DESTINO.resolve()}")
    except KeyboardInterrupt:
        logging.warning("Execução interrompida pelo usuário.")
        print(f"\n{VERMELHO}✖ Execução interrompida pelo usuário.{RESET}")
    except Exception as e:
        logging.critical(f"Ocorreu um erro fatal e inesperado: {e}", exc_info=True)
        print(f"\n{VERMELHO}✖ Ocorreu um erro fatal. Verifique o arquivo 'analise_xml.log' para detalhes.{RESET}")