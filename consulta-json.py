import requests

# URL completa da API
url = "http://tudocongelados.dyndns.info:1234/sysproserverisapi.dll/api/exporta/produto/producao?dt_inicial=01/01/2025&dt_final=31/10/2025"

try:
    # Executa a requisição
    response = requests.get(url)
    
    # Verifica se a requisição foi bem-sucedida
    response.raise_for_status() 

    # Imprime os dados recebidos (assumindo que é um JSON)
    print(response.json())

except requests.exceptions.RequestException as e:
    print(f"Ocorreu um erro na requisição: {e}")