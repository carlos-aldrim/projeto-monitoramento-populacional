import requests
import time
from utils import create_producer

producer = create_producer()

def fetch_ibge_estados():
    url = 'https://servicodados.ibge.gov.br/api/v1/localidades/estados'
    resp = requests.get(url)
    return resp.json()

def fetch_brasilapi_municipios():
    estados = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES',
               'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR',
               'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC',
               'SP', 'SE', 'TO']

    all_municipios = []

    for uf in estados:
        url = f'https://brasilapi.com.br/api/ibge/municipios/v1/{uf}?providers=dados-abertos-br,gov,wikipedia'
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            municipios = resp.json()
            all_municipios.extend(municipios)
        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar dados para {uf}: {e}")
        except requests.exceptions.JSONDecodeError as e:
            print(f"Erro ao decodificar JSON de {uf}: {e}")

    return all_municipios

def fetch_viacep_cep(uf, cidade, logradouro):
    url = f'https://viacep.com.br/ws/{uf}/{cidade}/{logradouro}/json/'
    resp = requests.get(url)
    if resp.status_code == 200 and isinstance(resp.json(), list):
        return resp.json()
    return []

def produce_data():
    print("Produzindo dados...")

    estados = fetch_ibge_estados()
    for estado in estados:
        producer.send('ibge-topic', value=estado)
    producer.flush()
    print(f"Enviados {len(estados)} mensagens para 'ibge-topic'")

    time.sleep(1)

    municipios = fetch_brasilapi_municipios()
    for municipio in municipios:
        producer.send('brasilapi-topic', value=municipio)
    producer.flush()
    print(f"Enviados {len(municipios)} mensagens para 'brasilapi-topic'")

    time.sleep(1)

    exemplos = [
        ('SP', 'Sao Paulo', 'Avenida Paulista'),
        ('RJ', 'Rio de Janeiro', 'Rua Primeiro de Março'),
        ('MG', 'Belo Horizonte', 'Avenida Afonso Pena'),
        ('BA', 'Salvador', 'Rua Chile'),
    ]

    total = 0
    for uf, cidade, rua in exemplos:
        ceps = fetch_viacep_cep(uf, cidade, rua)
        for cep in ceps:
            producer.send('correios-topic', value=cep)
            total += 1
    producer.flush()
    print(f"Enviados {total} mensagens para 'correios-topic'")

def main():
    while True:
        produce_data()
        print("Esperando 1 minuto para próxima ingestão...\n")
        time.sleep(60)

if __name__ == '__main__':
    main()