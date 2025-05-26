from pydantic import BaseModel, Field, ValidationError
from datetime import datetime, timedelta
import requests

# Classe Pydantic
class ObterLatitudeLongitude(BaseModel):
    latitude: str = Field(description='latitude da localização', examples=['-23.000'])
    longitude: str = Field(description='longitude da localização', examples=['-46.000'])

# Ajuste do fuso horário
def data_hora_ajustada(data_hora: str) -> str:
    data = datetime.strptime(data_hora, '%Y-%m-%dT%H:%M')
    data_ajustada = data - timedelta(hours=3)
    return data_ajustada.strftime('%d-%m-%Y %H:%M')

# Tool (sem @tool)
def obter_dados_temperatura(latitude: str, longitude: str) -> list:
    """
    Obtém informações meteorológicas para uma coordenada geográfica e retorna informações como
    temperatura atual, hora e data da medição e velocidade do vento

    Args:
        latitude (str): Latitude da localização.
        longitude (str): Longitude da localização.

    Returns:
        list: Temperatura, velocidade do vento e data/hora ajustada.
    """
    # Validação manual via Pydantic
    try:
        ObterLatitudeLongitude(latitude=latitude, longitude=longitude)
    except ValidationError as e:
        raise ValueError(f"Erro de validação nos parâmetros: {e}")

    endpoint = 'https://api.open-meteo.com/v1/forecast'
    url = f'{endpoint}?latitude={latitude}&longitude={longitude}&current_weather=true'
    response = requests.get(url)

    if response.status_code == 200:
        dados = response.json().get('current_weather', {})
        temperatura = str(dados.get('temperature'))
        vento = str(dados.get('windspeed'))
        data_hora = data_hora_ajustada(dados.get('time'))
        return [temperatura, vento, data_hora]
    else:
        raise ConnectionError(f"Erro ao acessar API: {response.status_code}")
