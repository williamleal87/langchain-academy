@echo off
echo === Criando ambiente virtual .venv ===
python -m venv .venv

echo === Ativando o ambiente virtual ===
call .venv\Scripts\activate.bat

echo === Instalando dependências do requirements.txt ===
pip install -r requirements.txt

echo === Ambiente pronto! ===
pause
