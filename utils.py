import os
from dotenv import load_dotenv

load_dotenv()

key = os.environ.get('API_KEY')
contrasenaSQL = os.environ.get('SQL_PASSWORD')


