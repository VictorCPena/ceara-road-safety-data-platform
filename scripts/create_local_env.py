"""Create ignored development secrets; refuses to overwrite an existing file."""
from pathlib import Path
import os
import secrets
p=Path('.env')
with p.open('x') as f:
    for key in ['AIRFLOW_DB_PASSWORD','AIRFLOW_JWT_SECRET','AIRFLOW_API_SECRET']:
        f.write(key+'='+secrets.token_hex(32)+'\n')
os.chmod(p,0o600)
print('Created .env for localhost development. Keep it private.')
