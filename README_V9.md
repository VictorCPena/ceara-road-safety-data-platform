# Observatório Viário CE — V9

Versão de consolidação do produto visual da plataforma de Engenharia de Dados
para segurança viária no Ceará.

## Navegação

A interface foi reduzida de nove páginas para cinco áreas de produto:

1. **Visão geral** — KPIs, evolução, mapa e sinais do recorte.
2. **Território** — mapa vetorial, análise municipal, rodovias e trechos críticos.
3. **Ocorrências** — causas, tipos, padrões temporais e clima.
4. **Envolvidos** — perfil das pessoas e veículos.
5. **Dados & metodologia** — fontes, arquitetura, limites e stack.

## Mapa

O produto não depende de Folium/Leaflet nem de tiles externos.

```text
GeoJSON municipal do Ceará
           +
      DuckDB Gold
           ↓
  Altair / Vega-Lite
```

O GeoJSON é baixado uma vez e fica versionado em:

```text
dashboard/assets/ceara_municipalities.geojson
```

Se ainda não existir:

```bash
python scripts/download_ceara_geojson.py
```

## Rodar localmente

```bash
pip install -r requirements-dashboard.txt
streamlit run dashboard/app.py
```

## Antes do push

```bash
python -m py_compile dashboard/app.py
git status
git add dashboard/app.py dashboard/assets scripts/download_ceara_geojson.py \
  requirements-dashboard.txt Dockerfile.dashboard .streamlit
git commit -m "feat: consolidate road safety observatory UX"
git push
```
