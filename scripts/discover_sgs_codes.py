#!/usr/bin/env python3
"""Faz varredura de metadados SGS (Olinda/BCB) e sugere códigos para o catálogo.

Este script usa apenas biblioteca padrão para facilitar execução em ambientes sem instalação local.

Exemplo:
  python scripts/discover_sgs_codes.py \
    --catalogo data/catalogo_series_exemplo.csv \
    --saida data/catalogo_series_sugerido.csv \
    --top 5
"""

from __future__ import annotations

import argparse
import csv
import difflib
import json
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path

OLINDA_BASE = "https://olinda.bcb.gov.br/olinda/servico/SGS/versao/v1/odata/Series"


def normalize(texto: str) -> str:
    texto = texto.lower().strip()
    texto = re.sub(r"[^a-z0-9çáéíóúâêôãõàü\s]", " ", texto)
    texto = re.sub(r"\s+", " ", texto)
    return texto


def fetch_series(top: int = 20000) -> list[dict]:
    params = {
        "$top": str(top),
        "$select": "Codigo,Nome,Unidade,Periodicidade",
        "$format": "json",
    }
    url = f"{OLINDA_BASE}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return payload.get("value", [])


def carregar_catalogo(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def sugerir_codigo(linha: dict, series: list[dict], top: int) -> tuple[str | None, str, list[tuple[float, dict]]]:
    alvo = normalize(f"{linha['publico']} {linha['modalidade']} {linha['indicador']}")
    candidatos: list[tuple[float, dict]] = []

    for s in series:
        nome = normalize(str(s.get("Nome", "")))
        score = difflib.SequenceMatcher(None, alvo, nome).ratio()
        candidatos.append((score, s))

    candidatos.sort(key=lambda x: x[0], reverse=True)
    top_hits = candidatos[:top]
    best = top_hits[0][1] if top_hits else None
    best_code = str(best.get("Codigo")) if best else None
    best_name = str(best.get("Nome")) if best else ""
    return best_code, best_name, top_hits


def main() -> int:
    parser = argparse.ArgumentParser(description="Sugere códigos SGS por similaridade de nomes")
    parser.add_argument("--catalogo", type=Path, required=True)
    parser.add_argument("--saida", type=Path, default=Path("data/catalogo_series_sugerido.csv"))
    parser.add_argument("--top", type=int, default=3, help="Quantidade de melhores candidatos no relatório")
    args = parser.parse_args()

    rows = carregar_catalogo(args.catalogo)

    try:
        series = fetch_series()
    except Exception as e:
        print("[ERRO] Não foi possível consultar Olinda/BCB.")
        print("Detalhe:", repr(e))
        print("Dica: rode no GitHub Actions/Codespaces para contornar restrições de rede local.")
        return 2

    args.saida.parent.mkdir(parents=True, exist_ok=True)

    header = list(rows[0].keys()) + ["codigo_sgs_sugerido", "nome_sgs_sugerido", "score_sugestao"]
    with args.saida.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()

        for r in rows:
            code, nome, hits = sugerir_codigo(r, series, args.top)
            best_score = f"{hits[0][0]:.4f}" if hits else ""
            out = dict(r)
            out["codigo_sgs_sugerido"] = code or ""
            out["nome_sgs_sugerido"] = nome
            out["score_sugestao"] = best_score
            writer.writerow(out)

            print("-", r["publico"], "|", r["modalidade"], "|", r["indicador"])
            for score, cand in hits:
                print(f"    candidato: {cand.get('Codigo')} | score={score:.4f} | {cand.get('Nome')}")

    print(f"\nArquivo com sugestões: {args.saida}")
    print("Revise manualmente os códigos sugeridos antes de usar em produção.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
