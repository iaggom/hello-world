#!/usr/bin/env python3
"""Coleta histórico de crédito no SGS/BCB, calcula variações e gera gráficos.

Uso típico:
python scripts/bcb_credit_history.py \
  --catalogo data/catalogo_series_exemplo.csv \
  --inicio 2015-01-01 \
  --saida data/saida_credito \
  --graficos
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import pandas as pd
import requests

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"


def baixar_serie_sgs(codigo: int, inicio: str, fim: str | None = None) -> pd.DataFrame:
    params = {"formato": "json", "dataInicial": _iso_para_brasil(inicio)}
    if fim:
        params["dataFinal"] = _iso_para_brasil(fim)

    url = BASE_URL.format(codigo=codigo)
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()

    dados = resp.json()
    if not dados:
        return pd.DataFrame(columns=["data", "valor"])

    df = pd.DataFrame(dados)
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
    df["valor"] = (
        df["valor"].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    )
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    return df[["data", "valor"]].dropna()


def _iso_para_brasil(data_iso: str) -> str:
    ano, mes, dia = data_iso.split("-")
    return f"{dia}/{mes}/{ano}"


def calcular_variacoes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["publico", "modalidade", "indicador", "data"]).copy()

    base_nominal = {"saldo", "concessao"}
    mask_nominal = df["indicador"].isin(base_nominal)

    df["var_m_m"] = pd.NA
    df["var_a_a"] = pd.NA

    df.loc[mask_nominal, "var_m_m"] = (
        df[mask_nominal].groupby(["publico", "modalidade", "indicador"])["valor"].pct_change() * 100
    )
    df.loc[mask_nominal, "var_a_a"] = (
        df[mask_nominal].groupby(["publico", "modalidade", "indicador"])["valor"].pct_change(12) * 100
    )

    mask_pp = ~mask_nominal
    df.loc[mask_pp, "var_m_m"] = df[mask_pp].groupby(["publico", "modalidade", "indicador"])["valor"].diff()
    df.loc[mask_pp, "var_a_a"] = df[mask_pp].groupby(["publico", "modalidade", "indicador"])["valor"].diff(12)

    return df


def carregar_catalogo(path: Path) -> pd.DataFrame:
    cat = pd.read_csv(path)
    esperadas = {"publico", "modalidade", "indicador", "codigo_sgs", "unidade"}
    faltantes = esperadas - set(cat.columns)
    if faltantes:
        raise ValueError(f"Colunas ausentes no catálogo: {sorted(faltantes)}")
    return cat


def coletar_todas_series(catalogo: pd.DataFrame, inicio: str, fim: str | None) -> pd.DataFrame:
    partes: list[pd.DataFrame] = []

    for item in _iter_catalogo_validado(catalogo):
        codigo = int(item.codigo_sgs)
        serie = baixar_serie_sgs(codigo, inicio=inicio, fim=fim)
        if serie.empty:
            continue

        serie["publico"] = item.publico
        serie["modalidade"] = item.modalidade
        serie["indicador"] = item.indicador
        serie["codigo_sgs"] = codigo
        serie["unidade"] = item.unidade
        partes.append(serie)

    if not partes:
        return pd.DataFrame(
            columns=["data", "valor", "publico", "modalidade", "indicador", "codigo_sgs", "unidade"]
        )

    bruto = pd.concat(partes, ignore_index=True)
    return bruto.sort_values(["publico", "modalidade", "indicador", "data"])


def _iter_catalogo_validado(catalogo: pd.DataFrame) -> Iterable[pd.Series]:
    for item in catalogo.itertuples(index=False):
        if pd.isna(item.codigo_sgs) or str(item.codigo_sgs).strip() == "":
            print(f"[AVISO] Série sem código, ignorada: {item.publico} | {item.modalidade} | {item.indicador}")
            continue
        yield item


def salvar_outputs(df: pd.DataFrame, pasta_saida: Path) -> None:
    pasta_saida.mkdir(parents=True, exist_ok=True)

    df.to_csv(pasta_saida / "historico_credito_long.csv", index=False)

    if df.empty:
        return

    ultima_data = df["data"].max()
    foto = df[df["data"] == ultima_data].copy()
    foto = foto[["publico", "modalidade", "indicador", "valor", "var_m_m", "var_a_a", "codigo_sgs", "unidade"]]
    foto.to_csv(pasta_saida / "foto_ultima_data.csv", index=False)


def _preparar_serie_plot(base: pd.DataFrame) -> pd.DataFrame:
    serie = base.sort_values("data").copy()
    serie["mm12"] = serie["valor"].rolling(12, min_periods=3).mean()
    serie["std12"] = serie["valor"].rolling(12, min_periods=3).std()
    serie["banda_sup"] = serie["mm12"] + 2 * serie["std12"]
    serie["banda_inf"] = serie["mm12"] - 2 * serie["std12"]
    return serie


def _plotar_serie_em_eixo(ax: plt.Axes, serie: pd.DataFrame, titulo: str, unidade: str, legenda: bool = False) -> None:
    ax.set_facecolor("#ffffff")

    ax.fill_between(
        serie["data"],
        serie["banda_inf"],
        serie["banda_sup"],
        color="#e6e6e6",
        alpha=1.0,
        label="Banda ±2 desvios padrão",
        zorder=1,
    )

    ax.plot(
        serie["data"],
        serie["mm12"],
        color="#d62728",
        linestyle="--",
        linewidth=1.3,
        label="Média móvel 12m",
        zorder=2,
    )

    ax.plot(
        serie["data"],
        serie["valor"],
        color="#1f4bd8",
        linewidth=1.5,
        label="Valor mensal reportado",
        zorder=3,
    )

    ax.axhline(0, color="#bdbdbd", linewidth=0.7)
    ax.grid(axis="y", alpha=0.22, color="#b0b0b0", linewidth=0.6)
    ax.set_title(titulo)
    ax.set_ylabel(unidade)
    ax.set_xlabel("Data")

    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)

    if legenda:
        ax.legend(loc="upper left", frameon=False, ncol=3, fontsize=8)


def gerar_graficos_series(df: pd.DataFrame, pasta_saida: Path) -> int:
    if df.empty:
        return 0

    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "axes.titlesize": 10,
            "axes.labelsize": 8,
            "xtick.labelsize": 7,
            "ytick.labelsize": 7,
        }
    )

    pasta_graficos = pasta_saida / "graficos"
    pasta_paineis = pasta_saida / "paineis_16x9"
    pasta_graficos.mkdir(parents=True, exist_ok=True)
    pasta_paineis.mkdir(parents=True, exist_ok=True)

    series_preparadas: list[tuple[tuple, pd.DataFrame]] = []
    grupos = ["publico", "modalidade", "indicador", "codigo_sgs", "unidade"]
    for chave, base in df.groupby(grupos, dropna=False):
        serie = _preparar_serie_plot(base)
        if len(serie) < 3:
            continue
        series_preparadas.append((chave, serie))

    # PNG individual (opcionalmente útil para análises isoladas)
    for chave, serie in series_preparadas:
        publico, modalidade, indicador, codigo_sgs, unidade = chave
        fig, ax = plt.subplots(figsize=(9, 4.8))
        _plotar_serie_em_eixo(
            ax,
            serie,
            titulo=f"{publico} | {modalidade} | {indicador}",
            unidade=unidade,
            legenda=True,
        )
        nome = _slugify(f"{publico}_{modalidade}_{indicador}_{int(codigo_sgs)}")
        fig.tight_layout()
        fig.savefig(pasta_graficos / f"{nome}.png", dpi=180)
        plt.close(fig)

    # Painéis 16:9 com 4 gráficos por página (2x2), prontos para slide.
    paginas = 0
    for i in range(0, len(series_preparadas), 4):
        lote = series_preparadas[i : i + 4]
        fig, axes = plt.subplots(2, 2, figsize=(16, 9))
        axes_flat = axes.flatten()

        for j, ax in enumerate(axes_flat):
            if j >= len(lote):
                ax.axis("off")
                continue

            chave, serie = lote[j]
            publico, modalidade, indicador, _codigo_sgs, unidade = chave
            _plotar_serie_em_eixo(
                ax,
                serie,
                titulo=f"{publico} | {modalidade} | {indicador}",
                unidade=unidade,
                legenda=(j == 0),
            )

        pagina = paginas + 1
        fig.suptitle("Séries de Crédito - Painel 16:9 (4 gráficos)", fontsize=14, y=0.995)
        fig.tight_layout(rect=[0, 0, 1, 0.965])
        fig.savefig(pasta_paineis / f"painel_{pagina:03d}.png", dpi=180)
        plt.close(fig)
        paginas += 1

    return len(series_preparadas)


def _slugify(valor: str) -> str:
    base = re.sub(r"[^a-zA-Z0-9]+", "_", valor.strip().lower())
    return re.sub(r"_+", "_", base).strip("_")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Coleta histórico de crédito no SGS/BCB")
    parser.add_argument("--catalogo", type=Path, required=True, help="CSV com mapeamento modalidade -> código SGS")
    parser.add_argument("--inicio", default="2012-01-01", help="Data inicial no formato YYYY-MM-DD")
    parser.add_argument("--fim", default=None, help="Data final no formato YYYY-MM-DD")
    parser.add_argument("--saida", type=Path, default=Path("data/saida_credito"), help="Pasta de saída")
    parser.add_argument("--graficos", action="store_true", help="Gera PNG por série e painéis 16:9 (4 por página)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    catalogo = carregar_catalogo(args.catalogo)
    bruto = coletar_todas_series(catalogo, inicio=args.inicio, fim=args.fim)
    tratado = calcular_variacoes(bruto)
    salvar_outputs(tratado, args.saida)

    print(f"Registros coletados: {len(tratado):,}")
    if not tratado.empty:
        print(f"Intervalo: {tratado['data'].min().date()} -> {tratado['data'].max().date()}")
        print(f"Arquivos em: {args.saida}")

    if args.graficos:
        total = gerar_graficos_series(tratado, args.saida)
        print(f"Gráficos individuais gerados: {total}")
        print(f"Painéis 16:9 em: {args.saida / 'paineis_16x9'}")


if __name__ == "__main__":
    main()
