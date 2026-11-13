"""資源エネルギー庁の統計の取得 + dbt ビルド。

1. total_energy: 総合エネルギー統計 時系列表の公開 Excel を取得し縦持ち CSV へ整形
2. power_survey: 電力調査統計 都道府県別電力需要実績の公開 Excel を年度ごとに取得し CSV へ整形
3. power_generation: 電力調査統計 都道府県別発電実績の公開 Excel を年度ごとに取得し CSV へ整形
4. power_demand_operator: 電力調査統計 電力需要実績（事業者別）の公開 Excel を年度ごとに取得し CSV へ整形
5. power_generation_operator: 電力調査統計 発電実績（事業者別）の公開 Excel を年度ごとに取得し CSV へ整形
6. power_municipality: 電力調査統計 市町村別需要電力量・逆潮流量の公開 Excel を年度ごとに取得し CSV へ整形
7. power_plants:  電力調査統計 都道府県別発電所数、出力の公開 Excel を年度ごとに取得し CSV へ整形
8. power_thermal_fuel: 電力調査統計 火力発電燃料実績の公開 Excel を年度ごとに取得し CSV へ整形
9. power_plants_operator: 電力調査統計 電気事業者の発電所数、出力の公開 Excel を年度ごとに取得し CSV へ整形
10. power_plants_captive: 電力調査統計 自家用発電所数、出力の公開 Excel を年度ごとに取得し CSV へ整形
11. power_generation_captive: 電力調査統計 自家用発電実績の公開 Excel を年度ごとに取得し CSV へ整形
12. power_captive_halfyear: 電力調査統計 自家用発電所等実績の公開 Excel を年度ごとに取得し
    月次の実績と年度計の発電所数の2つの CSV へ整形
13. petroleum_price: 石油製品価格調査 給油所小売価格の週次ファイルを取得し縦持ち CSV へ整形
14. dbt:        dbt ビルド
"""

import logging
from pathlib import Path

from dbt.cli.main import dbtRunner

import petroleum_price
import power_captive_halfyear
import power_demand_operator
import power_generation
import power_generation_captive
import power_generation_operator
import power_municipality
import power_plants
import power_plants_captive
import power_plants_operator
import power_survey
import power_thermal_fuel
import total_energy

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("pipelines")

WORK_DIR = Path(".queria")
STEPS = 14


def csv(name: str) -> Path:
    return WORK_DIR / f"{name}.csv"


def log_rows(name: str, rows: int) -> None:
    logger.info(f"  {name}.csv: {rows} rows")


def dbt_build() -> None:
    dbt = dbtRunner()
    for cmd in (["deps"], ["run"], ["docs", "generate"]):
        result = dbt.invoke(cmd)
        if not result.success:
            raise SystemExit(f"dbt {cmd[0]} failed")


# (ログの見出し, 取得関数, 出力する CSV の名前)
SINGLE_OUTPUT = [
    ("total_energy (総合エネルギー統計 時系列表)", total_energy.download_and_parse, "energy_balance"),
    ("power_survey (電力調査統計 都道府県別電力需要実績)", power_survey.download_and_parse, "power_demand"),
    ("power_generation (電力調査統計 都道府県別発電実績)", power_generation.download_and_parse, "power_generation"),
    ("power_demand_operator (電力調査統計 電力需要実績 事業者別)", power_demand_operator.download_and_parse, "power_demand_operator"),
    ("power_generation_operator (電力調査統計 発電実績 事業者別)", power_generation_operator.download_and_parse, "power_generation_operator"),
]

AFTER_MUNICIPALITY = [
    ("power_plants (電力調査統計 都道府県別発電所数、出力)", power_plants.download_and_parse, "power_plants"),
    ("power_thermal_fuel (電力調査統計 火力発電燃料実績)", power_thermal_fuel.download_and_parse, "power_thermal_fuel"),
    ("power_plants_operator (電力調査統計 電気事業者の発電所数、出力)", power_plants_operator.download_and_parse, "power_plants_operator"),
    ("power_plants_captive (電力調査統計 自家用発電所数、出力)", power_plants_captive.download_and_parse, "power_plants_captive"),
    ("power_generation_captive (電力調査統計 自家用発電実績)", power_generation_captive.download_and_parse, "power_generation_captive"),
]


def run_single(step: int, label: str, fetch, name: str) -> None:
    logger.info(f"{step}/{STEPS}: {label}")
    log_rows(name, fetch(csv(name)))


def main() -> None:
    WORK_DIR.mkdir(exist_ok=True)

    for step, (label, fetch, name) in enumerate(SINGLE_OUTPUT, start=1):
        run_single(step, label, fetch, name)

    logger.info(f"6/{STEPS}: power_municipality (電力調査統計 市町村別需要電力量・逆潮流量)")
    for kind, name in (
        (power_municipality.DEMAND, "power_demand_municipality"),
        (power_municipality.REVERSE_FLOW, "reverse_power_flow_municipality"),
    ):
        log_rows(name, power_municipality.download_and_parse(kind, csv(name)))

    for step, (label, fetch, name) in enumerate(AFTER_MUNICIPALITY, start=7):
        run_single(step, label, fetch, name)

    logger.info(f"12/{STEPS}: power_captive_halfyear (電力調査統計 自家用発電所等実績)")
    rows, plants = power_captive_halfyear.download_and_parse(
        csv("power_captive_halfyear"), csv("power_captive_halfyear_plants")
    )
    log_rows("power_captive_halfyear", rows)
    log_rows("power_captive_halfyear_plants", plants)

    run_single(13, "petroleum_price (石油製品価格調査 給油所小売価格 週次)", petroleum_price.download_and_parse, "petroleum_retail_price")

    logger.info(f"14/{STEPS}: dbt build")
    dbt_build()


if __name__ == "__main__":
    main()
