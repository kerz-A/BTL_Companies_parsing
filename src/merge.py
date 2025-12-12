import pandas as pd
from pathlib import Path

# Пути к исходным CSV
files = {
    "marketingtech": r"C:\Users\UZER\Documents\Ke\Тестовое на data analyst\src\data\marketingtech_top20.csv",
    "alladvertising": r"C:\Users\UZER\Documents\Ke\Тестовое на data analyst\src\data\alladvertising_top20.csv",
    "directline": r"C:\Users\UZER\Documents\Ke\Тестовое на data analyst\src\data\directline_pr_agencies.csv",
    "pavezlo": r"C:\Users\UZER\Documents\Ke\Тестовое на data analyst\src\data\pavezlo_marketing_agencies.csv"
}

# Итоговый файл
OUT_FILE = Path("data/agencies_merged.csv")
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# Универсальный набор колонок (объединение всех)
columns = [
    "inn","name","revenue_year","revenue","segment_tag","source","rating_ref",
    "okved_main","employees","site","description","region","contacts","email",
    "address","founded","specializations","services","img_src","img_alt"
]

dfs = []
for src, path in files.items():
    df = pd.read_csv(path, encoding="utf-8")
    # добавляем недостающие колонки
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    # приводим порядок колонок
    df = df[columns]
    dfs.append(df)

# объединяем
merged = pd.concat(dfs, ignore_index=True)

# сохраняем
merged.to_csv(OUT_FILE, index=False, encoding="utf-8")
print("Saved", len(merged), "rows to", OUT_FILE)
