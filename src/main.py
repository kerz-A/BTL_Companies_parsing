import subprocess
import sys

def run(script):
    print(f"[INFO] Запуск {script}...")
    result = subprocess.run([sys.executable, script])
    if result.returncode != 0:
        print(f"[ERROR] Ошибка при выполнении {script}")
        sys.exit(1)

def main():
    # 1. Запуск всех парсеров
    run("src\parsers\alladvertising_parsing.py")
    run("src\parsers\directline_parsing.py")
    run("src\parsers\marketing-tech_parsing.py")
    run("src\parsers\Povezlo_parsing.py")

    # 2. Слияние результатов
    run("src\merge.py")

    # 3. Автоматический парсинг ИНН/ОГРН
    run("src\INN_OGRN_finding.py")

    print("[INFO] Все шаги завершены. Результаты в data/final/companies_final.csv")

if __name__ == "__main__":
    main()
