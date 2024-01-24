import sys
import time

from accruals import main as accruals_main
from fbo import main as fbo_main
from leftovers import main as leftovers_main
from advstencil import main as advstencil_main
from advsearch import main as advsearch_main


def main():
    print("\nВыполнение логики accruals.py...")
    accruals_main()
    print("\nОжидаем 60 секунд для запуска следующей задачи...")
    time.sleep(60)

    print("\nВыполнение логики fbo.py...")
    fbo_main()
    print("\nОжидаем 60 секунд для запуска следующей задачи...")
    time.sleep(60)

    print("\nВыполнение логики leftovers.py...")
    leftovers_main()
    print("\nОжидаем 60 секунд для запуска следующей задачи...")
    time.sleep(60)

    print("\nВыполнение логики advstencil.py...")
    advstencil_main()
    print("\nОжидаем 60 секунд для запуска следующей задачи...")
    time.sleep(60)

    print("Выполнение логики advsearch.py...")
    advsearch_main()
    print("\n")
    print("#" * 50)
    print(f"Обновление {sys.argv[1]} успешно завершено")
    print("#" * 50)
    print("\n")


if __name__ == "__main__":
    main()
