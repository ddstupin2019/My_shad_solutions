markdown
# CompGraph: Computational Graph Library for Map-Reduce Operations

**CompGraph** — это библиотека для построения и выполнения вычислительных графов в стиле Map-Reduce, предназначенная для обработки больших объемов данных в потоковом режиме с минимальным потреблением памяти. Написаная через боль и страдания мной, при подержки deepseek.

## Установка

### Через uv (рекомендуется)

```bash
# Установите uv если ещё не установлен
curl -LsSf https://astral.sh/uv/install.sh | sh

# Установите compgraph в режиме разработки из папки
# /Topaz2090/08.2.HW2/tasks
uv pip install -e compgraph --force-reinstall

#Через pip
pip install -e compgraph --force-reinstall


# Показать все доступные команды
compgraph --help
```
# Доступные алгоритмы


1. Word Count
Подсчет частоты слов во всех документах.
```bash
compgraph word-count <input_file> <output_file>
```
2. Inverted Index с TF-IDF
Построение инвертированного индекса с ранжированием по метрике TF-IDF.
```bash
compgraph inverted-index <input_file> <output_file>
```
Формула TF-IDF:
TFIDF(word, doc) = freq(word in doc) * ln(total_docs / docs_with_word)

3. PMI (Pointwise Mutual Information)
Нахождение топ-10 наиболее характерных слов для каждого документа.

```bash
compgraph pmi <input_file> <output_file>
```
Формула PMI:
pmi(word, doc) = ln(freq(word in doc) / freq(word in all docs))

4. Анализ скорости движения
Вычисление средней скорости движения по городу в зависимости от часа и дня недели.

```bash
compgraph yandex-maps <travel_times_file> <road_graph_file> <output_file>
```

5. Анализ скорости движения
Вычисление средней скорости движения по городу в зависимости от часа и дня недели c графиком.

```bash
compgraph yandex-maps <travel_times_file> <road_graph_file> <output_file> --plot --plot-file <chart_file>
```

# Использование как библиотеки
```python
from compgraph import Graph, operations
from compgraph.algorithms import word_count_graph

# Создание графа для подсчета слов
graph = word_count_graph(input_stream_name='docs')

# Данные для обработки
data = [
    {"doc_id": 1, "text": "Hello world!"},
    {"doc_id": 2, "text": "World of programming"},
]

# Запуск графа
result = graph.run(docs=lambda: iter(data))

for row in result:
    print(row)
# Output:
# {'text': 'hello', 'count': 1}
# {'text': 'world', 'count': 2}
# {'text': 'of', 'count': 1}
# {'text': 'programming', 'count': 1}
```

# Структура проекта
```text
compgraph/
├── __init__.py
├── cli.py                          # Командный интерфейс
├── graph.py                        # Основной класс Graph
├── operations.py                   # Все операции
├── algorithms.py                   # Готовые алгоритмы
├── external_sort.py                # Внешняя сортировка
├── plot_image.py                   # Рисование графика
└── tests/                          # Тесты
    ├──correctness
        ├──test_algorithms.py   # Тесты Алгоритмов
        └──test_operations.py   # Тесты базовых операциий
    ├──memory
        ├──test_algorithms.py   # Тесты памяти алгоритмов
        └──test_operations.py   # Тесты памяти операций
    ├── test_cli.py             # Тесты cli
    └── test_custom_operations.py # Тесты кастомных операций
```

# Пример создание простого графа
```python
graph = Graph.graph_from_iter('input') \
    .map(operations.LowerCase('text')) \
    .map(operations.FilterPunctuation('text')) \
    .map(operations.Split('text')) \
    .sort(['text']) \
    .reduce(operations.Count('count'), ['text']) \
    .sort(['count', 'text'])
```
# Пользовательские операции
```python
from compgraph import operations
from typing import Dict, Any, Iterator

class CustomReducer(operations.Reducer):
    def __call__(self, rows: Iterator[Dict[str, Any]]) -> Dict[str, Any]:
        # Ваша логика редьюсера
        result = {}
        for row in rows:
            # Обработка строк
            pass
        yield result
```

# Использование
```python
graph.reduce(CustomReducer(), ['key'])
```
# Тестирование
```bash
# Запуск всех тестов
pytest compgraph/tests/

# Тесты CLI
pytest compgraph/tests/test_cli.py

# Тесты на корректность
pytest compgraph/tests/correctness/

# Тесты на память
pytest compgraph/tests/memory/

# С покрытием кода
pytest --cov=compgraph compgraph/tests/
```
# Производительность
Потоковая обработка: Все операции работают в потоковом режиме

Константная память: Не зависит от объема входных данных

Внешняя сортировка: Использует эффективную внешнюю сортировку для больших данных

Оптимизированные операции: Все базовые операции оптимизированы для работы с генераторами честно честно
