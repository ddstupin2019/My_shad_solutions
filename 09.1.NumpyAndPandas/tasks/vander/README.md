## Vandermonde matrix

`numpy`

### Условие
Напишите функцию, генерирующую [матрицу Вандермонда](https://ru.wikipedia.org/wiki/Определитель_Вандермонда), принимающую на вход вектор (x1, ... , xn).

В этом задании **запрещается** пользоваться готовыми реализациями (например, [numpy.vander](https://numpy.org/doc/stable/reference/generated/numpy.vander.html)), а также [np.repeat](https://numpy.org/doc/stable/reference/generated/numpy.repeat.html) и [np.transpose](https://numpy.org/doc/stable/reference/generated/numpy.transpose.html).

При решении задействуйте [np.reshape](https://numpy.org/doc/stable/reference/generated/numpy.reshape.html) и/или [np.newaxis](https://numpy.org/doc/stable/user/basics.indexing.html#slicing-and-striding).

### Замечания

* нельзя использовать циклы (ключевые слова for и while), list comprehension, map и т.п.
* под матрицей понимается двумерный numpy.array