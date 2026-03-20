"""
Simplified VM code which works for some cases.
You need extend/rewrite code to pass all cases.
"""

import builtins
import dis
import types
import typing as tp
NULL = object()


class Frame:

    """
    Frame header in cpython with description
        https://github.com/python/cpython/blob/3.13/Include/internal/pycore_frame.h

    Text description of frame parameters
        https://docs.python.org/3/library/inspect.html?highlight=frame#types-and-members
    """

    def __init__(self,
                 frame_code: types.CodeType,
                 frame_builtins: dict[str, tp.Any],
                 frame_globals: dict[str, tp.Any],
                 frame_locals: dict[str, tp.Any], frame_closure: tuple = ()) -> None:
        self.code = frame_code
        self.builtins = frame_builtins
        self.globals = frame_globals
        self.locals = frame_locals
        self.data_stack: tp.Any = []
        self.return_value = None
        self.last_exception: Exception | None = None
        self.nam_instruction = 0
        self._closure = frame_closure or ()
        self._extended_arg = 0
        self._pending_extended = False

        self.ins_map = {}
        self._waiting_for_send = False
        self._yield_value = None
        self._generator_finished = False

        self.block_stack = []
        self.exception_table = dis.Bytecode(
            self.code).exception_entries  # type: ignore

        qwe = list(dis.get_instructions(self.code))
        for i in range(len(qwe) - 1):
            self.ins_map[qwe[i].offset] = qwe[i], qwe[i + 1].offset
        self.ins_map[qwe[-1].offset] = qwe[-1], -1

    def top(self) -> tp.Any:
        return self.data_stack[-1]

    def pop(self) -> tp.Any:
        return self.data_stack.pop()

    def push(self, *values: tp.Any) -> None:
        self.data_stack.extend(values)

    def popn(self, n: int) -> tp.Any:
        """
        Pop a number of values from the value stack.
        A list of n values is returned, the deepest value first.
        """
        if n > 0:
            returned = self.data_stack[-n:]
            self.data_stack[-n:] = []
            return returned
        else:
            return []

    def run(self) -> tp.Any:
        while self.nam_instruction != -1:
            tmp = self.nam_instruction
            ins, self.nam_instruction = self.ins_map[self.nam_instruction]

            if self._pending_extended:
                ins = types.SimpleNamespace(
                    offset=ins.offset,
                    opname=ins.opname,
                    argval=(self._extended_arg << 8) | ins.argval,
                    arg=(self._extended_arg << 8) | ins.arg
                )
                self._extended_arg = 0
                self._pending_extended = False
            # print(ins.offset, ins.opname, ins.argval, ins.arg)
            # print(self.locals)
            # print(len(self.data_stack), self.data_stack)

            try:
                getattr(self, ins.opname.lower() + "_op")(ins.argval, ins.arg)
            except BaseException as e:
                self.nam_instruction = tmp

                handler, depth, lasti = self.find_exception_handler(e)
                if handler is None:
                    raise
                self.last_exception = e  # type: ignore
                if len(self.data_stack) > depth:  # type: ignore
                    self.data_stack = self.data_stack[:depth]

                if lasti:
                    self.push(tmp)
                self.push(e)

                self.jump(handler)
                continue

            if self.return_value is not None:
                return self.return_value

        return self.return_value

    def find_exception_handler(self, exc):
        offset = self.nam_instruction
        for entry in dis.Bytecode(self.code).exception_entries:  # type: ignore
            if entry.start <= offset < entry.end:
                return entry.target, entry.depth, entry.lasti
        return None, None, False

    def extended_arg_op(self, argval, arg):
        self._extended_arg = (self._extended_arg << 8) | arg
        self._pending_extended = True

    def unpack_ex_op(self, argval, arg):
        """
        UNPACK_EX: Расширенная распаковка с поддержкой *args.
        """
        before = arg & 0xFF
        after = (arg >> 8) & 0xFF
        iterable = self.pop()

        try:
            items = list(iterable)
        except TypeError:
            raise TypeError(
                f"cannot unpack non-iterable {type(iterable).__name__} object")

        min_required = before + after
        if len(items) < min_required:
            raise ValueError(
                f"not enough values to unpack (expected at least {min_required}, got {len(items)})"
            )
        before_items = items[:before]
        middle_items = items[before:len(items) - after if after > 0 else None]
        after_items = items[len(items) - after:] if after > 0 else []

        for item in reversed(after_items):
            self.push(item)
        self.push(middle_items)
        for item in reversed(before_items):
            self.push(item)

    def push_exc_info_op(self, argval, arg):
        new_exc = self.pop()
        self.push(None)
        self.push(new_exc)

    def check_exc_match_op(self, argval, arg):
        match_type = self.pop()
        exc_type = type(self.top())

        result = issubclass(exc_type, match_type)
        self.push(result)

    def pop_except_op(self, argval, arg):
        prev_exc = self.pop()
        self.last_exception = prev_exc

    def reraise_op(self, argval, arg):
        raise self.top()

    def store_fast_store_fast_op(self, argval, arg):
        self.locals[argval[0]] = self.pop()
        self.locals[argval[1]] = self.pop()

    def load_closure_op(self, argval, arg):
        if argval in self.locals:
            self.push(self.locals[argval])
        elif arg < len(self._closure):
            self.push(self._closure[arg])
        else:
            raise NameError(f"closure variable '{argval}' not found")

    def before_with_op(self, argval, arg):

        manager = self.pop()

        enter = manager.__enter__
        exit = manager.__exit__

        result = enter()

        self.push(exit)
        self.push(result)

    def before_async_with_op(self, argval, arg):
        manager = self.pop()
        aexit = manager.__aexit__
        aenter_coro = manager.__aenter__()
        self.push(aexit)
        self.push(aenter_coro)

    def with_except_start_op(self, argval, arg):
        ex = self.data_stack[-1]
        exit_func = self.data_stack[-4]
        result = exit_func(type(ex), ex, ex.__traceback__)
        self.push(result)

    def jump(self, jump):
        """Move the bytecode pointer to `jump`, so it will execute next."""
        self.nam_instruction = jump

    def get_awaitable_op(self, argval, arg):
        obj = self.pop()
        if hasattr(obj, '__await__'):
            self.push(obj.__await__())
        else:
            self.push(obj)

    def get_yield_from_iter_op(self, argval, arg):
        tos = self.pop()

        if isinstance(tos, (types.GeneratorType, types.CoroutineType, Generator)):
            self.push(tos)
        else:
            try:
                self.push(iter(tos))
            except TypeError:
                raise TypeError(
                    f"cannot 'yield from' a non-iterator: {type(tos).__name__}")

    def send_op(self, argval, arg):
        value = self.pop()
        receiver = self.top()

        try:
            if isinstance(receiver, Generator):
                result = receiver.send(value)
            else:
                result = next(receiver)
            self.push(result)

        except StopIteration as e:
            self.push(e.value)
            self.jump(argval)

    def raise_varargs_op(self, argval, arg):
        argc = arg
        if argc == 0:
            if self.last_exception is None:
                raise RuntimeError("No active exception to re-raise")
            raise self.last_exception

        elif argc == 1:
            exception = self.pop()
            if isinstance(exception, type) and issubclass(exception, BaseException):
                exception = exception()
            self.last_exception = exception
            raise exception

        elif argc == 2:
            cause = self.pop()
            exception = self.pop()
            if isinstance(exception, type) and issubclass(exception, BaseException):
                exception = exception()
            if isinstance(cause, type) and issubclass(cause, BaseException):
                cause = cause()
            if exception is not None and cause is not None:
                exception.__cause__ = cause
            self.last_exception = exception
            raise exception

    def jump_backward_no_interrupt_op(self, argval, arg):
        self.jump(argval)

    def end_send_op(self, argval, arg):
        a = self.pop()
        self.pop()
        self.push(a)

    def store_fast_op(self, argval, arg):
        self.locals[argval] = self.pop()

    def return_generator_op(self, argval, arg):
        gen = Generator(self)
        self.return_value = gen
        self.push(gen)
        self._is_generator_executing = True

    def get_iter_op(self, argval, arg) -> None:
        self.push(iter(self.pop()))

    def jump_backward_op(self, jump: int, arg) -> None:
        self.jump(jump)

    def end_for_op(self, argval: tp.Any, arg) -> None:
        pass

    def for_iter_op(self, jump, arg) -> None:
        try:
            tmp = next(self.top())
            self.push(tmp)
        except StopIteration:
            self.jump(jump)

    def resume_op(self, argval: int, arg) -> tp.Any:
        pass

    def push_null_op(self, argval: int, arg) -> tp.Any:
        self.push(NULL)

    def precall_op(self, argval: int, arg) -> tp.Any:
        pass

    def return_const_op(self, argval: tp.Any, arg) -> tp.Any:
        self.return_value = argval
        self.nam_instruction = -1

    def call_op(self, argval: int, arg) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.13.7/library/dis.html#opcode-CALL
        """
        arguments = self.popn(argval)
        first = self.pop()
        second = self.pop()

        # Three stack layouts possible:
        # LOAD_GLOBAL flag: [..., callable, NULL, args]  -> first=NULL, second=callable
        # PUSH_NULL+LOAD:   [..., NULL, callable, args]  -> first=callable, second=NULL
        # Decorator/no-NULL:[..., callable, extra_arg]   -> first=extra_arg, second=callable
        if first is NULL:
            func = second
            null_or_self = first
        elif second is NULL:
            func = first
            null_or_self = second
        else:
            # Neither NULL: second (deeper) is callable, first is extra arg (e.g. decorator pattern)
            func = second
            null_or_self = first

        if null_or_self is not NULL:
            arguments = [null_or_self] + arguments

        tmp = func(*arguments)
        self.push(tmp)

    def call_kw_op(self, argval: int, arg) -> None:
        kw_names = self.pop()
        num_kw = len(kw_names) if kw_names else 0
        num_pos = argval - num_kw

        kw_values = self.popn(num_kw)
        pos_args = self.popn(num_pos)
        first = self.pop()
        second = self.pop()

        if first is NULL:
            callable_obj = second
            null_or_self = first
        elif second is NULL:
            callable_obj = first
            null_or_self = second
        else:
            callable_obj = second
            null_or_self = first

        if null_or_self is not NULL:
            pos_args = [null_or_self] + pos_args

        kwargs = {kw_names[i]: kw_values[i] for i in range(num_kw)}
        self.push(callable_obj(*pos_args, **kwargs))

    def load_fast_and_clear_op(self, argval, arg) -> None:
        if argval in self.locals:
            self.push(self.locals[argval])
        else:
            self.push(None)
        self.locals[argval] = None

    def swap_op(self, argval, arg):
        self.data_stack[-argval], self.data_stack[-1] = self.data_stack[-1], self.data_stack[-argval]

    def store_fast_load_fast_op(self, argval, arg):
        self.locals[argval[0]] = self.pop()
        self.push(self.locals[argval[1]])

    def list_append_op(self, argval, arg):
        item = self.pop()
        self.data_stack[-argval].append(item)

    def build_map_op(self, argval, arg):
        tmp = self.popn(2 * argval)
        self.push({tmp[i]: tmp[i + 1] for i in range(0, 2 * argval, 2)})

    def load_fast_load_fast_op(self, argval, arg):
        self.push(self.locals[argval[0]])
        self.push(self.locals[argval[1]])

    def map_add_op(self, argval, arg):
        value = self.pop()
        key = self.pop()
        self.data_stack[-argval][key] = value

    def set_add_op(self, argval, arg):
        item = self.pop()
        self.data_stack[-argval].add(item)

    def copy_op(self, argval, arg):
        if len(self.data_stack) < argval:
            pass
        else:
            self.push(self.data_stack[-argval])

    def is_op_op(self, argval, arg):
        b = self.pop()
        a = self.pop()
        if argval == 0:
            self.push(a is b)
        else:
            self.push(a is not b)

    def unary_negative_op(self, argval, arg):
        self.push(-self.pop())

    def unary_not_op(self, argval, arg):
        self.push(not self.pop())

    def unary_invert_op(self, argval, arg):
        self.push(~self.pop())

    def store_attr_op(self, argval, arg):
        obj = self.pop()
        value = self.pop()
        setattr(obj, argval, value)

    def delete_attr_op(self, argval, arg):
        obj = self.pop()
        delattr(obj, argval)

    def delete_name_op(self, argval, arg):
        if argval in self.locals:
            del self.locals[argval]
        elif argval in self.globals:
            del self.globals[argval]
        else:
            raise NameError(
                f"all variable '{argval}' referenced before assignment")

    def delete_fast_op(self, argval, arg):
        if argval in self.locals:
            del self.locals[argval]
        else:
            raise NameError(
                f"local variable '{argval}' referenced before assignment"
            )

    def load_fast_check_op(self, argval, arg):
        if argval not in self.locals:
            raise UnboundLocalError(
                f"cannot access local variable '{argval}' where it is not associated with a value"
            )
        self.push(self.locals[argval])

    def load_build_class_op(self, argval, arg):
        self.push(__build_class__)

    def load_name_op(self, argval: str, arg) -> None:
        """
        Partial realization

        Operation description:
            https://docs.python.org/release/3.13.7/library/dis.html#opcode-LOAD_NAME
        """
        if argval in self.locals:
            self.push(self.locals[argval])
        elif argval in self.globals:
            self.push(self.globals[argval])
        elif argval in self.builtins:
            self.push(self.builtins[argval])
        else:
            raise NameError("tututu")

    def load_global_op(self, argval: str, arg: int) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.13.7/library/dis.html#opcode-LOAD_GLOBAL
        """
        if argval in self.globals:
            self.push(self.globals[argval])
        elif argval in self.builtins:
            self.push(self.builtins[argval])
        else:
            raise NameError(argval, arg)
        if arg & 1:
            self.push(NULL)

    def unpack_sequence_op(self, argval: int, arg):
        tmp = list(self.pop())
        n = len(tmp)
        if n < argval:
            raise ValueError(f"not enough values to unpack (expected {argval}, got {n})")
        elif n > argval:
            raise ValueError(f"too many values to unpack (expected {argval})")
        for i in tmp[::-1]:
            self.push(i)

    def delete_subscr_op(self, arval, arg):
        key = self.pop()
        container = self.pop()
        del container[key]

    def store_slice_op(self, arval, arg):
        end = self.pop()
        start = self.pop()
        container = self.pop()
        values = self.pop()
        container[start:end] = values

    def list_extend_op(self, arval, arg):
        seq = self.pop()
        self.data_stack[-arval].extend(seq)

    def build_tuple_op(self, arval, arg):
        if arval == 0:
            value = ()
        else:
            value = tuple(self.data_stack[-arval:])
            self.data_stack = self.data_stack[:-arval]

        self.push(value)

    def build_list_op(self, arval, arg):
        if arval == 0:
            value = []
        else:
            value = list(self.data_stack[-arval:])
            self.data_stack = self.data_stack[:-arval]

        self.push(value)

    def build_const_key_map_op(self, argval, arg):
        keys = self.pop()
        values = [self.pop() for i in range(argval)][::-1]
        rez = {}
        for i in range(argval):
            rez[keys[i]] = values[i]
        self.push(rez)

    def build_set_op(self, argval, arg):
        if argval == 0:
            value = set()
        else:
            value = set(self.data_stack[-argval:])
            self.data_stack = self.data_stack[:-argval]

        self.push(value)

    def set_update_op(self, argval, arg):
        seq = self.pop()
        self.data_stack[-argval].update(seq)

    def convert_value_op(self, argval, arg):
        value = self.pop()
        if arg == 1:
            result = str(value)
        elif arg == 2:
            result = repr(value)
        elif arg == 3:
            result = ascii(value)
        else:
            raise TypeError(f'convert_value_op {argval} {arg}')
        self.push(result)

    def format_simple_op(self, argval, arg):
        value = self.pop()
        result = value.__format__("")
        self.push(result)

    def load_fast_op(self, argval, arg):
        self.push(self.locals[argval])

    def build_string_op(self, argval, arg):
        self.push(''.join([self.pop() for i in range(argval)][::-1]))

    def copy_free_vars_op(self, argval, arg):
        """
        COPY_FREE_VARS(n): Копирует n свободных переменных из closure во фрейм.

        argval — количество свободных переменных для копирования (n)
        """
        closure = self._closure
        for i in range(argval):
            cell = closure[i]
            var_name = self.code.co_freevars[i]
            self.locals[var_name] = cell

    def load_deref_op(self, argval, arg):
        var_name = argval
        cell = self.locals.get(var_name)
        if cell is None:
            if self._closure and arg < len(self._closure):
                cell = self._closure[arg]

        self.push(cell.cell_contents) # type: ignore

    def set_function_attribute_op(self, argval, arg):
        """
        SET_FUNCTION_ATTRIBUTE: Устанавливает атрибут функции.

        arg (flag):
            0x01 — __defaults__ (tuple positional defaults)
            0x02 — __kwdefaults__ (dict kw-only defaults)
            0x04 — __annotations__ (tuple of annotations)
            0x08 — __closure__ (tuple of cells)

        Стек до: [..., func, value]
        Стек после: [..., func]
        """
        func = self.pop()
        value = self.pop()

        if arg == 0x01:
            func.__defaults__ = value
        elif arg == 0x02:
            func.__kwdefaults__ = value
        elif arg == 0x04:
            if isinstance(value, tuple):
                value = {value[i]: value[i + 1] for i in range(0, len(value), 2)}
            func.__annotations__ = value
        elif arg == 0x08:
            func.__closure__ = value
        else:
            raise ValueError(f"SET_FUNCTION_ATTRIBUTE: unknown flag {arg}")

        self.push(func)

    def store_deref_op(self, argval, arg):
        """
        STORE_DEREF(i): Сохраняет значение в cell по индексу i в co_freevars.

        argval — индекс в co_freevars
        Используется для nonlocal переменных.

        Стек: [..., value] → [..., ]
        """
        value = self.pop()

        var_name = argval

        cell = self.locals.get(var_name)

        if cell is None:
            if self._closure and arg < len(self._closure):
                cell = self._closure[arg]

        if cell is None:
            raise NameError(
                f"free variable '{var_name}' referenced before assignment"
            )

        # 4. Записываем значение в cell (не заменяем cell!)
        cell.cell_contents = value

    def delete_deref_op(self, argval, arg):
        var_name = argval
        cell = self.locals.get(var_name)
        if cell is None:
            if self._closure and arg < len(self._closure):
                cell = self._closure[arg]
            else:
                raise NameError(f"free variable '{var_name}' referenced before assignment")
        try:
            del cell.cell_contents
        except AttributeError:
            raise NameError(f"free variable '{var_name}' referenced before assignment")

    def store_subscr_op(self, argval, arg):
        key = self.pop()
        container = self.pop()
        value = self.pop()
        container[key] = value

    def make_cell_op(self, argval, arg):
        """
        MAKE_CELL(i): Создаёт новую ячейку (cell) в слоте i.

        argval — индекс переменной в co_cellvars

        Если в locals[argval] есть значение, оно сохраняется в ячейку.
        Затем locals[argval] заменяется на cell object.

        Стек: не изменяется
        Locals: locals[i] → cell
        """
        import types

        var_name = argval
        value = self.locals.get(var_name, None)

        if value is not None:
            cell = types.CellType(value)
        else:
            cell = types.CellType()

        self.locals[var_name] = cell

    def load_attr_op(self, argval, arg):
        obj = self.pop()
        if hasattr(obj, argval):
            attr = getattr(obj, argval)
        else:
            if str(obj).find('<class') != -1 or str(obj).find('object') != -1:
                raise AttributeError()
            else:
                raise TypeError()

        if arg & 1:
            self.push(attr)
            self.push(NULL)
        else:
            self.push(attr)

    def binary_subscr_op(self, argval, arg):
        key = self.pop()
        container = self.pop()
        self.push(container[key])

    def build_slice_op(self, argval, arg):
        if argval == 2:
            end = self.pop()
            start = self.pop()
            self.push(slice(start, end))
        else:
            step = self.pop()
            end = self.pop()
            start = self.pop()
            self.push(slice(start, end, step))

    def binary_slice_op(self, argval, arg):
        end = self.pop()
        start = self.pop()
        container = self.pop()
        self.push(container[start:end])

    def pop_jump_if_true_op(self, argval, arg):
        if self.pop():
            self.jump(argval)

    def pop_jump_if_false_op(self, argval, arg):
        if not self.pop():
            self.jump(argval)

    def import_name_op(self, argval, arg) -> None:
        """IMPORT_NAME — базовая реализация"""
        fromlist = self.pop()
        level = self.pop()
        module = __import__(argval, fromlist=fromlist or (), level=level)
        self.push(module)

    def import_from_op(self, argval, arg) -> None:
        """IMPORT_FROM — получить имя из модуля"""
        module = self.top()
        self.push(getattr(module, argval))

    def to_bool_op(self, argval, arg):
        self.push(bool(self.pop()))

    def dict_update_op(self, argval, arg):
        map = self.pop()
        self.data_stack[-argval].update(map)

    def compare_op_op(self, argval, arg):
        fl = arg & 16
        op = arg >> 5
        b = self.pop()
        a = self.pop()

        rez = 0
        if op == 0:
            rez = a < b
        elif op == 1:
            rez = a <= b
        elif op == 2:
            rez = a == b
        elif op == 3:
            rez = a != b
        elif op == 4:
            rez = a > b
        elif op == 5:
            rez = a >= b
        elif op == 6:
            rez = a in b
        elif op == 7:
            rez = a not in b
        elif op == 8:
            rez = a is b
        elif op == 9:
            rez = a is not b
        else:
            raise ValueError('no oper')

        if fl:
            rez = bool(rez)

        self.push(rez)

    def binary_op_op(self, op: int, arg) -> None:
        b = self.pop()
        a = self.pop()

        if op == 0:
            self.push(a + b)
        elif op == 1:
            self.push(a & b)
        elif op == 2:
            self.push(a // b)
        elif op == 3:
            self.push(a << b)
        elif op == 4:
            self.push(a @ b)
        elif op == 5:
            self.push(a * b)
        elif op == 6:
            self.push(a % b)
        elif op == 7:
            self.push(a | b)
        elif op == 8:
            self.push(a ** b)
        elif op == 9:
            self.push(a >> b)
        elif op == 10:
            self.push(a - b)
        elif op == 11:
            self.push(a / b)
        elif op == 12:
            self.push(a ^ b)
        elif op == 13:
            self.push(a + b)
        elif op == 14:
            self.push(a & b)
        elif op == 15:
            self.push(a // b)
        elif op == 16:
            self.push(a << b)
        elif op == 17:
            a @= b
            self.push(a)
        elif op == 18:
            self.push(a * b)
        elif op == 19:
            self.push(a % b)
        elif op == 20:
            self.push(a | b)
        elif op == 21:
            self.push(a ** b)
        elif op == 22:
            self.push(a >> b)
        elif op == 23:
            self.push(a - b)
        elif op == 24:
            self.push(a / b)
        elif op == 25:
            self.push(a ^ b)
        else:
            raise TypeError()

    def pop_jump_if_not_none_op(self, argval, arg) -> None:
        if self.pop() is not None:
            self.jump(argval)

    def call_intrinsic_1_op(self, argval, arg: int) -> None:
        argument = self.pop()

        if arg == 1:
            print(argument)
            result = None
        elif arg == 2:
            if isinstance(argument, str):
                module = __import__(argument, fromlist=['*'])
            else:
                module = argument
            if hasattr(module, '__all__'):
                names = module.__all__
            else:
                names = [name for name in dir(
                    module) if not name.startswith('_')]
            for name in names:
                self.globals[name] = getattr(module, name)
            result = None
        elif arg == 3:
            if isinstance(argument, StopIteration):
                exc = RuntimeError("generator raised StopIteration")
                exc.__cause__ = argument
                result = exc
            else:
                result = argument
        elif arg == 5:
            result = argument
        elif arg == 6:
            result = tuple(argument) if isinstance(
                argument, list) else argument
        elif arg == 11:
            name, type_params, compute_value = argument

            class _LazyTypeAlias:
                def __init__(self, _name, _type_params, _compute):
                    self.__name__ = _name
                    self.__type_params__ = _type_params if _type_params else ()
                    self._compute = _compute

                @property
                def __value__(self):
                    return self._compute()

            result = _LazyTypeAlias(name, type_params, compute_value)
        elif arg == 0:
            raise ValueError(f"CALL_INTRINSIC_1: invalid intrinsic {arg}")
        else:
            result = argument

        self.push(result)

    def load_const_op(self, argval: tp.Any, arg) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.13.7/library/dis.html#opcode-LOAD_CONST
        """
        self.push(argval)

    def return_value_op(self, argval: tp.Any, arg) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.13.7/library/dis.html#opcode-RETURN_VALUE
        """
        self.return_value = self.pop()
        self.nam_instruction = -1

    def pop_top_op(self, argval: tp.Any, arg) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.13.7/library/dis.html#opcode-POP_TOP
        """
        self.pop()

    def make_function_op(self, argval: int, arg) -> None:
        """
        MAKE_FUNCTION: Создаёт функцию из code object.
        """
        self.push(Function(argval, arg, self))

    def dict_merge_op(self, arg, argval):
        q = self.pop()
        for i in q.keys():
            if i in self.data_stack[-argval]:
                raise
        self.data_stack[-argval].update(q)

    def call_function_ex_op(self, arg, argval):
        if arg & 1:
            kwargs = self.pop()
        else:
            kwargs = {}
        args = self.pop()
        first = self.pop()
        second = self.pop()

        if first is NULL:
            func = second
            null_or_self = first
        elif second is NULL:
            func = first
            null_or_self = second
        else:
            func = second
            null_or_self = first

        if null_or_self is not NULL:
            if isinstance(args, tuple):
                args = (null_or_self,) + args
            elif isinstance(args, list):
                args = [null_or_self] + args
            else:
                args = tuple([null_or_self] + list(args))
        if not isinstance(args, (tuple, list)):
            args = tuple(args)
        result = func(*args, **kwargs)
        self.push(result)

    def nop_op(self, argval, arg):
        pass

    def cleanup_throw_op(self, argval, arg):
        exc = self.pop()
        raise exc

    def end_async_for_op(self, argval, arg):
        self.pop()

    def get_aiter_op(self, argval, arg):
        obj = self.pop()
        self.push(obj.__aiter__())

    def get_anext_op(self, argval, arg):
        aiter = self.top()
        self.push(aiter.__anext__())

    def jump_forward_op(self, argval, arg):
        self.jump(argval)

    def contains_op_op(self, argval, arg):
        b = self.pop()
        a = self.pop()
        if argval:
            self.push(a not in b)
        else:
            self.push(a in b)

    def pop_jump_if_none_op(self, argval, arg):
        if self.pop() is None:
            self.jump(argval)

    def setup_annotations_op(self, argval, arg) -> None:
        if '__annotations__' not in self.locals:
            self.locals['__annotations__'] = {}

    def store_annotation_op(self, argval: str, arg) -> None:
        annotation = self.pop()
        if '__annotations__' not in self.locals:
            self.locals['__annotations__'] = {}
        self.locals['__annotations__'][argval] = annotation

    def store_name_op(self, argval: str, arg) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.13.7/library/dis.html#opcode-STORE_NAME
        """
        const = self.pop()
        self.locals[argval] = const

    def store_global_op(self, argval: str, arg) -> None:
        const = self.pop()
        self.globals[argval] = const

    def delete_global_op(self, argval: str, arg) -> None:
        if argval not in self.globals:
            raise NameError(f"name '{argval}' is not defined")
        del self.globals[argval]

    def load_locals_op(self, argval, arg) -> None:
        self.push(self.locals)

    def load_from_dict_or_deref_op(self, argval, arg) -> None:
        d = self.pop()
        if argval in d:
            val = d[argval]
            if hasattr(val, 'cell_contents'):
                self.push(val.cell_contents)
            else:
                self.push(val)
        else:
            cell = self.locals.get(argval)
            if cell is None and self._closure and arg < len(self._closure):
                cell = self._closure[arg]
            if cell is None:
                raise NameError(f"name '{argval}' is not defined")
            self.push(cell.cell_contents)

    def load_from_dict_or_globals_op(self, argval, arg) -> None:
        d = self.pop()
        if argval in d:
            self.push(d[argval])
        elif argval in self.globals:
            self.push(self.globals[argval])
        elif argval in self.builtins:
            self.push(self.builtins[argval])
        else:
            raise NameError(f"name '{argval}' is not defined")

    def load_assertion_error_op(self, argval, arg) -> None:
        self.push(AssertionError)

    def format_with_spec_op(self, argval, arg) -> None:
        spec = self.pop()
        value = self.pop()
        self.push(value.__format__(spec))

    def load_super_attr_op(self, argval, arg) -> None:
        self_ = self.pop()
        cls = self.pop()
        super_callable = self.pop()
        sup = super_callable(cls, self_)
        attr = getattr(sup, argval)
        if arg & 1:
            self.push(attr)
            self.push(NULL)
        else:
            self.push(attr)

    def match_sequence_op(self, argval, arg) -> None:
        import collections.abc
        subject = self.top()
        self.push(
            isinstance(subject, collections.abc.Sequence)
            and not isinstance(subject, (str, bytes, bytearray))
        )

    def match_mapping_op(self, argval, arg) -> None:
        import collections.abc
        self.push(isinstance(self.top(), collections.abc.Mapping))

    def get_len_op(self, argval, arg) -> None:
        self.push(len(self.top()))

    def match_keys_op(self, argval, arg) -> None:
        # Per CPython docs: neither TOS (keys) nor TOS1 (subject) is popped
        keys = self.top()               # peek at TOS
        subject = self.data_stack[-2]   # peek at TOS1
        try:
            values = tuple(subject[k] for k in keys)
            self.push(values)
        except KeyError:
            self.push(None)

    def match_class_op(self, argval, arg) -> None:
        kwnames = self.pop()
        type_ = self.pop()
        subject = self.top()
        if not isinstance(subject, type_):
            self.push(None)
            return
        match_args = getattr(type_, '__match_args__', ())
        attrs = []
        try:
            for i in range(arg):
                if i < len(match_args):
                    attrs.append(getattr(subject, match_args[i]))
                else:
                    self.push(None)
                    return
            for kw in kwnames:
                attrs.append(getattr(subject, kw))
        except AttributeError:
            self.push(None)
            return
        self.push(tuple(attrs))

    def check_eg_match_op(self, argval, arg) -> None:
        match_type = self.pop()
        exc_value = self.pop()
        if hasattr(exc_value, 'split'):
            match, rest = exc_value.split(match_type)
        elif isinstance(exc_value, BaseException):
            if isinstance(exc_value, match_type):
                match, rest = exc_value, None
            else:
                match, rest = None, exc_value
        else:
            match, rest = None, exc_value
        self.push(rest)
        self.push(match)

    def call_intrinsic_2_op(self, argval, arg) -> None:
        value1 = self.pop()
        value2 = self.pop()
        if arg == 1:  # INTRINSIC_PREP_RERAISE_STAR
            orig = value2
            excs = value1
            raised = [e for e in excs if e is not None]
            if not raised:
                self.push(None)
            else:
                try:
                    result = BaseExceptionGroup(
                        orig.args[0] if orig and orig.args else 'error', raised
                    )
                    self.push(result)
                except Exception:
                    self.push(raised[0] if raised else None)
        else:
            self.push(value2)

    def resume_generator(self) -> tp.Any:
        if self._generator_finished:
            return None

        if self._waiting_for_send:
            self._waiting_for_send = False
            self.push(self._yield_value)
            self._yield_value = None

        while self.nam_instruction != -1:
            tmp = self.nam_instruction
            ins, self.nam_instruction = self.ins_map[self.nam_instruction]

            if ins.opname == 'YIELD_VALUE':
                self._waiting_for_send = True
                return self.pop()

            try:
                getattr(self, ins.opname.lower() + "_op")(ins.argval, ins.arg)
            except BaseException as e:
                self.nam_instruction = tmp
                handler, depth, lasti = self.find_exception_handler(e)
                if handler is None:
                    raise
                self.last_exception = e #pyrefly: ignore
                if len(self.data_stack) > depth:
                    self.data_stack = self.data_stack[:depth]
                if lasti:
                    self.push(tmp)
                self.push(e)
                self.jump(handler)
                continue

        self._generator_finished = True
        return self.return_value


class VirtualMachine:
    def run(self, code_obj: types.CodeType) -> None:
        """
        :param code_obj: code for interpreting
        """
        globals_context: dict[str, tp.Any] = {}
        frame = Frame(code_obj, builtins.globals()[
                      '__builtins__'], globals_context, globals_context)
        return frame.run()


class Generator:
    def __init__(self, frame):
        self.frame: Frame = frame
        self.started = False
        self.closed = False
        self._exhausted = False

    def __iter__(self):
        return self

    def __next__(self):
        return self.send(None)

    def send(self, val):
        if self.closed:
            raise RuntimeError("generator is already closed")

        if self._exhausted:
            raise StopIteration(self.frame.return_value)

        if not self.started and val is not None:
            raise TypeError(
                "can't send non-None value to just-started generator")

        if not self.started:
            self.started = True
            self.frame.return_value = None

        self.frame._yield_value = val

        try:
            result = self.frame.resume_generator()
        except StopIteration as e:
            raise RuntimeError("generator raised StopIteration") from e

        if self.frame._generator_finished:
            self._exhausted = True
            raise StopIteration(self.frame.return_value)

        return result

    def throw(self, typ, val=None, tb=None):
        if self.closed:
            raise RuntimeError("generator is already closed")
        if val is None:
            val = typ()
        raise val

    def close(self):
        self.closed = True
        self.frame._generator_finished = True

    def __aiter__(self):
        return self

    def __anext__(self):
        async def _next():
            try:
                return self.send(None)
            except StopIteration:
                raise StopAsyncIteration
        return _next()


def __build_class__(func, name, *bases, **kwds):

    new_bases = []
    for base in bases:
        if hasattr(base, '__mro_entries__'):
            new_bases.extend(base.__mro_entries__(bases))
        else:
            new_bases.append(base)
    bases = tuple(new_bases)

    if 'metaclass' in kwds:
        metaclass = kwds.pop('metaclass')
    elif bases:
        metaclass = type(bases[0])
    else:
        metaclass = type

    for base in bases:
        base_meta = type(base)
        if issubclass(metaclass, base_meta):
            continue
        elif issubclass(base_meta, metaclass):
            metaclass = base_meta
        else:
            raise TypeError(
                "metaclass conflict: the metaclass of a derived class "
                "must be a (non-strict) subclass of the metaclasses of all its bases"
            )

    if hasattr(metaclass, '__prepare__'):
        namespace = metaclass.__prepare__(name, bases, **kwds)
    else:
        namespace = {}

    func(namespace)
    cls = metaclass(name, bases, namespace, **kwds)
    return cls


class Function:
    __slots__ = (
        'code', 'has_var_positional', 'has_var_keyword', 'is_generator',
        'num_regular_args', 'kwonly_count', '_globals_ref', '__builtins_ref__',
        '__name__', '__qualname__', '__closure__', '__defaults__', '__code__',
        '__globals__', '__kwdefaults__', '__annotations__',
        '__dict__',
    )

    def __init__(self, argval, arg, frame: Frame):
        if argval is None:
            argval = 0

        closure = ()
        annotations = None
        kwdefaults = None
        defaults = None
        self.code = frame.pop()
        code = self.code
        flags = argval if argval is not None else 0
        if flags & 0x08:
            closure = frame.pop()
            if not isinstance(closure, tuple):
                closure = tuple(closure)
        if flags & 0x04:
            annotations = frame.pop()
        if flags & 0x02:
            kwdefaults = frame.pop()
        if flags & 0x01:
            defaults = frame.pop()
        self.has_var_positional = bool(code.co_flags & 0x04)
        self.has_var_keyword = bool(code.co_flags & 0x08)
        self.is_generator = bool(code.co_flags & 0x20)
        self.is_coroutine = bool(code.co_flags & 0x80)

        self.num_regular_args = code.co_argcount
        self.kwonly_count = getattr(code, 'co_kwonlyargcount', 0)

        self._globals_ref = frame.globals

        self.__name__ = code.co_name
        self.__builtins_ref__ = frame.builtins
        self.__qualname__ = code.co_name
        self.__closure__ = closure
        self.__defaults__ = defaults
        self.__code__ = code
        self.__globals__ = frame.globals
        self.__kwdefaults__ = kwdefaults
        self.__annotations__ = annotations if annotations else {}
        self.__module__ = frame.globals.get('__module__', '__main__')
        self.__doc__ = code.co_consts[0] if code.co_consts and isinstance(
            code.co_consts[0], str) else None

    def __call__(self, *call_args, **call_kwargs):
        is_class_body = (self.__code__.co_argcount == 0
                         and len(call_args) == 1
                         and isinstance(call_args[0], dict)
                         and not call_kwargs)

        if is_class_body:
            f_locals = call_args[0]
        else:
            current_defaults = ()
            if hasattr(self, '__defaults__'):
                current_defaults = self.__defaults__
            current_kwdefaults = self.__kwdefaults__

            f_locals = self._bind_args_for_vm(
                self.__code__, current_defaults, current_kwdefaults, call_args, call_kwargs)  # type: ignore
            if not hasattr(self, 'is_generator'):
                self.is_generator = False

            if self.is_generator and call_args and '.0' not in f_locals:
                f_locals['.0'] = call_args[0]

        frame_closure = ()
        if hasattr(self, '__closure__'):
            frame_closure = self.__closure__

        frame = Frame(self.__code__, self.__builtins_ref__,
                      self.__globals__, f_locals, frame_closure)
        rez = frame.run()

        if is_class_body:
            return call_args[0]

        if self.is_coroutine and isinstance(rez, Generator):
            vm_gen = rez

            @types.coroutine
            def _wrap():
                return (yield from vm_gen)

            return _wrap()

        return rez

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return types.MethodType(self, instance)

    def _bind_args_for_vm(self, code: types.CodeType, defaults: tuple, kwdefaults: dict,
                          call_args: tuple, call_kwargs: dict) -> dict[str, tp.Any]:
        CO_VARARGS = 0x04
        CO_VARKEYWORDS = 0x08

        result: dict[str, tp.Any] = {}

        have_args = bool(code.co_flags & CO_VARARGS)
        have_kwargs = bool(code.co_flags & CO_VARKEYWORDS)

        args_name = ''
        kwargs_name = ''

        kwonly_count = getattr(code, 'co_kwonlyargcount', 0)
        posonly_count = getattr(code, 'co_posonlyargcount', 0)
        total_params = code.co_argcount + kwonly_count + \
            (1 if have_args else 0) + (1 if have_kwargs else 0)

        if have_args:
            args_name = code.co_varnames[total_params - 1 - have_kwargs]
            result[args_name] = []

        if have_kwargs:
            kwargs_idx = code.co_argcount + \
                kwonly_count + (1 if have_args else 0)
            kwargs_name = code.co_varnames[kwargs_idx]
            result[kwargs_name] = {}

        if defaults is None:
            defaults = ()
        if kwdefaults is None:
            kwdefaults = {}

        for i, value in enumerate(call_args):
            if i < code.co_argcount:
                name = code.co_varnames[i]
                if name in result:
                    raise TypeError(
                        "multiple values for argument '{}'".format(name))
                result[name] = value
            elif have_args:
                result[args_name].append(value)
            else:
                raise TypeError("too many positional arguments")

        for name, value in call_kwargs.items():
            param_idx = -1
            for i in range(total_params):
                if code.co_varnames[i] == name:
                    param_idx = i
                    break

            if param_idx == -1:
                if have_kwargs:
                    result[kwargs_name][name] = value
                else:
                    raise TypeError(
                        "got an unexpected keyword argument '{}'".format(name))
            elif param_idx < posonly_count:
                raise TypeError(
                    "positional-only argument '{}' passed as keyword".format(name))
            elif name in result:
                raise TypeError(
                    "multiple values for argument '{}'".format(name))
            else:
                result[name] = value

        num_defaults = len(defaults)
        for i in range(num_defaults):
            param_idx = code.co_argcount - num_defaults + i
            name = code.co_varnames[param_idx]
            if name not in result:
                result[name] = defaults[i]

        for name, value in kwdefaults.items():
            if name not in result:
                result[name] = value

        for i in range(code.co_argcount):
            name = code.co_varnames[i]
            if name not in result:
                if i < posonly_count:
                    raise TypeError(
                        "missing required positional argument: '{}'".format(name))
                else:
                    raise TypeError(
                        "missing required argument: '{}'".format(name))

        kwonly_start = code.co_argcount + (1 if have_args else 0)
        for i in range(kwonly_count):
            name = code.co_varnames[kwonly_start + i]
            if name not in result:
                raise TypeError(
                    "missing required keyword-only argument: '{}'".format(name))

        if have_args:
            result[args_name] = tuple(result[args_name])

        return result
