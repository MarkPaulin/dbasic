import sys
from enum import Enum, auto


class MetaCommandResult(Enum):
    Success = auto()
    UnrecognisedCommand = auto()


class PrepareResult(Enum):
    Success = auto()
    UnrecognisedStatement = auto()


class StatementType(Enum):
    NoType = auto()
    Insert = auto()
    Select = auto()


class Statement:
    def __init__(self, _type: StatementType = StatementType.NoType):
        self._type = _type


def print_prompt() -> None:
    print("db > ", end="")


def read_input() -> str:
    return input()


def prepare_statement(_input: str, statement: Statement) -> PrepareResult:
    if _input[:6] == "insert":
        statement._type = StatementType.Insert
        return PrepareResult.Success
    if _input == "select":
        statement._type = StatementType.Select
        return PrepareResult.Success
    return PrepareResult.UnrecognisedStatement


def execute_statement(statement: Statement):
    if statement._type == StatementType.Insert:
        print("this is where insert will go")
    if statement._type == StatementType.Select:
        print("this is where select will go")


def do_meta_command(_input: str) -> MetaCommandResult:
    if _input == ".exit":
        sys.exit(0)
        return MetaCommandResult.Success
    else:
        return MetaCommandResult.UnrecognisedCommand


def main():
    while True:
        print_prompt()
        _input = read_input()

        if _input[0] == ".":
            res = do_meta_command(_input)
            if res == MetaCommandResult.Success:
                continue
            if res == MetaCommandResult.UnrecognisedCommand:
                print(f"Unrecognised command {_input}")
                continue

        statement = Statement()
        res = prepare_statement(_input, statement)
        if res == PrepareResult.Success:
            pass
        elif res == PrepareResult.UnrecognisedStatement:
            print(f"unrecognised keyword at start of '{_input}'")
            continue

        execute_statement(statement)
        print("executed")


if __name__ == "__main__":
    main()
