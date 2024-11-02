import sys
from enum import Enum, auto
from typing import List, Optional

ID_SIZE = 4
USERNAME_SIZE = 32
EMAIL_SIZE = 220
ID_OFFSET = 0
USERNAME_OFFSET = ID_OFFSET + ID_SIZE
EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE
ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE

PAGE_SIZE = 4096
TABLE_MAX_PAGES = 100
ROWS_PER_PAGE = PAGE_SIZE // ROW_SIZE
TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES


class Row:
    def __init__(
        self,
        id: Optional[int] = None,
        username: Optional[str] = None,
        email: Optional[str] = None,
    ):
        self.id = id
        self.username = username
        self.email = email

    def __str__(self):
        return f"({self.id}, {self.username}, {self.email})"


class Table:
    def __init__(self, n_rows: int = 0):
        self.n_rows = n_rows
        self.pages: List[bytearray] = []

    def row_slot(self, row_num: int) -> tuple[int, int]:
        page_num = row_num // ROWS_PER_PAGE
        row_offset = row_num % ROWS_PER_PAGE
        byte_offset = row_offset * ROW_SIZE
        return page_num, byte_offset

    def write_row(self, row_num: int, row: Row) -> None:
        ser = serialise_row(row)
        page_num, byte_offset = self.row_slot(row_num)
        if len(self.pages) <= page_num:
            for _ in range(page_num + 1 - len(self.pages)):
                self.pages.append(bytearray(PAGE_SIZE))
        self.pages[page_num][byte_offset : (byte_offset + ROW_SIZE)] = ser

    def read_row(self, row_num: int) -> Row:
        page_num, byte_offset = self.row_slot(row_num)
        row = deserialise_row(
            self.pages[page_num][byte_offset : (byte_offset + ROW_SIZE)]
        )
        return row


def serialise_row(row: Row) -> bytearray:
    row_ser = bytearray(ROW_SIZE)
    if row.id is None or row.username is None or row.email is None:
        return row_ser
    row_ser[ID_OFFSET:USERNAME_OFFSET] = row.id.to_bytes(ID_SIZE, byteorder="big")[
        :ID_SIZE
    ]
    row_ser[USERNAME_OFFSET:EMAIL_OFFSET] = row.username.encode()[:USERNAME_SIZE]
    row_ser[EMAIL_OFFSET:] = row.email.encode()[:EMAIL_SIZE]
    return row_ser


def deserialise_row(serialised: bytearray) -> Row:
    id = int.from_bytes(serialised[ID_OFFSET:USERNAME_OFFSET], byteorder="big")
    username = serialised[USERNAME_OFFSET:EMAIL_OFFSET].rstrip(b"\x00").decode()
    email = serialised[EMAIL_OFFSET:ROW_SIZE].rstrip(b"\x00").decode()
    return Row(id=id, username=username, email=email)


class MetaCommandResult(Enum):
    Success = auto()
    UnrecognisedCommand = auto()


class PrepareResult(Enum):
    Success = auto()
    UnrecognisedStatement = auto()
    SyntaxError = auto()


class StatementType(Enum):
    NoType = auto()
    Insert = auto()
    Select = auto()


class ExecuteResult(Enum):
    Success = auto()
    TableFull = auto()
    Other = auto()


class Statement:
    def __init__(
        self, _type: StatementType = StatementType.NoType, row_to_insert: Row = Row()
    ):
        self._type = _type
        self.row_to_insert = row_to_insert


def print_prompt() -> None:
    print("db > ", end="")


def read_input() -> str:
    return input()


def prepare_statement(_input: str, statement: Statement) -> PrepareResult:
    if _input[:6] == "insert":
        statement._type = StatementType.Insert
        input_split = _input.split()
        if len(input_split) < 4:
            return PrepareResult.SyntaxError
        statement.row_to_insert = Row(
            int(input_split[1]), input_split[2], input_split[3]
        )
        return PrepareResult.Success
    if _input == "select":
        statement._type = StatementType.Select
        return PrepareResult.Success
    return PrepareResult.UnrecognisedStatement


def execute_insert_statement(statement: Statement, table: Table) -> ExecuteResult:
    if table.n_rows == TABLE_MAX_ROWS:
        return ExecuteResult.TableFull

    row = statement.row_to_insert
    table.write_row(table.n_rows, row)
    table.n_rows += 1
    return ExecuteResult.Success


def execute_select_statement(statement: Statement, table: Table) -> ExecuteResult:
    for i in range(table.n_rows):
        page_num, byte_offset = table.row_slot(i)
        row = deserialise_row(
            table.pages[page_num][byte_offset : (byte_offset + ROW_SIZE)]
        )
        print(row)
    return ExecuteResult.Success


def execute_statement(statement: Statement, table: Table) -> ExecuteResult:
    if statement._type == StatementType.Insert:
        return execute_insert_statement(statement, table)
    if statement._type == StatementType.Select:
        return execute_select_statement(statement, table)
    return ExecuteResult.Other


def do_meta_command(_input: str) -> MetaCommandResult:
    if _input == ".exit":
        sys.exit(0)
        return MetaCommandResult.Success
    else:
        return MetaCommandResult.UnrecognisedCommand


def main():  # noqa: C901
    table = Table()
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
        elif res == PrepareResult.SyntaxError:
            print("syntax error. could not parse statement")
            continue
        elif res == PrepareResult.UnrecognisedStatement:
            print(f"unrecognised keyword at start of '{_input}'")
            continue

        res = execute_statement(statement, table)
        if res == ExecuteResult.Success:
            print("executed")
        elif res == ExecuteResult.TableFull:
            print("error: table full")


if __name__ == "__main__":
    main()
