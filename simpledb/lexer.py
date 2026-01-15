### `simpledb/lexer.py
"""
simpledb/lexer.py

SQL-like tokenizer (lexer) for the SimpleDB mini-RDBMS.

Responsibilities:
- Convert an input SQL string into a list of tokens with line/column positions
- Recognize keywords, identifiers, literals, and punctuation used by our SQL subset
- Provide reliable error messages for unexpected characters and unterminated strings

Notes:
- This is a deliberately small SQL subset.
- String literals use single quotes: 'hello'
- Booleans: true/false (case-insensitive)
- NULL is tokenized as a keyword; the parser/executor decide where it is valid.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

from .errors import Position, SqlSyntaxError


class TokenType(Enum):
    """Token categories recognized by the lexer."""
    EOF = auto()

    # Identifiers + literals
    IDENT = auto()
    INT = auto()
    STRING = auto()
    BOOL = auto()
    NULL = auto()

    # Symbols
    LPAREN = auto()   # (
    RPAREN = auto()   # )
    COMMA = auto()    # ,
    SEMI = auto()     # ;
    EQ = auto()       # =
    STAR = auto()     # *
    DOT = auto()      # .

    # Keywords (subset)
    CREATE = auto()
    TABLE = auto()
    INDEX = auto()
    INSERT = auto()
    INTO = auto()
    VALUES = auto()
    SELECT = auto()
    FROM = auto()
    WHERE = auto()
    AND = auto()
    UPDATE = auto()
    SET = auto()
    DELETE = auto()
    JOIN = auto()
    ON = auto()
    PRIMARY = auto()
    KEY = auto()
    UNIQUE = auto()
    NOT = auto()


KEYWORDS: dict[str, TokenType] = {
    "CREATE": TokenType.CREATE,
    "TABLE": TokenType.TABLE,
    "INDEX": TokenType.INDEX,
    "INSERT": TokenType.INSERT,
    "INTO": TokenType.INTO,
    "VALUES": TokenType.VALUES,
    "SELECT": TokenType.SELECT,
    "FROM": TokenType.FROM,
    "WHERE": TokenType.WHERE,
    "AND": TokenType.AND,
    "UPDATE": TokenType.UPDATE,
    "SET": TokenType.SET,
    "DELETE": TokenType.DELETE,
    "JOIN": TokenType.JOIN,
    "ON": TokenType.ON,
    "PRIMARY": TokenType.PRIMARY,
    "KEY": TokenType.KEY,
    "UNIQUE": TokenType.UNIQUE,
    "NOT": TokenType.NOT,
    "NULL": TokenType.NULL,
}


@dataclass(frozen=True)
class Token:
    """
    A lexical token.

    Attributes:
        typ: TokenType
        lexeme: The original text fragment (or best-effort representation)
        value: Parsed value for literals/idents:
               - IDENT -> str
               - INT -> int
               - STRING -> str (without quotes)
               - BOOL -> bool
               - NULL -> None
        pos: Position in input (line/col)
    """
    typ: TokenType
    lexeme: str
    value: object | None
    pos: Position


def tokenize(sql: str) -> list[Token]:
    """
    Tokenize a SQL-like string into a list of Token objects.

    Args:
        sql: Raw SQL input string.

    Returns:
        List of Token, always terminated with EOF token.

    Raises:
        SqlSyntaxError: for unexpected characters or unterminated strings.
    """
    tokens: list[Token] = []
    i = 0
    line = 1
    col = 1

    def cur_pos() -> Position:
        return Position(line=line, col=col)

    def peek(offset: int = 0) -> str:
        j = i + offset
        if j >= len(sql):
            return ""
        return sql[j]

    def advance(n: int = 1) -> None:
        """Advance the cursor by n characters while tracking line/column."""
        nonlocal i, line, col
        for _ in range(n):
            if i >= len(sql):
                return
            ch = sql[i]
            i += 1
            if ch == "\n":
                line += 1
                col = 1
            else:
                col += 1

    while i < len(sql):
        ch = peek(0)

        # Skip whitespace
        if ch.isspace():
            advance(1)
            continue

        # Single-character symbols
        if ch == "(":
            tokens.append(Token(TokenType.LPAREN, ch, None, cur_pos()))
            advance(1)
            continue
        if ch == ")":
            tokens.append(Token(TokenType.RPAREN, ch, None, cur_pos()))
            advance(1)
            continue
        if ch == ",":
            tokens.append(Token(TokenType.COMMA, ch, None, cur_pos()))
            advance(1)
            continue
        if ch == ";":
            tokens.append(Token(TokenType.SEMI, ch, None, cur_pos()))
            advance(1)
            continue
        if ch == "=":
            tokens.append(Token(TokenType.EQ, ch, None, cur_pos()))
            advance(1)
            continue
        if ch == "*":
            tokens.append(Token(TokenType.STAR, ch, None, cur_pos()))
            advance(1)
            continue
        if ch == ".":
            tokens.append(Token(TokenType.DOT, ch, None, cur_pos()))
            advance(1)
            continue

        # String literal: '...'
        if ch == "'":
            start = cur_pos()
            advance(1)  # consume opening quote
            buf: list[str] = []
            while True:
                if i >= len(sql):
                    raise SqlSyntaxError("Unterminated string literal", start)
                c = peek(0)
                if c == "'":
                    advance(1)  # consume closing quote
                    break
                # Note: escaping not supported in this minimal lexer.
                buf.append(c)
                advance(1)
            s = "".join(buf)
            tokens.append(Token(TokenType.STRING, f"'{s}'", s, start))
            continue

        # Integer literal
        if ch.isdigit():
            start = cur_pos()
            j = i
            while j < len(sql) and sql[j].isdigit():
                j += 1
            lex = sql[i:j]
            tokens.append(Token(TokenType.INT, lex, int(lex), start))
            advance(j - i)
            continue

        # Identifier / keyword / boolean / NULL
        if ch.isalpha() or ch == "_":
            start = cur_pos()
            j = i
            while j < len(sql) and (sql[j].isalnum() or sql[j] == "_"):
                j += 1

            lex = sql[i:j]
            upper = lex.upper()

            if upper == "TRUE":
                tokens.append(Token(TokenType.BOOL, lex, True, start))
            elif upper == "FALSE":
                tokens.append(Token(TokenType.BOOL, lex, False, start))
            elif upper == "NULL":
                tokens.append(Token(TokenType.NULL, lex, None, start))
            elif upper in KEYWORDS:
                tokens.append(Token(KEYWORDS[upper], lex, upper, start))
            else:
                tokens.append(Token(TokenType.IDENT, lex, lex, start))

            advance(j - i)
            continue

        # Unknown character
        raise SqlSyntaxError(f"Unexpected character: {ch!r}", cur_pos())

    tokens.append(Token(TokenType.EOF, "", None, Position(line=line, col=col)))
    return tokens
