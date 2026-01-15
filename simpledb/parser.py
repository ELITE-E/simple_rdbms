"""
simpledb/parser.py

Recursive-descent parser for the SimpleDB SQL subset.

Responsibilities:
- Convert token sequences into AST nodes (see simpledb/ast.py)
- Provide clear syntax errors with line/column positions
- Support a minimal SQL subset:
    - CREATE TABLE
    - CREATE INDEX
    - INSERT
    - SELECT (with optional JOINs and WHERE)
    - UPDATE
    - DELETE
- WHERE supports only equality predicates combined with AND.
- Literals supported: INT, STRING, BOOL, NULL

Notes:
- This parser does not attempt to be ANSI SQL compliant; it is intentionally small.
- JOIN queries require qualified columns in ON clauses: t1.col = t2.col
"""

from __future__ import annotations

from dataclasses import dataclass

from .ast import (
    Assignment,
    ColumnDef,
    ColumnRef,
    Condition,
    CreateIndex,
    CreateTable,
    Delete,
    Insert,
    JoinClause,
    Select,
    Statement,
    TypeSpec,
    Update,
    WhereClause,
)
from .errors import Position, SqlSyntaxError
from .lexer import Token, TokenType, tokenize


@dataclass
class Parser:
    """
    Stateful parser over a token list.

    Attributes:
        tokens: List of Token.
        i: Current token index.
    """
    tokens: list[Token]
    i: int = 0

    def peek(self, offset: int = 0) -> Token:
        """Return the token at current index + offset without consuming."""
        j = self.i + offset
        if j >= len(self.tokens):
            return self.tokens[-1]
        return self.tokens[j]

    def at(self, typ: TokenType) -> bool:
        """Check whether current token is of a specific type."""
        return self.peek().typ == typ

    def consume(self) -> Token:
        """Consume and return the current token."""
        t = self.peek()
        self.i += 1
        return t

    def expect(self, typ: TokenType, msg: str) -> Token:
        """Consume a token of the expected type, otherwise raise syntax error."""
        t = self.peek()
        if t.typ != typ:
            raise SqlSyntaxError(msg, t.pos)
        return self.consume()

    def match(self, typ: TokenType) -> bool:
        """If current token matches typ, consume it and return True."""
        if self.at(typ):
            self.consume()
            return True
        return False

    # ---------------- entry points ----------------

    def parse_script(self) -> list[Statement]:
        """
        Parse one or more statements separated by semicolons.

        Returns:
            List of Statement AST nodes.

        Notes:
            Trailing semicolons and empty statements (e.g., ";;") are allowed.
        """
        stmts: list[Statement] = []
        while not self.at(TokenType.EOF):
            if self.match(TokenType.SEMI):
                continue
            stmts.append(self.parse_statement())
            self.match(TokenType.SEMI)
        return stmts

    def parse_one(self) -> Statement:
        """
        Parse exactly one statement.

        Returns:
            A single Statement.

        Raises:
            SqlSyntaxError if input is empty or contains multiple statements.
        """
        stmts = self.parse_script()
        if not stmts:
            raise SqlSyntaxError("Empty input", Position(1, 1))
        if len(stmts) > 1:
            raise SqlSyntaxError("Expected a single statement", self.peek().pos)
        return stmts[0]

    # ---------------- statement dispatch ----------------

    def parse_statement(self) -> Statement:
        """Dispatch based on the first keyword token."""
        t = self.peek()
        if t.typ == TokenType.CREATE:
            return self.parse_create()
        if t.typ == TokenType.INSERT:
            return self.parse_insert()
        if t.typ == TokenType.SELECT:
            return self.parse_select()
        if t.typ == TokenType.UPDATE:
            return self.parse_update()
        if t.typ == TokenType.DELETE:
            return self.parse_delete()
        raise SqlSyntaxError(f"Unexpected token: {t.lexeme!r}", t.pos)

    # ---------------- CREATE ----------------

    def parse_create(self) -> Statement:
        """
        CREATE statement dispatcher:
          - CREATE TABLE ...
          - CREATE INDEX ...
        """
        self.expect(TokenType.CREATE, "Expected CREATE")

        if self.match(TokenType.TABLE):
            return self.parse_create_table_after_keyword()
        if self.match(TokenType.INDEX):
            return self.parse_create_index_after_keyword()

        raise SqlSyntaxError("Expected TABLE or INDEX after CREATE", self.peek().pos)

    def parse_create_table_after_keyword(self) -> CreateTable:
        """
        Parse:
          CREATE TABLE <name> ( <coldef>, <coldef>, ... )
        """
        table = str(self.expect(TokenType.IDENT, "Expected table name").value)
        self.expect(TokenType.LPAREN, "Expected '(' after table name")

        cols: list[ColumnDef] = []
        cols.append(self.parse_column_def())
        while self.match(TokenType.COMMA):
            cols.append(self.parse_column_def())

        self.expect(TokenType.RPAREN, "Expected ')' after column definitions")
        return CreateTable(table_name=table, columns=cols)

    def parse_column_def(self) -> ColumnDef:
        """
        Parse:
          <colname> <type> [NOT NULL] [UNIQUE] [PRIMARY KEY]
        Constraints may appear in any order.
        """
        col_name = str(self.expect(TokenType.IDENT, "Expected column name").value)
        typ = self.parse_type_spec()

        not_null = False
        unique = False
        primary_key = False

        while True:
            if self.match(TokenType.NOT):
                # Accept both "NOT NULL" and treat any missing NULL as error
                self.expect(TokenType.NULL, "Expected NULL after NOT")
                not_null = True
                continue
            if self.match(TokenType.UNIQUE):
                unique = True
                continue
            if self.match(TokenType.PRIMARY):
                self.expect(TokenType.KEY, "Expected KEY after PRIMARY")
                primary_key = True
                continue
            break

        return ColumnDef(
            name=col_name,
            typ=typ,
            not_null=not_null,
            unique=unique,
            primary_key=primary_key,
        )

    def parse_type_spec(self) -> TypeSpec:
        """
        Parse:
          TYPE := IDENT [ '(' INT (',' INT)* ')' ]

        Examples:
          INTEGER
          VARCHAR(255)
        """
        type_name = str(self.expect(TokenType.IDENT, "Expected type name").value).upper()
        params: list[int] = []

        if self.match(TokenType.LPAREN):
            params.append(int(self.expect(TokenType.INT, "Expected integer type parameter").value))
            while self.match(TokenType.COMMA):
                params.append(int(self.expect(TokenType.INT, "Expected integer type parameter").value))
            self.expect(TokenType.RPAREN, "Expected ')' after type parameters")

        return TypeSpec(name=type_name, params=params)

    def parse_create_index_after_keyword(self) -> CreateIndex:
        """
        Parse:
          CREATE INDEX <idx_name> ON <table>(<column>)
        """
        idx_name = str(self.expect(TokenType.IDENT, "Expected index name").value)
        self.expect(TokenType.ON, "Expected ON after index name")
        table = str(self.expect(TokenType.IDENT, "Expected table name").value)
        self.expect(TokenType.LPAREN, "Expected '(' after table name")
        col = str(self.expect(TokenType.IDENT, "Expected column name").value)
        self.expect(TokenType.RPAREN, "Expected ')' after column name")
        return CreateIndex(index_name=idx_name, table_name=table, column_name=col)

    # ---------------- INSERT ----------------

    def parse_insert(self) -> Insert:
        """
        Parse:
          INSERT INTO table (c1, c2, ...) VALUES (v1, v2, ...)
        """
        self.expect(TokenType.INSERT, "Expected INSERT")
        self.expect(TokenType.INTO, "Expected INTO after INSERT")
        table = str(self.expect(TokenType.IDENT, "Expected table name").value)

        self.expect(TokenType.LPAREN, "Expected '(' before column list")
        cols = [str(self.expect(TokenType.IDENT, "Expected column name").value)]
        while self.match(TokenType.COMMA):
            cols.append(str(self.expect(TokenType.IDENT, "Expected column name").value))
        self.expect(TokenType.RPAREN, "Expected ')' after column list")

        self.expect(TokenType.VALUES, "Expected VALUES")
        self.expect(TokenType.LPAREN, "Expected '(' before values")
        vals = [self.parse_literal()]
        while self.match(TokenType.COMMA):
            vals.append(self.parse_literal())
        self.expect(TokenType.RPAREN, "Expected ')' after values")

        if len(cols) != len(vals):
            raise SqlSyntaxError("Number of columns does not match number of values", self.peek().pos)

        return Insert(table_name=table, columns=cols, values=vals)

    # ---------------- SELECT (+ JOIN, WHERE) ----------------

    def parse_select(self) -> Select:
        """
        Parse:
          SELECT <cols> FROM <table> [JOIN <t2> ON a=b]* [WHERE cond AND cond ...]
        """
        self.expect(TokenType.SELECT, "Expected SELECT")
        cols = self.parse_select_list()
        self.expect(TokenType.FROM, "Expected FROM")
        from_table = str(self.expect(TokenType.IDENT, "Expected table name").value)

        joins: list[JoinClause] = []
        while self.match(TokenType.JOIN):
            joins.append(self.parse_join_clause())

        where = None
        if self.match(TokenType.WHERE):
            where = self.parse_where_clause_after_where()

        return Select(columns=cols, from_table=from_table, joins=joins, where=where)

    def parse_select_list(self) -> list[ColumnRef] | None:
        """
        Parse:
          '*' OR colref (',' colref)*
        """
        if self.match(TokenType.STAR):
            return None
        cols = [self.parse_column_ref()]
        while self.match(TokenType.COMMA):
            cols.append(self.parse_column_ref())
        return cols

    def parse_join_clause(self) -> JoinClause:
        """
        Parse:
          JOIN <table> ON <colref> = <colref>
        """
        table = str(self.expect(TokenType.IDENT, "Expected table name after JOIN").value)
        self.expect(TokenType.ON, "Expected ON in JOIN clause")
        left = self.parse_column_ref()
        self.expect(TokenType.EQ, "Expected '=' in JOIN condition")
        right = self.parse_column_ref()
        return JoinClause(table_name=table, left=left, right=right)

    # ---------------- UPDATE ----------------

    def parse_update(self) -> Update:
        """
        Parse:
          UPDATE <table> SET c=v [,c=v]* [WHERE ...]
        """
        self.expect(TokenType.UPDATE, "Expected UPDATE")
        table = str(self.expect(TokenType.IDENT, "Expected table name").value)
        self.expect(TokenType.SET, "Expected SET")

        assignments = [self.parse_assignment()]
        while self.match(TokenType.COMMA):
            assignments.append(self.parse_assignment())

        where = None
        if self.match(TokenType.WHERE):
            where = self.parse_where_clause_after_where()

        return Update(table_name=table, assignments=assignments, where=where)

    def parse_assignment(self) -> Assignment:
        """
        Parse:
          <ident> = <literal>
        """
        col = str(self.expect(TokenType.IDENT, "Expected column name").value)
        self.expect(TokenType.EQ, "Expected '=' in assignment")
        val = self.parse_literal()
        return Assignment(column=col, value=val)

    # ---------------- DELETE ----------------

    def parse_delete(self) -> Delete:
        """
        Parse:
          DELETE FROM <table> [WHERE ...]
        """
        self.expect(TokenType.DELETE, "Expected DELETE")
        self.expect(TokenType.FROM, "Expected FROM after DELETE")
        table = str(self.expect(TokenType.IDENT, "Expected table name").value)

        where = None
        if self.match(TokenType.WHERE):
            where = self.parse_where_clause_after_where()

        return Delete(table_name=table, where=where)

    # ---------------- WHERE ----------------

    def parse_where_clause_after_where(self) -> WhereClause:
        """
        Parse:
          condition (AND condition)*
        """
        conds = [self.parse_condition()]
        while self.match(TokenType.AND):
            conds.append(self.parse_condition())
        return WhereClause(conditions=conds)

    def parse_condition(self) -> Condition:
        """
        Parse:
          <colref> = <literal>
        """
        left = self.parse_column_ref()
        self.expect(TokenType.EQ, "Expected '=' in WHERE condition")
        right = self.parse_literal()
        return Condition(left=left, op="=", right=right)

    # ---------------- atoms ----------------

    def parse_column_ref(self) -> ColumnRef:
        """
        Parse:
          IDENT | IDENT '.' IDENT
        """
        first = str(self.expect(TokenType.IDENT, "Expected identifier").value)
        if self.match(TokenType.DOT):
            second = str(self.expect(TokenType.IDENT, "Expected identifier after '.'").value)
            return ColumnRef(table=first, column=second)
        return ColumnRef(table=None, column=first)

    def parse_literal(self):
        """
        Parse a literal value.

        Returns:
            int | str | bool | None

        Raises:
            SqlSyntaxError if token is not a supported literal type.
        """
        t = self.peek()
        if t.typ == TokenType.INT:
            return int(self.consume().value)
        if t.typ == TokenType.STRING:
            return str(self.consume().value)
        if t.typ == TokenType.BOOL:
            return bool(self.consume().value)
        if t.typ == TokenType.NULL:
            self.consume()
            return None
        raise SqlSyntaxError("Expected literal (INT, STRING, BOOL, NULL)", t.pos)


# ---------- public helpers ----------

def parse_sql(sql: str) -> Statement:
    """
    Parse exactly one SQL statement.

    Args:
        sql: SQL string.

    Returns:
        AST Statement.

    Raises:
        SqlSyntaxError: if parsing fails or multiple statements provided.
    """
    tokens = tokenize(sql)
    return Parser(tokens).parse_one()


def parse_script(sql: str) -> list[Statement]:
    """
    Parse one or more SQL statements separated by semicolons.

    Args:
        sql: SQL script string.

    Returns:
        List of AST Statements.
    """
    tokens = tokenize(sql)
    return Parser(tokens).parse_script()