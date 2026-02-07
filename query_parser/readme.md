### Supported grammar (summary)
```
-- database management
CREATE DATABASE <name>
SHOW DATABASES
USE <name>

-- table DDL (with optional primary key + foreign keys)
CREATE TABLE mytable (
    id int primary key,
    name string,
    age int,
    FOREIGN KEY (age) REFERENCES ages(id)
)
DROP mytable

-- insert
INSERT INTO mytable VALUES ("abc", 5)

-- select / joins / where on PK
SELECT * FROM mytable
SELECT name FROM mytable WHERE id = 5
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id
SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id WHERE t1.name = "abc"

-- transactions
BEGIN
COMMIT
ROLLBACK
```

### Pipeline
- **Lexer** (`lexer/lexer.go`): tokenizes keywords, identifiers, strings, numbers.
- **Parser** (`parser/parser.go`): recursive descent builder for AST nodes in `parser/ast.go`.
- **Code Generator** (`code-generator/code_generator.go`): turns AST into stack-based bytecode (`executor.Instruction`) that the VM runs.

### Notes
- WHERE currently optimizes only on primary-key equality; other predicates fall back to scans.
- UPDATE/DELETE are parsed but executor support is still pending.