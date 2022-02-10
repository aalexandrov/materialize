# Test DSL Exploration Notes

Simplified version of the example from [Appendix A](#appendix-a).

```
model G1 = Model {
  box B1 = Get {
    id: GlobalId::User(0),
    unique_keys: [[0]],
    columns: [
      base(0): INT NOT NULL,
      base(1): INT NOT NULL,
    ]
  }
}
```

Corresponding token stream consisting of keywords, identifiers and literals, the latter two are denoted by the corresponding `i()`, and `l()` constructors:

```
model
i(G1)
=
Model
{
...
}
```

So for example, the `BaseColumn` variant of `BoxScalarExpr`:

```
base
(
l(0)
)
:
INT
NOT
NULL
```

This can be parsed like:

```rust
impl Parse for BaseColumn {
    fn parse(tokens: &mut TokenStream) -> ParseResult<Self> {
        tokens.expect(Keyword::BASE)?;
        tokens.expect(Keyword::L_CURVED_BRACKET)?;
        let position = parse::<int>(tokens)?;
        tokens.expect(Keyword::R_CURVED_BRACKET)?;
        tokens.expect(Keyword::COLON)?;
        let column_type parse::<Type>()?;
        Ok(BaseColumn { position, column_type })
    }
}
```

From a domain specific level

## Appendix A

```SQL
CREATE TABLE L(a INT NOT NULL, b STRING NOT NULL);
CREATE TABLE R(a INT NOT NULL, b STRING NOT NULL);
SELECT 
    x.b, y.b
FROM
    x, y
WHERE
    x.a = y.a AND
    x > 5
```

```
model G1 = Model {
  box B1 = Get {
    id: GlobalId::User(0),
    unique_keys: [[0]],
    columns: [
      base(0): INT NOT NULL,
      base(1): INT NOT NULL,
    ]
  }
  box B2 = Get {
    id: GlobalId::User(1),
    unique_keys: [[0], [1]],
    columns: [
      base(0): INT NOT NULL,
      base(1): INT NOT NULL,
    ]
  }
  box B3 = Select {
    quantifier l = Foreach(B1)
    quantifier y = Foreach(B2)
    eq [[rhs#2, lhs#0]]
    if rhs#2 = "Great DSL!"
    columns: [
      x.#1,
      y.#1,
    ]
  }
}
```