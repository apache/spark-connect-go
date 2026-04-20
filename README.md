# spark-connect-go

> A maintained fork of [`apache/spark-connect-go`](https://github.com/apache/spark-connect-go) with a `database/sql` driver, edge-typed DataFrames, exposed gRPC dial options, and a typed `ClusterNotReady` error. Tracks upstream; deltas are queued to upstream.

Spark Connect is Spark's [language-neutral gRPC protocol](https://spark.apache.org/docs/latest/spark-connect-overview.html). The upstream Go client is the official reference implementation. This fork carries the deltas needed for production usage while those patches work their way upstream — drop in by swapping the import path; the session API, DataFrame surface, and protobuf stubs are unchanged.

## What's added

- **`database/sql` driver.** `sql.Open("spark", "sc://host:port")` works with goose, sqlc-generated code, pgx-style consumers — anything that speaks `database/sql`. Registered under the name `spark` in `spark/sql/driver`. `$N` positional placeholders are rendered client-side into Spark SQL literals (the native parameter proto isn't reliable across every supported Spark version).
- **Edge-typed DataFrames.** `As[T](df) → *DataFrameOf[T]` caches a reflected row plan once; `Collect`, `Stream`, `First` materialise into struct types at the point you know the result shape. Top-level `Collect[T] / Stream[T] / First[T]` helpers do the `As[T]` plus the call in one shot.
- **`SparkSessionBuilder.WithDialOptions`.** gRPC dial options exposed on the builder — auth interceptors, TLS, observability handlers wire in without subclassing.
- **`sparkerrors.IsClusterNotReady(err)`.** Typed error for cluster cold-start states. Databricks serverless clusters take 30-90s to warm; retry logic upstack needs a reliable signal instead of string-matching on error messages.

Every delta is tracked as a PR queued for `apache/spark-connect-go`. When a delta lands upstream we drop it from the fork. Long-term goal is zero deltas.

## Install

```bash
go get github.com/datalake-go/spark-connect-go
```

Requires a Spark Connect server (Spark 3.4+).

## Quick start

```go
import (
    sparksql "github.com/datalake-go/spark-connect-go/spark/sql"
)

session, err := sparksql.NewSessionBuilder().
    Remote("sc://spark.internal:15002").
    Build(ctx)
if err != nil { /* ... */ }
defer session.Stop()

df, _ := session.Sql(ctx, "SELECT id, email FROM users WHERE tier = 'gold'")
_ = df.Show(ctx, 20, false)
```

The `sparksql` alias avoids collision with stdlib `database/sql` — the actual package name is `sql`.

### Using DataFrames

The untyped `DataFrame` is the building block — same surface as upstream. Transformations (`Where`, `Limit`, `OrderBy`, `Select`, `Join`, `GroupBy`) compose lazily and execute on the Spark side; materialisers (`Show`, `Collect`, `First`, `Count`) round-trip and return `[]types.Row`.

```go
df, _ := session.Sql(ctx, "SELECT id, email, created_at FROM users")

filtered, _ := df.Where(ctx, "tier = 'gold'")
top, _      := filtered.OrderBy(ctx, "created_at DESC").Limit(ctx, 100)

rows, _ := top.Collect(ctx)
for _, r := range rows {
    // r is types.Row — positional access by index or by name
}
```

Use this when the result shape is dynamic, or as the composition surface that you eventually re-type at the edge.

### Using Typed DataFrames

`As[T](df) → *DataFrameOf[T]` is the typed surface. It binds a result shape to a struct, caches the reflected row plan once, and materialises into `[]T` / `*T` without re-validating on every call.

```go
type User struct {
    ID      string    `spark:"id"`
    Email   string    `spark:"email"`
    Created time.Time `spark:"created_at"`
}

df, _    := session.Sql(ctx, "SELECT id, email, created_at FROM users WHERE tier = 'gold'")
typed, _ := sparksql.As[User](df)

users, _   := typed.Collect(ctx)
alice, err := typed.First(ctx)
if errors.Is(err, sparksql.ErrNotFound) { /* zero rows */ }
```

If you only need the result once, `Collect[T] / First[T] / Stream[T]` are top-level helpers that fold `As[T]` into the call:

```go
users, _ := sparksql.Collect[User](ctx, df)
```

Untagged fields map by snake_case'd field name, so plain Go structs work without tags. `spark:"-"` skips a field. `*DataFrameOf[T]` deliberately has no transformation methods — `Where` / `Limit` / `Select` / `Join` change the row shape and would make `T` lie. Compose on the untyped `DataFrame`, then re-type at the edge:

```go
typed, _    := sparksql.As[User](df)
narrower, _ := typed.DataFrame().Select(ctx, "id", "email")  // back to untyped
ids, _      := sparksql.Collect[struct{ ID string `spark:"id"` }](ctx, narrower)
```

### Streaming Results

`Stream[T]` returns a Go 1.23 [`iter.Seq2[T, error]`](https://pkg.go.dev/iter#Seq2). One of the things Go gives us over the Python / Scala clients is a real pull-based iterator — rows decode one at a time as the gRPC stream resolves them, with constant memory regardless of result size. No need to buffer the whole result, no callback API: just `range`.

```go
for row, err := range sparksql.Stream[User](ctx, df) {
    if err != nil { break }
    // use row — decoded from the next Arrow batch as it lands
}
```

Schema binding happens on the first row; if a later row's schema diverges from the first, the error surfaces through the iterator (no per-row panics).

Use `Stream[T]` when result sets are large, when you want to short-circuit early without dragging the rest of the rows over the wire, or when you're piping into another `iter.Seq2` consumer.

### `database/sql` driver

```go
import (
    "database/sql"
    _ "github.com/datalake-go/spark-connect-go/spark/sql/driver"
)

db, _   := sql.Open("spark", "sc://spark.internal:15002")
rows, _ := db.QueryContext(ctx, "SELECT id FROM users WHERE tier = $1", "gold")
```

`$N` placeholders render with type-aware quoting (strings, numbers, bools, `[]byte`, `time.Time`). `?` placeholders aren't supported — most `database/sql`-adjacent codegen (sqlc, goose dialects, pgx patterns) emits `$N`, so the narrower grammar keeps the renderer simple.

### Cluster cold-start

```go
import "github.com/datalake-go/spark-connect-go/spark/sparkerrors"

df, err := session.Sql(ctx, query)
if sparkerrors.IsClusterNotReady(err) {
    // retry with backoff — Databricks serverless usually warms in 30-90s
}
```

## Building from source

```bash
git clone https://github.com/datalake-go/spark-connect-go.git
cd spark-connect-go
make && make test
```

Regenerating protobuf stubs from the Spark submodule:

```bash
git submodule update --init --recursive
make gen && make test
```

## Contributing

Feature work that could land upstream should be proposed against [`apache/spark-connect-go`](https://github.com/apache/spark-connect-go) first. Fork-only changes (anything that wouldn't be accepted upstream) stay on this tree. See [CONTRIBUTING.md](CONTRIBUTING.md).
