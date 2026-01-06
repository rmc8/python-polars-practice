import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path

    import marimo as mo
    import polars as pl

    this_dir = Path(__file__).parent
    fruit_data = this_dir / "data" / "fruit.csv"
    return fruit_data, pl


@app.cell
def _(fruit_data, pl):
    fruit = pl.read_csv(fruit_data)
    fruit.filter(pl.col("is_round") & (pl.col("weight") > 1000))
    return (fruit,)


@app.cell
def _(pl):
    (
        pl.DataFrame(
            {
                "i": [6.0, 0, 2, 2.5],
                "j": [7.0, 1, 2, 3],
            }
        ).with_columns(
            (pl.col("i") * pl.col("j")).alias("*"),
            pl.col("i").mul(pl.col("j")).alias("Expr.mul()"),
        )
    )
    return


@app.cell
def _(fruit, pl):
    fruit.select(pl.col("name"), (pl.col("weight") / 1000))
    return


@app.cell
def _(pl):
    (
        pl.DataFrame(
            {
                "i": [0.0, 2, 2, -2, -2],
                "j": [1, 2, 3, 4, -5],
            }
        ).with_columns(
            (pl.col("i") + pl.col("j")).alias("i + j"),
            (pl.col("i") - pl.col("j")).alias("i - j"),
            (pl.col("i") * pl.col("j")).alias("i * j"),
            (pl.col("i") / pl.col("j")).alias("i / j"),
            (pl.col("i") // pl.col("j")).alias("i // j"),
            (pl.col("i") ** pl.col("j")).alias("i ** j"),
            (pl.col("j") % 2).alias("j % 2"),
            pl.col("i").dot(pl.col("j")).alias("i . j"),
        )
    )
    return


@app.cell
def _(pl):
    pl.select(pl.lit("a") > pl.lit("b"))
    return


@app.cell
def _(fruit, pl):
    (
        fruit.select(
            pl.col("name"),
            pl.col("weight"),
        ).filter(
            pl.col("weight") >= 1000,
        )
    )
    return


@app.cell
def _(pl):
    (
        pl.DataFrame(
            {
                "a": [-273.15, 0, 42, 100],
                "b": [1.4142, 2.7183, 42, 3.1415],
            }
        ).with_columns(
            (pl.col("a") == pl.col("b")).alias("a == b"),
            (pl.col("a") <= pl.col("b")).alias("a <= b"),
            (pl.all() > 0).name.suffix(" > 0"),
            ((pl.col("b") - pl.lit(2).sqrt()).abs() < 1e-3).alias("b = sqrt(2)"),
            ((1 < pl.col("b")) & (pl.col("b") < 3)).alias("1 < b < 3"),
        )
    )
    return


@app.cell
def _(pl):
    pl.select(
        bool_num=pl.lit(True) > 0,
        time_time=pl.time(23, 58) > pl.time(0, 0),
        datetime_date=pl.datetime(1969, 7, 21, 2, 56) < pl.date(1976, 7, 20),
        str_num=pl.lit("5") < pl.lit(3).cast(pl.String),
        datetime_time=pl.datetime(1999, 1, 1).dt.time() != pl.time(0, 0),
    ).transpose(
        include_header=True,
        header_name="comparison",
        column_names=["allowed"],
    )
    return


@app.cell
def _(pl):
    x = 7
    p = pl.lit(3) < pl.lit(x)
    q = pl.lit(x) < pl.lit(5)
    pl.select(p & q).item()
    return


@app.cell
def _(pl):
    (
        pl.DataFrame(
            {
                "p": [True, True, False, False],
                "q": [True, False, True, False],
            }
        ).with_columns(
            (pl.col("p") & pl.col("q")).alias("p & q"),
            (pl.col("p") | pl.col("q")).alias("p | q"),
            (~pl.col("p")).alias("~p"),
            (pl.col("p") ^ pl.col("q")).alias("p ^ q"),
            (~(pl.col("p") & pl.col("q")).alias("p ↑ q")),
            ((pl.col("p").or_(pl.col("q"))).not_()).alias("p ↓ q"),
        )
    )
    return


@app.cell
def _(pl):
    pl.select(pl.lit(10) | pl.lit(34)).item()
    return


@app.cell
def _(pl):
    bits = pl.DataFrame(
        {
            "x": [1, 1, 0, 0, 7, 10],
            "y": [1, 0, 1, 0, 2, 34],
        },
        schema={"x": pl.UInt8, "y": pl.UInt8},
    ).with_columns(
        (pl.col("x") & pl.col("y")).alias("x & y"),
        (pl.col("x") | pl.col("y")).alias("x | y"),
        (~pl.col("x")).alias("~x"),
        (pl.col("x") ^ pl.col("y")).alias("x ^ y"),
    )
    bits
    return (bits,)


@app.cell
def _(bits, pl):
    bits.select(pl.all().map_elements("{0:08b}".format, return_dtype=pl.String))
    return


@app.cell
def _(pl):
    scientists = pl.DataFrame(
        {
            "first_name": ["George", "Grace", "John", "Kurt", "Ada"],
            "last_name": ["Boole", "Hopper", "Tukey", "Godel", "Lovelace"],
            "country": [
                "England",
                "United States",
                "United States",
                "Austria-Huangary",
                "England",
            ],
        }
    )
    scientists

    return (scientists,)


@app.cell
def _(pl, scientists):
    scientists.select(
        concat_list=pl.concat_list(pl.col("^*_name$")),
        struct=pl.struct(pl.all()),
    )
    return


@app.cell
def _(pl, scientists):
    scientists.select(
        concat_str=pl.concat_str(pl.all(), separator=" "),
        format=pl.format("{}, {} from {}", "last_name", "first_name", "country"),
    )
    return


@app.cell
def _(pl):
    prefs = pl.DataFrame(
        {
            "id": [1, 7, 42, 101, 999],
            "has_pet": [True, False, True, False, True],
            "likes_travel": [False, False, False, False, True],
            "lives_movies": [True, False, True, False, True],
            "likes_books": [False, False, True, True, True],
        }
    ).with_columns(
        all=pl.all_horizontal(pl.exclude("id")),
        any=pl.any_horizontal(pl.exclude("id")),
    )
    prefs
    return (prefs,)


@app.cell
def _(pl, prefs):
    prefs.select(
        sum=pl.sum_horizontal(pl.all()),
        max=pl.max_horizontal(pl.all()),
        min=pl.min_horizontal(pl.all()),
    )
    return


@app.cell
def _(pl, prefs):
    prefs.select(
        pl.col("id"),
        likes_what=pl.when(pl.all_horizontal(pl.col("^likes_.*$")))
        .then(pl.lit("Likes everything"))
        .when(pl.any_horizontal(pl.col("^likes_.*$")))
        .then(pl.lit("Likes something"))
        .otherwise(pl.lit("Likes nothing")),
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
