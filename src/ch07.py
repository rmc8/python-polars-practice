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
    fruit
    return (fruit,)


app._unparsable_cell(
    r"""

        fruit.select(
            pl.col(\\"name\\"), # 一般的な方法
            pl.col(\\"^.*or.*$\\"), # 正規表現での抽出
            pl.col(\\"weight\\") / 1000, # 列に対しての算術演算
            \\"is_round\\",　# 文字列形式での列の参照（↑のような計算は不可）
        )
    
    """,
    name="_",
)


@app.cell
def _(fruit, pl):
    # 新しい列をつくる
    fruit.with_columns(
        pl.lit(True).alias("is_fruit"),
        is_berry=pl.col("name").str.ends_with("berry"),
    )
    return


@app.cell
def _(fruit, pl):
    fruit.filter((pl.col("weight") > 1000) & pl.col("is_round"))
    return


@app.cell
def _(fruit, pl):
    fruit.group_by(pl.col("origin").str.split(" ").list.last()).agg(
        average_weight=pl.col("weight").mean()
    )
    return


@app.cell
def _(fruit, pl):
    fruit.sort(
        pl.col("name").str.len_bytes(),
        descending=True,
    )
    return


@app.cell
def _(fruit, pl):
    fruit.select(pl.col(["name", "color"])).columns
    return


@app.cell
def _(pl):
    pl.select(pl.lit(42))
    return


@app.cell
def _(pl):
    pl.select(pl.lit(42).alias("answer"))
    return


@app.cell
def _(fruit, pl):
    fruit.with_columns(planet=pl.lit("Earth"))
    return


@app.cell
def _(fruit, pl):
    fruit.with_columns(row_is_even=pl.lit([False, True]))
    return


@app.cell
def _(pl):
    pl.select(
        start=pl.int_range(0, 5),
        end=pl.arange(0, 10, 2).pow(2),
    ).with_columns(int_range=pl.int_ranges("start", "end")).with_columns(
        range_length=pl.col("int_range").list.len()
    )
    return


@app.cell
def _(pl):
    df = pl.DataFrame(
        {
            "text": "value",
            "An integer": 5040,
            "BOOLEAN": True,
        }
    )
    df
    return (df,)


@app.cell
def _(df, pl):
    df.select(
        pl.col("text").name.to_uppercase(),
        pl.col("An integer").alias("int"),
        pl.col("BOOLEAN").name.to_lowercase(),
    )
    return


@app.cell
def _(df, pl):
    df.select(pl.all().name.map(lambda s: s.lower().replace(" ", "_")))
    return


@app.cell
def _(fruit):
    fruit.filter((fruit["weight"] > 1000) & fruit["is_round"])
    return


@app.cell
def _(fruit, pl):
    (
        fruit.lazy()
        .filter((pl.col("weight") > 1000) & pl.col("is_round"))
        .with_columns(is_berry=pl.col("name").str.ends_with("berry"))
        .collect()
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
