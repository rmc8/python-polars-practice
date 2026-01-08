import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path

    import marimo as mo
    import polars as pl
    import polars.selectors as cs

    this_dir = Path(__file__).parent
    data_path = this_dir / "data" / "starwars.parquet"
    return cs, data_path, pl


@app.cell
def _(data_path, pl):
    starwars = pl.read_parquet(data_path)
    rebels = starwars.drop("films").filter(
        pl.col("name").is_in(["Luke Skywalker", "Leia Organa", "Han Solo"])
    )
    print(rebels[:, :6])
    print(rebels[:, 6:11])
    print(rebels[:, 11:])
    return rebels, starwars


@app.cell
def _(pl, rebels):
    rebels.select(
        "name",
        pl.col("homeworld"),
        pl.col("^.*_color$"),
        (pl.col("height") / 100).alias("height_m"),
    )
    return


@app.cell
def _(cs, rebels):
    rebels.select(
        "name",
        cs.by_name("homeworld"),
        cs.by_name("^.*_color$"),
        (cs.by_name("height") / 100).alias("height_m")
    )
    return


@app.cell
def _(cs, rebels):
    rebels.select(
        cs.starts_with("birth_")
    )
    return


@app.cell
def _(cs, rebels):
    rebels.select(
        cs.ends_with("_color"),
    )
    return


@app.cell
def _(cs, rebels):
    rebels.select(
        cs.contains("_")
    )
    return


@app.cell
def _(cs, rebels):
    rebels.select(
        cs.matches("^[a-z]{4}$")
    )
    return


@app.cell
def _(cs, rebels):
    rebels.group_by("hair_color").agg(cs.numeric().mean())
    return


@app.cell
def _(cs, rebels):
    rebels.select(cs.string())
    return


@app.cell
def _(cs, rebels):
    rebels.select(cs.temporal())
    return


@app.cell
def _(cs, pl, rebels):
    rebels.select(cs.by_dtype(pl.List(pl.String)))
    return


@app.cell
def _(cs, rebels):
    rebels.select(cs.by_index(range(0, len(rebels.columns),3)))
    return


@app.cell
def _(cs, rebels):
    rebels.select("name", cs.by_index(range(-2, 0)))
    return


@app.cell
def _():
    # rebels.select(cs.by_index(20)) # Error
    return


@app.cell
def _(cs, rebels):
    rebels.select(cs.by_name("hair_color") | cs.numeric())
    return


@app.cell
def _(cs, pl):
    df = pl.DataFrame({"d": 1, "i": True, "s": True, "c":True, "o": 1.0})
    print(df)
    x = cs.by_name("d", "i", "s")
    y = cs.boolean()

    print("\nselector => columns")
    for s in ["x", "y", "x | y", "x & y", "x - y", "x ^ y", "~x", "x -x"]:
        print(f"{s:8} => {cs.expand_selector(df, eval(s))}")
    return df, x


@app.cell
def _(df, x):
    df.select(x - x)
    return


@app.cell
def _(pl, rebels):
    rebels.with_columns(bmi=pl.col("mass") / ((pl.col("height")  / 100 ) ** 2))[:,-3:]
    return


@app.cell
def _(pl, rebels):
    rebels.with_columns(
        bmi=pl.col("mass") / ((pl.col("height") /100) ** 2),
        age_destory=(
            (pl.date(1983, 5, 25) - pl.col("birth_date")).dt.total_days() / 365
        ).cast(pl.UInt8)
    )[:,-6:]
    return


@app.cell
def _(pl, rebels):
    # rebels.with_columns(
    #     bmi=pl.col("mass") /((pl.col("height") / 100) ** 2),
    #     bmi_cut=pl.col("bmi").cut(
    #         [18.5, 25], labels=["Underweight", "Normal", "Overweight"]
    #     ),
    # )
    rebels.with_columns(
        bmi=pl.col("mass") /((pl.col("height") / 100) ** 2),
    ).with_columns(
        bmi_cut=pl.col("bmi").cut(
            [18.5, 25], labels=["Underweight", "Normal", "Overweight"]
        ),
    )
    return


@app.cell
def _(pl, starwars):
    # starwars.select(
    #     "name",
    #     bmi=(pl.col("mass") / ((pl.col("height") / 100) ** 2)),
    #     "spaces"
    # )
    (
        starwars
        .select(
            "name",
            (pl.col("mass") / ((pl.col("height") / 100) ** 2)).alias("bmi"),
            "species",
        )
        .drop_nulls()
        .top_k(5, by="bmi")
    )
    return


@app.cell
def _(rebels):
    rebels.drop(
    
        "name",
        "films",
        "screen_time",
        strict=False,
    )

    return


@app.cell
def _(cs, rebels):
    rebels.select(
        cs.exclude("name", "films", "screen_time")
    )
    return


@app.cell
def _(rebels):
    (
        rebels
        .rename(
            {"homeworld": "planet", "mass": "rename"}
        ).rename(
            lambda s: s.removesuffix("_color")
        ).select(
            "name",
            "planet",
            "hair",
            "skin",
            "eye",
        )
    )
    return


@app.cell
def _(cs, pl, rebels):
    rebel_names = rebels.select("name")
    rebel_colors = rebels.select(cs.ends_with("_color"))
    rebel_quotes = pl.Series(
        "quote",
        [
            "You know, sometimes I amaze myself.",
            "That does't sound too hard.",
            "I have a bad feeling about this.",
        ]
    )
    (rebel_names.hstack(rebel_colors).hstack([rebel_quotes]))


    return


@app.cell
def _(rebels):
    rebels.with_row_index(name="rebel_id", offset=1)
    return


if __name__ == "__main__":
    app.run()
