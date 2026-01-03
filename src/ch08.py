import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import math
    from pathlib import Path

    import numpy as np 
    import polars as pl

    this_dir = Path(__file__).parent
    return math, np, pl, this_dir


@app.cell
def _(math, np):
    print(f"{math.pi=}")
    rng = np.random.default_rng(1729)
    print(f"{rng.random()=}")
    return (rng,)


@app.cell
def _(pl, this_dir):
    penguin_data = this_dir / "data" / "penguins.csv"
    penguins = pl.read_csv(penguin_data, null_values="NA").select(
        "species",
        "island",
        "sex",
        "year",
        mass=pl.col("body_mass_g") / 1000,
    )
    penguins.with_columns(
        mass_sqrt=pl.col("mass").sqrt(),
        mass_exp=pl.col("mass").exp(),
    )
    return (penguins,)


@app.cell
def _(penguins, pl):
    penguins.select(pl.col("mass").mean(), pl.col("island").first())
    return


@app.cell
def _(penguins, pl):
    penguins.select(pl.col("island").unique())
    return


@app.cell
def _(penguins, pl):
    penguins.select(
        pl.col("species")
        .unique()
        .repeat_by(3000)
        .explode()
        .extend_constant("Saiyan", n=1)
    )
    return


@app.cell
def _(math, pl):
    (
        pl.DataFrame({"x": [-2.0, 0.0, 0.5, 1.0, math.e, 1000.0]}).with_columns(
            abs=pl.col("x").abs(),
            exp=pl.col("x").exp(),
            log2=pl.col("x").log(2),
            log10=pl.col("x").log10(),
            log1p=pl.col("x").log1p(),
            sign=pl.col("x").sign(),
            sqrt=pl.col("x").sqrt(),
        )
    )
    return


@app.cell
def _(math, pl):
    (
        pl.DataFrame(
            {"x": [-2.0, 0.0, 0.5, 1.0, math.e, 1000.0]}
        ).with_columns(
           cum_count=pl.col("x").cum_count(),
           cum_max=pl.col("x").cum_max(),
           cum_min=pl.col("x").cum_min(),
           cum_prod=pl.col("x").cum_prod(reverse=True),
           cum_sum=pl.col("x").cum_sum(),
           diff=pl.col("x").diff(),
           pct_change=pl.col("x").pct_change(),
        )
    )
    return


@app.cell
def _(math, pl):
    (
        pl.DataFrame(
            {"x": [-1.0, 0.0, 1.0, None, None, 3.0, 4.0, math.nan, 6.0]}
        ).with_columns(
            backward_fill=pl.col("x").backward_fill(),
            foward_fill=pl.col("x").forward_fill(limit=1),
            interp1=pl.col("x").interpolate(method="linear"),
            interp2=pl.col("x").interpolate(method="nearest"),
            shift1=pl.col("x").shift(1),
            shift2=pl.col("x").shift(-2),
        )
    )
    return


@app.cell
def _(pl):
    (
        pl.DataFrame(
            {"x": ["A", "C", "D", "C"]}
        ).with_columns(
            is_duplicated=pl.col("x").is_duplicated(),
            is_first_distinct=pl.col("x").is_first_distinct(),
            is_last_distinct=pl.col("x").is_last_distinct(),
            is_unique=pl.col("x").is_unique(),
        )
    )
    return


@app.cell
def _(pl, this_dir):
    from plotnine import ggplot, aes, geom_line, labs, theme_tufte, theme

    stock_data = this_dir / "data" / "stock" / "nvda" / "2023.csv"
    stock = pl.read_csv(
        stock_data, try_parse_dates=True
        ).select(
            "date", "close"
        ).with_columns(
            ewm_mean=pl.col("close").ewm_mean(com=7, ignore_nulls=True),
            rolling_mean=pl.col("close").rolling_mean(window_size=7),
            rolling_min=pl.col("close").rolling_min(window_size=7),
        )

    p = (
        ggplot(
            stock.unpivot(index="date"),
            aes("date", "value", color="variable")
        )
        + geom_line(size=1)
        + labs(x="Date", y="Value", color="Method")
        + theme_tufte(base_family="Arial", base_size=14)
        + theme(figure_size=(8, 5), dpi=200)
    )
    p.draw()
    return


@app.cell
def _(pl):
    (
        pl.DataFrame(
            {
                "x": [1,3,None,3,7],
                "y": ["D", "I", "S", "C", "0"],
            }
        ).with_columns(
            arg_sort=pl.col("x").arg_sort(),
            shuffle=pl.col("x").shuffle(seed=7),
            sort=pl.col("x").sort(nulls_last=True),
            sort_by=pl.col("x").sort_by("y"),
            reverse=pl.col("x").reverse(),
            rank=pl.col("x").rank(),
        )
    )

    return


@app.cell
def _(pl):
    (
        pl.DataFrame(
            {"x": [33, 33, 27, 33, 60,60,60,33,60]}
        ).with_columns(
            rle_id=pl.col("x").rle_id(),
        )
    )
    return


@app.cell
def _(pl):
    df = pl.DataFrame(
        {
            "x":[1,0,1],
            "y":[1,1,1],
            "z":[0,0,0],
        }
    )
    print(df)
    print(
        df.select(
            pl.all().all().name.suffix("_all"),
            pl.all().any().name.suffix("_any"),
        )
    )
    return


@app.cell
def _(pl, rng):
    samples = rng.normal(loc=5, scale=3, size=1_000_000)
    (
        pl.DataFrame(
            {"x": samples}
        ).select(
            max=pl.col("x").max(),
            min=pl.col("x").min(),
            mean=pl.col("x").mean(),
            quantile=pl.col("x").quantile(quantile=0.95),
            skew=pl.col("x").skew(),
            std=pl.col("x").std(),
            sum=pl.col("x").sum(),
            var=pl.col("x").var(),
        )
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
 
    """)
    return


@app.cell
def _(pl, rng):
    samples2 = pl.Series(
        rng.integers(low=0, high=10_000, size=1_729)
    )
    samples2[403] = None
    df_ints = pl.DataFrame({"x": samples2}).with_row_index()
    df_ints.slice(400, 6)
    return (df_ints,)


@app.cell
def _(df_ints, pl):
    df_ints.select(
        bottom_k=pl.col("x").bottom_k(7),
        head=pl.col("x").head(7),
        sample=pl.col("x").sample(7),
        slice=pl.col("x").slice(400, 7),
        gather=pl.col("x").gather([1,1,2,3,5,8,13]),
        gather_every=pl.col("x").gather_every(247),
        top_k=pl.col("x").top_k(7),
    )
    return


@app.cell
def _(np, pl):
    x=[None, 1.0, 2.0, 3.0, np.nan]
    (
        pl.DataFrame({"x": x}).select(
            drop_nans=pl.col("x").drop_nans(),
            drop_nulls=pl.col("x").drop_nulls(),
        )
    )
    return


@app.cell
def _(pl):
    numbers=[33,33,27,33,60,60,60,33,60]
    (
        pl.DataFrame(
            {"x":numbers}
        ).select(
            arg_true=(pl.col("x") >= 60).arg_true(),
        )
    )
    return (numbers,)


@app.cell
def _(numbers, pl):
    (
        pl.DataFrame(
            {"x": numbers}
        ).select(
            mode=pl.col("x").mode().sort(),
        )
    )
    return


@app.cell
def _(numbers, pl):
    (
        pl.DataFrame(
            {"x": numbers}
        ).select(
            mode=pl.col("x").mode().sort(),
        )
    )
    return


@app.cell
def _(numbers, pl):
    (
        pl.DataFrame(
            {"x": numbers}
        ).select(
            reshape=pl.col("x").reshape((3,3))
        )
    )
    return


@app.cell
def _(numbers, pl):
    (
        pl.DataFrame(
            {"x": numbers}
        ).select(
            rle=pl.col("x").rle(),
        )
    )
    return


@app.cell
def _(numbers, pl):
    (
        pl.DataFrame(
            {"x": numbers}
        ).select(
            rle=pl.col("x").sort().search_sorted(42),
        )
    )
    return


@app.cell
def _(pl):
    (
        pl.DataFrame(
            {
                "x": [["a", "b"], ["c", "d"]]
            }
        ).select(
            explode=pl.col("x").explode(),
        )
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
