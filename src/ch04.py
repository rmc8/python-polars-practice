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
def _(pl):
    sales_series = pl.Series([150.00, 300.00, 250.00])
    sales_series

    return (sales_series,)


@app.cell
def _(pl, sales_series):
    sales_df = pl.DataFrame({"sales": sales_series, "customer_id": [24, 25, 26]})
    sales_df
    return


@app.cell
def _(fruit_data, pl):
    lazy_df = pl.scan_csv(fruit_data).with_columns(is_heavy=pl.col("weight") > 200)
    lazy_df.show_graph()
    return


@app.cell
def _(pl):
    coordinates = pl.DataFrame(
        [
            pl.Series("point_2d", [[1, 3], [2, 5]]),
            pl.Series("point_3d", [[1, 7, 3], [8, 1, 0]]),
        ],
        schema={
            "piont_2d": pl.Array(shape=2, inner=pl.Int64),
            "point_3d": pl.Array(shape=3, inner=pl.Int64),
        },
    )
    coordinates
    return


@app.cell
def _(pl):
    weather_readings = pl.DataFrame(
        {
            "temperature": [[72.5, 75.0, 77.3], [68.0, 70.2]],
            "wind_speed": [[15, 20], [10, 12, 14, 16]],
        }
    )
    weather_readings
    return


@app.cell
def _(pl):
    rating_series = pl.Series(
        "ratings",
        [
            {"Movie": "Cars", "Theatre": "NE", "Avg_Rating": 4.5},
            {"Movie": "Toy Story", "Theatre": "ME", "Avg_Rating": 4.9},
        ],
    )
    rating_series
    return


@app.cell
def _(pl):
    missing_df = pl.DataFrame(
        {
            "value": [None, 2, 3, 4, None, None, 7, 8, 9, None],
        }
    )
    missing_df
    return (missing_df,)


@app.cell
def _(missing_df, pl):
    missing_df.with_columns(filled_with_signale=pl.col("value").fill_null(-1))
    # NaNはゼロ除算や無限大を利用した計算など、数学的に定義できない演算の結果を表すのに使う
    return


@app.cell
def _(missing_df, pl):
    missing_df.with_columns(
        forward=pl.col("value").fill_null(strategy="forward"),
        backward=pl.col("value").fill_null(strategy="backward"),
        min=pl.col("value").fill_null(strategy="min"),
        max=pl.col("value").fill_null(strategy="max"),
        mean=pl.col("value").fill_null(strategy="mean"),
        zero=pl.col("value").fill_null(strategy="zero"),
        one=pl.col("value").fill_null(strategy="one"),
    )
    return


@app.cell
def _(pl):
    string_df = pl.DataFrame({"id": ["10000", "20000", "30000"]})
    string_df
    return (string_df,)


@app.cell
def _(pl, string_df):
    int_df = string_df.select(pl.col("id").cast(pl.UInt16))
    int_df
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
