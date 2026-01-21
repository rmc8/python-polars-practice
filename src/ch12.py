import marimo

__generated_with = "0.19.4"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path

    import marimo as mo
    import polars as pl

    this_dir = Path(__file__).parent
    return (pl,)


@app.cell
def _(pl):
    corpus_ = pl.DataFrame(
        {
            "raw_text": [
                "    Data Science is amaging.   ",
                "Data_analysis > Data entry",
                "    Python&Polars; Fast",
            ]
        }
    )
    corpus_
    return (corpus_,)


@app.cell
def _(corpus_, pl):
    corpus = corpus_.with_columns(
        processed_text=pl.col("raw_text")
        .str.strip_chars()
        .str.to_lowercase()
        .str.replace_all("_", " ")
    )
    corpus
    return (corpus,)


@app.cell
def _(corpus, pl):
    corpus.with_columns(
        len_chars=pl.col("processed_text").str.len_chars(),
        len_bytes=pl.col("processed_text").str.len_bytes(),
        count_a=pl.col("processed_text").str.count_matches("a"),
    )
    return


@app.cell
def _(pl):
    posts = pl.DataFrame(
        {"post": ["Loving #python and #polars!", "A boomer post without a hastag"]}
    )
    hashtag_regex = r"#(\w+)"
    posts.with_columns(hashtags=pl.col("post").str.extract_all(hashtag_regex))
    return


@app.cell
def _(pl):
    bear_enum_dtype = pl.Enum(["Polar", "Panda", "Brown"])
    bear_enum_series = pl.Series(
        ["Polar", "Panda", "Brown", "Brown", "Polar"], dtype=bear_enum_dtype
    )
    bear_cat_series = pl.Series(
        ["Polar", "Panda", "Brown", "Brown", "Polar"],
        dtype=pl.Categorical,
    )
    print(bear_enum_series)
    print(bear_cat_series)
    return


@app.cell
def _(pl):
    pl.DataFrame(
        {
            "utc_mixed_offset": [
                "2021-03-27T00:00:00+0100",
                "2021-03-28T00:00:00+0100",
                "2021-03-29T00:00:00+0200",
                "2021-03-30T00:00:00+0200",
            ]
        }
    ).with_columns(
        parsed=pl.col("utc_mixed_offset").str.to_datetime("%Y-%m-%dT%H:%M:%S%z")
    ).with_columns(converted=pl.col("parsed").dt.convert_time_zone("Europe/Amsterdam"))
    return


@app.cell
def _(pl):
    bools = pl.DataFrame({"values": [[True, True], [False, False, True], [False]]})
    bools.with_columns(
        all_true=pl.col("values").list.all(),
        any_true=pl.col("values").list.any(),
    )
    return


@app.cell
def _(pl):
    groups = pl.DataFrame({"ages": [[18, 21], [30, 40, 50], [42, 69]]})
    groups.with_columns(
        over_forty=pl.col("ages").list.eval(
            pl.element() > 40,
            parallel=True,
        )
    ).with_columns(all_over_forty=pl.col("over_forty").list.all())
    return (groups,)


@app.cell
def _(groups, pl):
    groups.with_columns(
        ages_sorted_descending=pl.col("ages").list.sort(descending=True)
    )
    return


@app.cell
def _(groups):
    groups.explode("ages")
    return


@app.cell
def _(groups, pl):
    groups.select(ages=pl.col("ages").list.explode())
    return


@app.cell
def _(pl):
    events = pl.DataFrame(
        [
            pl.Series("location", ["Paris", "Amsterdam", "Barcelona"], dtype=pl.String),
            pl.Series(
                "temparatures",
                [
                    [23, 27, 21, 22, 24, 23, 22],
                    [17, 19, 15, 22, 18, 20, 21],
                    [30, 32, 28, 29, 34, 33, 31],
                ],
                dtype=pl.Array(pl.Int64, shape=7),
            ),
        ]
    )
    events
    return (events,)


@app.cell
def _(events, pl):
    events.with_columns(
        median=pl.col("temparatures").arr.median(),
        max=pl.col("temparatures").arr.max(),
        warmest_dow=pl.col("temparatures").arr.arg_max(),
    )
    return


@app.cell
def _(pl):
    from datetime import date

    orders = pl.DataFrame(
        {
            "customer_id": [2781, 6139, 5392],
            "order_details": [
                {"amount": 250.00, "date": date(2024, 1, 3), "items":5},
                {"amount": 150.00, "date": date(2024, 1, 5), "items": 1},
                {"amount": 100.00, "date": date(2024, 1, 2), "items": 3},
            ]
        }
    )
    orders
    return (orders,)


@app.cell
def _(orders, pl):
    orders.select(pl.col("order_details").struct.field("amount"))
    return


@app.cell
def _(orders):
    order_details_df = orders.unnest("order_details")
    order_details_df
    return (order_details_df,)


@app.cell
def _(order_details_df, pl):
    order_details_df.select(
        "amount",
        "date",
        "items",
        order_details=pl.struct(pl.col("amount"), pl.col("date"), pl.col("items")),
    )
    return


@app.cell
def _(pl):
    basket = pl.DataFrame(
        {
            "fruit": ["cherry", "apple", "banana", "banana", "apple", "banana"],
        }
    )
    basket
    return (basket,)


@app.cell
def _(basket, pl):
    basket.select(pl.col("fruit").value_counts(sort=True))
    return


@app.cell
def _(basket, pl):
    basket.select(pl.col("fruit").value_counts(sort=True).struct.unnest())
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
