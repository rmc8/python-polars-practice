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
def _():
    return


if __name__ == "__main__":
    app.run()
