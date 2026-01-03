import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path

    import marimo as mo
    import polars as pl

    this_dir = Path(__file__).parent
    penguin_data = this_dir / "data" / "penguins.csv"
    return Path, penguin_data, pl, this_dir


@app.cell
def _(penguin_data, pl):
    penguins = pl.read_csv(penguin_data)
    penguins
    return


@app.cell
def _(penguin_data, pl):
    penguins_fill_na = pl.read_csv(penguin_data, null_values="NA")
    penguins_fill_na
    return (penguins_fill_na,)


@app.cell
def _(penguins_fill_na):
    penguins_fill_na.null_count().transpose(
        include_header=True, column_names=["null_count"]
    )
    return


@app.cell
def _(this_dir):
    director_data = this_dir / "data" / "directors.csv"
    return (director_data,)


@app.cell
def _(Path, director_data):
    # ComputeError
    # pl.read_csv(director_data)

    import chardet

    def detect_encoding(filename: str | Path) -> str:
        with open(filename, "rb") as f:
            raw_data = f.read()
            result = chardet.detect(raw_data)
            return result["encoding"]

    detect_encoding(director_data)
    return


@app.cell
def _(director_data, pl):
    pl.read_csv(director_data, encoding="EUC-JP")
    return


@app.cell
def _(pl, this_dir):
    top_data = this_dir / "data" / "top2000-2023.xlsx"
    songs = pl.read_excel(top_data)
    songs
    return


@app.cell
def _(pl, this_dir):
    nvda_data = this_dir / "data" / "stock" / "nvda" / "201?.csv"
    pl.read_csv(nvda_data)
    return


@app.cell
def _(pl, this_dir):
    all_stock_data = this_dir / "data" / "stock" / "**" / "*.csv"
    all_stocks = pl.read_csv(all_stock_data)
    all_stocks
    return


@app.cell
def _():
    import calendar

    filenames = [
        f"data/stock/asml/{year}.csv"
        for year in range(1999, 2024)
        if calendar.isleap(year)
    ]

    filenames
    return (filenames,)


@app.cell
def _(filenames, pl):
    pl.concat(pl.read_csv(f) for f in filenames)
    return


@app.cell
def _(pl, this_dir):
    pokedex_data = this_dir / "data" / "pokedex.json"
    pokedex = pl.read_json(pokedex_data)
    pokedex
    return (pokedex,)


@app.cell
def _(pokedex):
    (
        pokedex.explode("pokemon")
        .unnest("pokemon")
        .select("id", "name", "type", "height", "weight")
    )
    return


@app.cell
def _(this_dir):
    from json import loads
    from pprint import pprint

    wiki_data = this_dir / "data" / "wikimedia.ndjson"

    with open(wiki_data) as f:
        pprint(loads(f.readline()))
    return (wiki_data,)


@app.cell
def _(pl, wiki_data):
    wikimedia = pl.read_ndjson(wiki_data)
    wikimedia
    return (wikimedia,)


@app.cell
def _(wikimedia):
    (
        wikimedia.rename({"id": "edit_id"})
        .unnest("meta")
        .select("timestamp", "title", "user", "comment")
    )
    return


@app.cell
def _(pl, this_dir):
    sakila_data = this_dir / "data" / "sakila.db"

    pl.read_database_uri(
        query="""
        SELECT
            f.film_id,
            f.title,
            c.name AS category,
            f.rating,
            f.length / 60.0 AS length
        FROM
            film AS f,
            film_category AS fc,
            category AS c
        WHERE
            fc.film_id = f.film_id
            AND fc.category_id = c.category_id
        LIMIT 10
        """,
        uri=f"sqlite:::{sakila_data}",
    )
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
