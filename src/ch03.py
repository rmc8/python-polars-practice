import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path
    import marimo as mo
    import pandas as pd
    import polars as pl

    this_dir = Path(__file__).parent
    animals_data = this_dir / "data" / "animals.csv"
    return animals_data, pd, pl


@app.cell
def _(animals_data, pd):
    animals_pd = pd.read_csv(animals_data, sep=",", header=0)
    return (animals_pd,)


@app.cell
def _(animals_data, pl):
    animals_pl = pl.read_csv(animals_data, separator=",", has_header=True)
    return (animals_pl,)


@app.cell
def _(animals_pd, animals_pl):
    print(f"{type(animals_pd) = }")
    print(f"{type(animals_pl) = }")
    return


@app.cell
def _(animals_pd):
    animals_pd
    return


@app.cell
def _(animals_pl):
    animals_pl
    return


@app.cell
def _(animals_pd):
    animals_pd["animal"]
    return


@app.cell
def _(animals_pl):
    animals_pl.get_column("animal")
    return


@app.cell
def _(animals_pd):
    animals_pd_droped = animals_pd.drop(columns=["habitat", "diet", "features"])
    animals_pd_droped
    return


@app.cell
def _(animals_pl):
    animals_pl_dropped = animals_pl.drop(["habitat", "diet", "features"])
    animals_pl_dropped
    return


@app.cell
def _(animals_data, pl):
    lazy_query = (
        pl.scan_csv(animals_data)
        .group_by("class")
        .agg(pl.col("weight").mean())
        .filter(pl.col("class") == "mammal")
    )
    lazy_query.show_graph(optimized=False)

    return (lazy_query,)


@app.cell
def _(lazy_query):
    lazy_query.show_graph()
    return


@app.cell
def _(lazy_query):
    lazy_query.collect()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
