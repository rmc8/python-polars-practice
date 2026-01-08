import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path

    import marimo as mo
    import polars as pl

    this_dir = Path(__file__).parent
    data_path = this_dir / "data" / "tools.csv"
    return data_path, pl


@app.cell
def _(data_path, pl):
    tools = pl.read_csv(data_path)
    tools
    return (tools,)


@app.cell
def _(pl, tools):
    tools.filter(pl.col("cordless") & (pl.col("brand") == "Makita"))
    return


@app.cell
def _(pl, tools):
    tools.filter(pl.col("cordless"), pl.col("brand") == "Makita")
    return


@app.cell
def _(tools):
    tools.filter("cordless")
    return


@app.cell
def _(tools):
    tools.filter(cordless=True, brand="Makita")
    return


@app.cell
def _(tools):
    tools.sort("price")
    return


@app.cell
def _(tools):
    tools.sort("price", descending=True)
    return


@app.cell
def _(tools):
    tools.sort("brand", "price", descending=[False, True])
    return


@app.cell
def _(pl, tools):
    tools.sort(pl.col("rpm") / pl.col("price"))
    return


@app.cell
def _(pl):
    lists = pl.DataFrame(
        {"lists": [[2,2], [2,1,3], [1]]},
    )
    lists.sort("lists")
    return


@app.cell
def _(pl):
    strucsts = pl.DataFrame(
        {
            "strucsts": [
                {"a": 1, "b": 2, "c":3},
                {"a": 1, "b": 3, "c":1},
                {"a": 1, "b": 1, "c": 2},
            ]
        }
    )
    strucsts.sort("strucsts")
    return


@app.cell
def _(pl, tools):
    tools_collection = tools.group_by("brand").agg(collection=pl.struct(pl.all()))
    tools_collection
    return (tools_collection,)


@app.cell
def _(pl, tools_collection):
    tools_collection.sort(pl.col("collection").list.len(), descending=True)
    return


@app.cell
def _(pl, tools_collection):
    tools_collection.sort(
        pl.col("collection")
        .list.eval(
            pl.element().struct.field("price")
                  ).list.mean()
    )
    return


@app.cell
def _(tools):
    tools.drop_nulls("rpm").height
    return


@app.cell
def _(pl, tools):
    tools.filter(pl.all_horizontal(pl.all().is_not_null())).height
    return


@app.cell
def _(tools):
    tools.with_row_index().gather_every(2).head()
    return


@app.cell
def _(tools):
    tools.top_k(3, by="price")
    return


@app.cell
def _(tools):
    tools.sample(fraction=0.2)
    return


@app.cell
def _(pl, tools):
    saws = pl.DataFrame(
        {
            "tool": [
                "Table Saw",
                "Plunge Cut Saw",
                "Miter Saw",
                "Jigsaw",
                "Bandsaw",
                "Chainsaw",
                "Seesaw",
            ]
        }
    )
    tools.join(saws, how="semi", on="tool")
    return


if __name__ == "__main__":
    app.run()
