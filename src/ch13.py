import marimo

__generated_with = "0.19.5"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path

    import marimo as mo
    import polars as pl

    this_dir = Path(__file__).parent
    return pl, this_dir


@app.cell
def _(pl, this_dir):
    fruit_data = this_dir / "data" / "fruit.csv"
    fruit = pl.read_csv(fruit_data)
    fruit_grouped = fruit.group_by("is_round")
    fruit_grouped
    return (fruit_grouped,)


@app.cell
def _(fruit_grouped):
    fruit_grouped.len()
    return


@app.cell
def _(pl, this_dir):
    top2000_path = this_dir / "data" / "top2000-2023.xlsx"
    top2000 = pl.read_excel(top2000_path, read_options={"skip_rows": 1}).set_sorted(
        "positie"
    )
    return (top2000,)


@app.cell
def _(pl, top2000):
    (
        top2000.group_by("jaar")
        .agg(
            songs=pl.concat_str(pl.col("artiest"), pl.lit(" - "), pl.col("titel")),
        )
        .sort("jaar", descending=True)
    )
    return


@app.cell
def _(top2000):
    (
        top2000.group_by("jaar", maintain_order=True)
        .head(3)
        .sort("jaar", descending=True)
        .head(9)
    )
    return


@app.cell
def _(top2000):
    (
        top2000.group_by("jaar", maintain_order=True)
        .tail(3)
        .sort("jaar", descending=True)
        .head(9)
    )
    return


@app.cell
def _(top2000):
    top2000.group_by("artiest").len().sort("len", descending=True).head(10)
    return


@app.cell
def _(pl, this_dir):
    sales_path = this_dir / "data" / "sales.csv"
    sales = pl.read_csv(sales_path)
    sales.columns
    return (sales,)


@app.cell
def _(sales):
    (
        sales.select("Product_Category", "Sub_Category", "Unit_Price")
        .group_by("Product_Category", "Sub_Category")
        .max()
        .sort("Unit_Price", descending=True)
        .head(10)
    )
    return


@app.cell
def _(sales):
    (
        sales.select("Country", "Profit")
        .group_by("Country")
        .sum()
        .sort("Profit", descending=True)
    )
    return


@app.cell
def _(sales):
    (
        sales.select("Sub_Category", "Product")
        .group_by("Sub_Category")
        .n_unique()
        .sort("Product", descending=True)
        .head(10)
    )
    return


@app.cell
def _(sales):
    (
        sales.select("Age_Group", "Order_Quantity")
        .group_by("Age_Group")
        .mean()
        .sort("Order_Quantity", descending=True)
    )
    return


@app.cell
def _(sales):
    (
        sales.select("Age_Group", "Revenue")
        .group_by("Age_Group")
        .quantile(0.9)
        .sort("Revenue", descending=True)
    )
    return


@app.cell
def _(pl, sales):
    (
        sales.select("Country", "Profit", "Revenue")
        .group_by("Country")
        .agg(
            pl.col("Profit"),
            pl.col("Revenue"),
        )
    )
    return


@app.cell
def _(pl, sales):
    (
        sales.group_by("Country").agg(
            pl.col("Profit").alias("All Profits Per Transactions"),
            pl.col("Revenue").name.prefix("All "),
            Cost=pl.col("Revenue") - pl.col("Profit"),
        )
    )
    return


@app.cell
def _(pl, sales):
    (
        sales.select("Country", "Profit", "Revenue")
        .group_by("Country")
        .agg(
            pl.col("Profit").sum().name.prefix("Total "),
            pl.col("Profit").mean().alias("Average Profit per Transaction"),
            pl.col("Revenue").sum().name.prefix("Total "),
            pl.col("Revenue").mean().alias("Average Revenue per Transaction"),
        )
    )
    return


@app.cell
def _(pl, sales):
    (
        sales.select("Country", "Profit", "Revenue")
        .group_by("Country")
        .agg(
            pl.all().sum().name.prefix("Total "),
            pl.all().mean().name.prefix("Average "),
        )
    )
    return


@app.cell
def _(pl, sales):
    (
        sales.select("Country", "Profit")
        .group_by("Country")
        .agg(
            (pl.col("Profit") > 1000).alias("Profit > 1000"),
            (pl.col("Profit") > 1000).sum().alias("Transactions with Profit > 1000"),
        )
    )
    return


@app.cell
def _(pl, sales):
    def sam_transacations_above_threshold(
        col: pl.Expr, threshold: float
    ) -> tuple[pl.Expr, pl.Expr]:
        """Sums transactions where the column col exceeds specified threshold"""
        original_column_name = col.meta.root_names()[0]
        condition_column = (col > threshold).alias(
            f"{original_column_name} > {threshold}"
        )
        new_column = (
            (col > threshold)
            .sum()
            .alias(f"Transactions with {original_column_name} > {threshold}")
        )
        return condition_column, new_column

    sales.select("Country", "Profit").group_by("Country").agg(
        sam_transacations_above_threshold(pl.col("Profit"), 999)
    )
    return


@app.cell
def _(pl):
    fold_example = pl.DataFrame(
        {
            "col1": [2],
            "col2": [3],
            "col3": [4],
        }
    )

    fold_example.with_columns(
        sum=pl.fold(
            acc=pl.lit(0),
            function=lambda acc, x: acc + x,
            exprs=pl.col("*"),
        )
    )
    return


@app.cell
def _(pl):
    products = pl.DataFrame(
        {
            "product_A": [10, 20, 30],
            "product_B": [20, 30, 40],
            "product_C": [30, 40, 50],
        }
    )
    weights = {"product_A": 0.5, "product_B": 1.5, "product_C": 2.0}
    weighted_exprs = [
        (pl.col(product) * weight).alias(product) for product, weight in weights.items()
    ]
    products_with_weighted_sum = products.with_columns(
        weighted_sum=pl.fold(
            acc=pl.lit(0),
            function=lambda acc, x: acc + x,
            exprs=weighted_exprs,
        )
    )
    products_with_weighted_sum
    return


@app.cell
def _(pl, top2000):
    (
        top2000.select(
            "jaar",
            "artiest",
            "titel",
            "positie",
            year_rank=pl.col("positie").rank().over("jaar"),
        ).sample(10, seed=42)
    )
    return


@app.cell
def _(pl):
    dates = pl.date_range(
        start=pl.date(2024, 4, 1),
        end=pl.date(2024, 4, 26),
        interval="2d",
        eager=True,
    )
    dates = dates.filter(dates.dt.weekday() < 6)
    dates_repeated = pl.concat([dates, dates]).sort()
    small_sales_df = (
        pl.DataFrame(
            {
                "date": dates_repeated,
                "store": ["Store A", "Store B"] * dates.len(),
                "sales": [
                    200,
                    150,
                    220,
                    160,
                    250,
                    180,
                    270,
                    190,
                    280,
                    210,
                    210,
                    170,
                    220,
                    180,
                    240,
                    190,
                    250,
                    200,
                    260,
                    210,
                ],
            }
        )
        .set_sorted("date")
        .set_sorted("store")
    )
    small_sales_df
    return (small_sales_df,)


@app.cell
def _(pl, small_sales_df):
    result = small_sales_df.rolling(
        index_column="date",
        period="7d",
        group_by="store",
    ).agg(sum_of_last_y_days_sales=pl.sum("sales"))
    final_df = small_sales_df.join(result, on=["date", "store"])
    final_df
    return


@app.cell
def _(small_sales_df):
    upsampled_small_sales_df = small_sales_df.upsample(
        time_column="date",
        every="1d",
        group_by="store",
        maintain_order=True,
    )
    upsampled_small_sales_df
    return (upsampled_small_sales_df,)


@app.cell
def _(pl, upsampled_small_sales_df):
    upsampled_small_sales_df.select(
        "date", pl.col("store").forward_fill(), pl.col("sales").interpolate()
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
