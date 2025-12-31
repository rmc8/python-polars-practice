import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import time
    from pathlib import Path
    import marimo as mo
    import polars as pl

    this_dir = Path(__file__).parent
    data_path = this_dir / "data" / "taxi" / "yellow_tripdata_*.parquet"
    return data_path, pl, time


@app.cell
def _(time):
    def run_time(fx):
        def inner(*args, **kwargs):
            start = time.time()
            ret =  fx(*args, **kwargs)
            duration = time.time() - start
            print(f"Time: {duration}")
            return ret

        return inner
    return (run_time,)


@app.cell
def _(data_path, pl, run_time):
    @run_time
    def _():
        trips = pl.read_parquet(data_path)
        sum_per_vendor = trips.select("VendorID", "total_amount", "trip_distance").group_by("VendorID").sum()
        income_per_distance_per_vendor = sum_per_vendor.select(
            "VendorID",
            income_per_distance=pl.col("total_amount") / pl.col("trip_distance"),
        )
        top_three = income_per_distance_per_vendor.sort(
            by="income_per_distance", descending=True,
        ).head(3)
        print(top_three)

    _()
    return


@app.cell
def _(data_path, pl, run_time):
    @run_time
    def _():
        trips = pl.scan_parquet(data_path)
        sum_per_vendor = trips.select("VendorID", "total_amount", "trip_distance").group_by("VendorID").sum()
        income_per_distance_per_vendor = sum_per_vendor.select(
            "VendorID",
            income_per_distance=pl.col("total_amount") /pl.col("trip_distance"),
        )
        top_three = income_per_distance_per_vendor.sort(
            by="income_per_distance", descending=True,
        ).head(3)
        print(top_three.collect())

    _()
    return


if __name__ == "__main__":
    app.run()
