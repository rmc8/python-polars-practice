import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import io
    import zipfile
    from pathlib import Path

    import pyarrow
    import httpx
    import marimo as mo
    import polars as pl
    from plotnine import (
        ggplot,
        geom_polygon,
        geom_point,
        scale_x_continuous,
        scale_y_continuous,
        scale_alpha_ordinal,
        scale_fill_brewer,
        guides,
        labs,
        aes,
        element_rect,
        element_text,
        theme_void,
        theme,
    )

    this_dir = Path(__file__).parent
    return (
        aes,
        element_rect,
        element_text,
        geom_point,
        geom_polygon,
        ggplot,
        guides,
        httpx,
        io,
        labs,
        mo,
        pl,
        scale_alpha_ordinal,
        scale_fill_brewer,
        scale_x_continuous,
        scale_y_continuous,
        theme,
        theme_void,
        this_dir,
        zipfile,
    )


@app.cell
def _(httpx, io, this_dir, zipfile):
    def download_zip_and_extruct(url: str, output_dir_name: str) -> None:
        r = httpx.get(url)
        r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))
        output_dir = this_dir / output_dir_name
        output_dir.mkdir(exist_ok=True)
        z.extractall(output_dir)

    data_url = "https://s3.amazonaws.com/tripdata/202403-citibike-tripdata.zip"
    # download_zip_and_extruct(data_url, "data")
    return


@app.cell
def _(pl, this_dir):
    trips = pl.read_csv(
        str(this_dir / "data" / "202403-citibike-tripdata_*.csv"),
        try_parse_dates=True,
        schema_overrides={
            "start_station_id": pl.String,
            "end_station_id": pl.String,
        },
    ).sort("started_at")
    trips.write_csv(this_dir / "data" / "202403-citibike-tripdata.csv")
    trips.height
    return (trips,)


@app.cell
def _(trips):
    print(trips[:, :4])
    return


@app.cell
def _(trips):
    print(trips[:, 4:8])
    return


@app.cell
def _(trips):
    print(trips[:, 8:])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## GeoJSONから地区情報を読み込む
    """)
    return


@app.cell
def _(pl, this_dir):
    geo_path = this_dir / "data" / "nyc-neighborhoods.geojson"
    neighborhoods = (
        pl.read_json(geo_path)
        .select("features")
        .explode("features")
        .unnest("features")
        .unnest("properties")
        .select("neighborhood", "borough", "geometry")
        .unnest("geometry")
        .with_columns(polygon=pl.col("coordinates").list.first())
        .select("neighborhood", "borough", "polygon")
        .filter(pl.col("borough") != "Staten Island")
        .sort("neighborhood")
    )
    neighborhoods
    return (neighborhoods,)


@app.cell
def _(neighborhoods, pl):
    neighborhoods_coords = (
        neighborhoods.with_row_index("id")
        .explode("polygon")
        .with_columns(
            lon=pl.col("polygon").list.first(),
            lat=pl.col("polygon").list.last(),
        )
        .drop("polygon")
    )
    neighborhoods_coords
    return (neighborhoods_coords,)


@app.cell
def _(pl, trips):
    stations = (
        trips.group_by(station=pl.col("start_station_name"))
        .agg(
            lon=pl.col("start_lng").median(),
            lat=pl.col("start_lat").median(),
        )
        .sort("station")
        .drop_nulls()
    )
    stations
    return (stations,)


@app.cell
def _(
    aes,
    element_rect,
    element_text,
    geom_point,
    geom_polygon,
    ggplot,
    guides,
    labs,
    neighborhoods_coords,
    scale_alpha_ordinal,
    scale_fill_brewer,
    scale_x_continuous,
    scale_y_continuous,
    stations,
    theme,
    theme_void,
):
    r = (
        ggplot(neighborhoods_coords, aes(x="lon", y="lat", group="id"))
        + geom_polygon(aes(alpha="neighborhood", fill="borough"), color="white")
        + geom_point(stations, size=0.1)
        + scale_x_continuous(expand=(0,0))
        + scale_y_continuous(expand=(0,0,0,0.01))
        + scale_alpha_ordinal(range=(0.3, 1))
        + scale_fill_brewer(type="qual", palette=2)
        + guides(alpha=False)
        + labs(
            title="New York City neighborhoods and Citi Bike stations",
            subtitle="2,143 stations across 106 neighborhoods",
            caption="Source: https://citibikenyc.com/system-data",
            fill="Borough",
        )
        + theme_void(base_family="Arial", base_size=14)
        + theme(
            dpi=300,
            figure_size=(7,9),
            plot_background=element_rect(fill="white", color="white"),
            plot_caption=element_text(style="italic"),
            plot_margin=0.01,
            plot_title=element_text(ha="left"),
        )
    )
    r
    return


@app.cell
def _(pl, trips):
    trips2 = trips.select(
        bike_type=pl.col("rideable_type")
        .str.split("_")
        .list.get(0)
        .cast(pl.Categorical),
        rider_type=pl.col("member_casual").cast(pl.Categorical),
        datetime_start=pl.col("started_at").str.to_datetime(),
        datetime_end=pl.col("ended_at").str.to_datetime(),
        station_start=pl.col("start_station_name"),
        station_end=pl.col("end_station_name"),
        lon_start=pl.col("start_lng"),
        lat_start=pl.col("start_lat"),
        lon_end=pl.col("end_lng"),
        lat_end=pl.col("end_lat"),
    ).with_columns(
        duration=(pl.col("datetime_end") - pl.col("datetime_start"))
    )
    trips2.columns
    return (trips2,)


@app.cell
def _(pl, trips2):
    trips3 = (
        trips2.drop_nulls()
        .filter(
            (pl.col("datetime_start") >= pl.date(2024, 3, 1))
            & (pl.col("datetime_end") < pl.date(2024, 4, 1))
        )
        .filter(
            ~(
                (pl.col("station_start") == pl.col("station_end"))
                & (pl.col("duration").dt.total_seconds() < 5 * 60)
            )
        )
    )
    trips3.height
    return


@app.cell
def _():
    # import polars_ds

    # trips4 = trips3.with_columns(
    #     distance=pl.concat_list("lon_start", "lat_start").geo.haversine_distance(
    #         pl.concat_list("lon_end", "lat_end")) /  1000
    # )

    # trips4.select(
    #     "lon_start",
    #     "lon_end",
    #     "lat_start",
    #     "lat_end",
    #     "distance",
    #     "duration",
    # )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
