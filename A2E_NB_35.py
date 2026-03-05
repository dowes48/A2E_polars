import os                                # basic file handling functions
import polars as pl                      # polars, a DataFrame library for manipulating structured data
from datetime import timedelta           # a single datatype needed for calculations
import US_MortalityTable_03 as USMort
import time

start_time = time.time()  # Record the start time

itr = '35'

# the working directory
os.chdir(r"C:\Users\dowes\OneDrive\Projects\A2E_paper")

# read the data file into a polars dataframe ('pl' is shorthand for polars)
df = pl.read_csv(r'data/HRA_data_02.csv', try_parse_dates = True)
## df = df[20069:20070]
# define Enumeration datatypes
enum_vs   = pl.Enum(["alive", "dead"])
enum_sex  = pl.Enum(["M", "F"])
enum_dx   = pl.Enum(["healthy", "cancer", "cardiac", "diabetes", "neuro", "pulm", "renal", "other"])
enum_site = pl.Enum(["AL", "AZ", "CA", "FL", "GA", "LA", "SC", "TX", "VA"])
enum_agebnd  = pl.Enum(["65 - 69", "70 - 74", "75 - 79", "80 - 84"])

# cast column values to defined datatypes
df = df.with_columns(
    pl.col("vs").cast(enum_vs),
    pl.col("sex").cast(enum_sex),
    pl.col("dxGrp").cast(enum_dx),
    pl.col("site").cast(enum_site),
    pl.col("ageBand").cast(enum_agebnd))

# rename exam year column 
df = df.rename({'examY': 'year'})

# split off columns of "details"
df_details = df.select('UID', 'sex', 'mr', 'dxGrp', 'site', 'ageBand')
print("\n\ndetails dataframe 'df_details' variables will be re-joined asfter expansion")
print(df_details)

# keep columns needed for expansion
df = df.select('UID', 'examD', 'exitD', 'vs', 'age', 'year')
print("\nStudy dataframe 'df' now contains only variables needed for expansion")
print(df)

# tDelta constants to be used in exposure calculation
tDelta_1yr = timedelta(days=365, hours=6)
tDelta_1day  = timedelta(days=1)

# calculate total number of intervals for each subject
df = df.with_columns(totalIntvls = ((pl.col('exitD') - pl.col('examD') + tDelta_1day)/tDelta_1yr)
                     .ceil().cast(pl.Int32))
print(df)

# create temporary column of identical interval index lists
df = df.with_columns(idx_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])

# take slice of idx_list and store list in new column ‘intvl’
df = df.with_columns(intvl = pl.col('idx_list').list.slice(0, pl.col('totalIntvls')))
df = df.drop('idx_list')

#  explode to get desired tabular output
df = df.explode(pl.col('intvl'))

# create temporary column of offset years, then calculate ‘intvl_date’
df = df.with_columns(offsetYrs = (pl.col("intvl") - 1).cast(pl.String))
df = df.with_columns(intvl_date = pl.col('examD').dt.offset_by((pl.col('offsetYrs')) + "y"), 
        intvl_age = pl.col('age') + pl.col('intvl') - 1)
## df = df.filter(pl.col('exitD') > pl.col('intvl_date'))
print(df)

# three important calculations: interval year, exposure, and actual
df = df.with_columns(
    # first the interval year (for acquiring qx)
    year = pl.col('intvl_date').dt.year(),

    # second the exposure
    persYrs = pl
        .when((pl.col('intvl_date').dt.offset_by('1y')) <= pl.col('exitD'))
        .then(pl.lit(1))
        .otherwise((pl.col('exitD') - pl.col('intvl_date') + tDelta_1day) / tDelta_1yr),

    # actual deaths
    actual = pl
        .when((pl.col('vs') == "dead") & (pl.col('intvl') 
            == pl.col('totalIntvls')))
        .then(pl.lit(1))
        .otherwise(pl.lit(0)))

# clean up and get ready for join
df = df.select('UID', 'vs', 'intvl', 'intvl_date', 'intvl_age', 
                         'year', 'persYrs', 'actual')
df = df.rename({'intvl_age': 'age'})
df = df.join(df_details, on='UID', how='left')
print(df)

# load mortality dataframe
df_mort = USMort.both_sexes_1933_2023()
print("\nHMD: USA, both sexes, 1933-2023")
print(df_mort)

# join to get qx
df = df.join(df_mort, on=('year','age','sex'), how='left')
print(df)

# last calculation, expected deaths
df = df.with_columns(expected = pl.col('persYrs') * pl.col('qx') * pl.col('mr')).sort('UID', 'intvl')

df = df.select('UID', 'age', 'sex', 'mr', 'dxGrp', 'site', 'ageBand', 'intvl', 'intvl_date', 'year',
               'persYrs', 'actual', 'qx', 'expected').sort('UID', 'intvl')

# -----------------------------------------------------
# print("\nthe 7 leftmost columns")
print(df[:,:7])

with pl.Config(
    tbl_cell_numeric_alignment="RIGHT",
    float_precision=5,
):
    print("\nthe 7 rightmost columns")
    print(df[:,7:])
# ----------------------------------------------------

df.write_csv(r'data/a2e_results_' + itr + '.csv')
# double-clicking the csv file will open it in MS Excel

# ----------------------------------------------------
end_time = time.time()    # Record the end time
script_time = end_time - start_time
print(f"Script time: {script_time:.4f} seconds") 

