from argparse import ArgumentParser, RawDescriptionHelpFormatter
from datetime import datetime
from itertools import groupby
from time import sleep
from typing import Iterator

import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

DEFAULT_PORT = 9999


def setup_items_stream(spark: SparkSession, address: str = 'localhost', port: int = DEFAULT_PORT) -> DataFrame:
    return (spark
        .readStream
        .format('socket')
        .option('host', address)
        .option('port', port)
        .load()
        # Treat as CSV
        .select(F.split('value', ',', limit=2).alias('split_cols'))
        .select(F.col('split_cols')[0].alias('item'), F.to_timestamp(F.col('split_cols')[1]).alias('timestamp'))
    )    


def dgim_update(key: tuple, pdfs: Iterator[pd.DataFrame], state: GroupState, N: int) -> Iterator[pd.DataFrame]:
    
    # Initialize the state and get the old buckets state
    if not state.exists:
        state.update(([],))
    
    (buckets,) = state.get

    # Merge all Pandas Dataframes into a single Dataframe and sort it so we can know the absolute ordering of the new values
    pdf_aggregate = pd.concat(pdfs, ignore_index=True)
    pdf_aggregate.sort_values(by=['timestamp'], ascending=False, inplace=True)
    pdf_aggregate.reset_index(inplace=True)   # the index is scrambled after sorting, reset it to normal for use as the timestamps in the next loop
    # Take the real-time timestamp and replace with integer timestamp
    for end_timestamp, bit in pdf_aggregate['item'].items():
        if bit == '1':
            buckets.append((1, end_timestamp))

    maximum_bit_timestamp = pdf_aggregate.shape[0]

    # Update the timestamp of the old buckets (they are now older than the oldest in the current batch of bits)
    # Additionally, remove the buckets that are too old
    buckets = [(bucket_size, end_timestamp + maximum_bit_timestamp) for bucket_size, end_timestamp in buckets if end_timestamp + maximum_bit_timestamp <= N]

    new_buckets_merged = []
    # Keep track of the last merged bucket
    merged_buckets = []

    # Deal first with the smaller buckets
    buckets.sort(key=lambda t: t[0])
    for _, buckets_of_same_size in groupby(buckets, key=lambda t: t[0]):
        # Sort the buckets themselves by the end timestamp (earliest first, which are those with larger timestamp)
        buckets_of_same_size = sorted(buckets_of_same_size, key=lambda t: t[1], reverse=True)

        # If we merged buckets of the previous size, add them to this batch since they now belong to it
        if len(merged_buckets) > 0:
            buckets_of_same_size.extend(merged_buckets)
            merged_buckets.clear()

        while len(buckets_of_same_size) > 2:
            # Merge the earliest buckets
            (bitsum1, _) = buckets_of_same_size.pop(0)
            (bitsum2, end_timestamp2) = buckets_of_same_size.pop(0)
            merged_buckets.append((bitsum1 + bitsum2, end_timestamp2))

        new_buckets_merged.extend(buckets_of_same_size)

    # Leftover merged buckets
    if len(merged_buckets) > 0:
        new_buckets_merged.extend(merged_buckets)

    new_buckets_merged.sort(key=lambda t: t[1], reverse=True)

    state.update((new_buckets_merged,))

    yield pd.DataFrame(new_buckets_merged, columns=['bucket_size', 'end_timestamp'])


def edw_update(key: tuple, pdfs: Iterator[pd.DataFrame], state: GroupState, c: float, score_threshold: float) -> Iterator[pd.DataFrame]:
    
    # Initialize the state and get the old counts state
    if not state.exists:
        state.update(({},))
    
    (counts,) = state.get

    # Merge all Pandas Dataframes into a single Dataframe and sort it so we can know the absolute ordering of the new values
    # Also reset the index so that we can get proper ordering
    pdf_aggregate = pd.concat(pdfs, ignore_index=True)
    pdf_aggregate.sort_values(by=['timestamp'], ascending=False, inplace=True)
    
    time_elaped = pdf_aggregate.shape[0]
    pdf_aggregate['time_step'] = range(time_elaped)

    # Update old counts (make them older)
    old_decay = ((1 - c)**time_elaped)
    counts = {item: count * old_decay for item, count in counts.items() if count * old_decay >= score_threshold}

    # Add new counts
    for item, group in pdf_aggregate.groupby(by='item'):
        counts.setdefault(item, 0)
        counts[item] += float(((1 - c)**group['time_step']).sum())

    state.update((counts,))

    counts_items = counts.items()
    yield pd.DataFrame({'item': [i for i, _ in counts_items], 'count': [c for _, c in counts_items]}, columns=['item', 'count'])



def dgim(
    N: int,
    k: int,
    t: int,
    port: int = DEFAULT_PORT,
):
    
    spark = SparkSession.builder.appName('StructuredStreaming').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    items = setup_items_stream(spark, port=port)

    bucket_schema = (T.StructType()
        .add('bucket_size', T.IntegerType(), nullable=False)
        .add('end_timestamp', T.IntegerType(), nullable=False)
    )
    # The output Pandas Dataframe has columns bucket_size and end_timestamp
    output_schema = bucket_schema
    # The user-defined state is a tuple, containing an array where each element is another tuple containing the bucket_size and end_timestamp
    state_schema = (T.StructType()
        .add('buckets', T.ArrayType(bucket_schema, containsNull=False), nullable=False)
    )

    # Halve the last bucket if the window size doesn't fully cover it
    col_dgim_error_correction = F.when(F.first('end_timestamp') + F.first('bucket_size') > k, F.floor(F.first('bucket_size') / 2)).otherwise(0)

    outputter = (items
        # Disregard items that are too old
        .withWatermark('timestamp', '1 hour') 
                
        # Don't group
        .withColumn('dummy_key', F.lit(1))
        .groupby('dummy_key')
        
        # Keep and update DGIM buckets over time
        .applyInPandasWithState(
            lambda key, pdfs, state: dgim_update(key, pdfs, state, N),
            outputStructType=output_schema,
            stateStructType=state_schema,
            outputMode='append',
            timeoutConf=GroupStateTimeout.NoTimeout
        )

        # Add a column with the processing time, and then only consider the latest rows according to it
        # We have to do this because applyInPandasWithState does not support 'complete' mode
        .withColumn('processing_timestamp', F.current_timestamp())
        .groupby('processing_timestamp')
        .agg(F.collect_list(F.struct('bucket_size', 'end_timestamp')).alias('computed_buckets'))

        # Output the counts to be queried
        .writeStream
        .trigger(processingTime=f'{t} seconds')
        .outputMode('complete')
        .format('memory')
        .queryName('outputterMem')
        .start()
    )

    # Keep processing until manual interruption
    while True:
        count_1s = (spark.sql('SELECT * FROM outputterMem')
            .sort('processing_timestamp')
            .withColumn('dummy_key', F.lit(1))
            .groupby('dummy_key')
            .agg(F.explode(F.last('computed_buckets')).alias('computed_buckets_exploded'))
            .select(F.col('computed_buckets_exploded').bucket_size.alias('bucket_size'), F.col('computed_buckets_exploded').end_timestamp.alias('end_timestamp'))
            .filter(F.col('end_timestamp') <= k)
            .agg((F.sum('bucket_size') - col_dgim_error_correction).alias('total_ones'))
        ).tail(5)

        print(f'[{datetime.now()}] Number of 1s in last {k}:', count_1s[0]['total_ones'])
        print()

        sleep(t)
        



def edw(
    c: float,
    threshold: float,
    t: int,
    port: int = DEFAULT_PORT,
):
    
    spark = SparkSession.builder.appName('StructuredStreaming').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    items = setup_items_stream(spark, port=port)

    counts_schema = (T.StructType()
        .add('item', T.StringType())
        .add('count', T.DoubleType())
    )
    # The output Pandas Dataframe has columns item and count
    output_schema = counts_schema
    # The user-defined state is a tuple, containing a map of item counts
    state_schema = (T.StructType()
        .add('counts', T.MapType(T.StringType(), T.DoubleType(), valueContainsNull=False), nullable=False)
    )

    outputter = (items
        # Disregard items that are too old
        .withWatermark('timestamp', '1 hour') 
                
        # Don't group
        .withColumn('dummy_key', F.lit(1))
        .groupby('dummy_key')
        
        # Keep and update exponentially decaying counts over time
        .applyInPandasWithState(
            lambda key, pdfs, state: edw_update(key, pdfs, state, c, threshold),
            outputStructType=output_schema,
            stateStructType=state_schema,
            outputMode='append',
            timeoutConf=GroupStateTimeout.NoTimeout
        )

        # Add a column with the processing time, and then only consider the latest rows according to it
        # We have to do this because applyInPandasWithState does not support 'complete' mode
        .withColumn('processing_timestamp', F.current_timestamp())
        .groupby('processing_timestamp')
        .agg(F.collect_list(F.struct('item', 'count')).alias('computed_counts'))


        # Offer the counts to be queried. The processing is done in fixed intervals of 1 second
        .writeStream
        .trigger(processingTime='1 second')
        .outputMode('complete')
        .format('memory')
        .queryName('outputterMem')
        .start()
    )

    # Keep processing until manual interruption.
    while True:
        top_5 = (spark.sql('SELECT * FROM outputterMem')
            .sort('processing_timestamp')
            .withColumn('dummy_key', F.lit(1))
            .groupby('dummy_key')
            .agg(F.explode(F.last('computed_counts')).alias('computed_counts_exploded'))
            .select(F.col('computed_counts_exploded').item.alias('item'), F.col('computed_counts_exploded').count.alias('count'))
            .sort('count', ascending=True)
        ).tail(5)

        print(f'[{datetime.now()}] Top 5 items:')
        print(*(f'{k}: {v}' for k, v in top_5), sep='\n')
        print()

        sleep(t)
        


if __name__ == '__main__':

    default_str = ' (default: %(default)s)'

    parser = ArgumentParser(
        prog="DGIM_EDW_Streaming",
        formatter_class=RawDescriptionHelpFormatter,
        description="""Python-file version of Exercise 2's Jupyter notebook, for submission via spark-submit.
The documentation is present in the notebook.
When submitting the script, the streams should be running on the background with a socket connection:
- for the DGIM algorithm, the script 'simulate_synthetic_stream.py' should be running;
- for the EDW algorithm, the script 'simulate_timestamped_stream.py' should be running, with the uncompressed data from 'stream_data.csv' fed into this script's standard input.
  Example: gunzip -dc data/stream_data.csv.gz | python3 simulate_timestamped_stream.py
The streams will be processed until a KeyboardInterrupt command is issued to the submitted script."""
    )
    parser.add_argument("algorithm", choices=["DGIM", "EDW"], help="the streaming algorithm to execute")
    parser.add_argument("-t", type=int, default=10, help="the frequency with which the algorithm's results are displayed" + default_str)
    parser.add_argument("--port", type=int, default=9999, help="the port on which to listen for the stream" + default_str)
    parser.add_argument("--dgim.N", type=int, default=1000, help="the maximum number of bits in the stream to consider" + default_str)
    parser.add_argument("--dgim.k", type=int, default=900, help="the window size on which to estimate counts" + default_str)
    parser.add_argument("--edw.c", type=float, default=1e-6, help="count smoothing factor")
    parser.add_argument("--edw.threshold", type=float, default=0.5, help="weight threshold below which items are discarded" + default_str)

    args = parser.parse_args()
    args_dgim = {k.split(".")[1]: v for k, v in vars(args).items() if k.startswith("dgim.")}
    args_edw = {k.split(".")[1]: v for k, v in vars(args).items() if k.startswith("edw.")}

    if args.algorithm == "DGIM":
        dgim(**args_dgim, t=args.t, port=args.port)

    elif args.algorithm == "EDW":
        edw(**args_edw, t=args.t, port=args.port)
