def start_stream(spark, args, schema_iterated, output_iterated, output_reduction, exprs):
    stream = (
        spark.readStream.schema(schema_iterated)
        # DELETE will only delete the input files of the previous batch when a new batch gets started.
        # This means the input files of the final batch will remain in the folder.
        .option("cleanSource", "OFF" if args["keep_tmps"] else "DELETE")
        .parquet(str(output_iterated))
        # .groupby("group", "motif")
        .groupby("motif")
        .agg(*exprs)  # keeps checkpoints of aggregation​
    )
    stream = (
        stream.writeStream
        # there are no semantics to output only once when stopping, so every batch gets outputted and overwritten by the next batch
        .outputMode("complete")
        # default trigger is 0 seconds, so one batch at a time, every time there is input
        # balance between producing and reducing will differ for more iteration intensive settings
        .trigger(processingTime=args["trigger"])
        .foreachBatch(lambda df, _: df.repartition(1).write.mode("overwrite").parquet(str(output_reduction)))
        .start()
    )
    return stream

def stop_stream(stream):
    stream.processAllAvailable()  # blocks until all Parquets files are reduced
    stream.stop()  # stop stream
