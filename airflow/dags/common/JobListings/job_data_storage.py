# from io import BytesIO
from io import StringIO


class JobDataStorage:
    def save_from_spark_as_delta(self, df, target_path):
        # overwrite: overwrite mode is important, otherwise errors
        # mergeSchema: because still in development and otherwise error
        df.write.option("mergeSchema", "true").format("delta").mode("overwrite").save(
            target_path
        )

    def save_from_spark_as_csv(self, df, target_path):
        df.write.csv(target_path, mode="overwrite", header=True)

    def save_df_to_s3_as_csv(s3_client, df, bucket, source_name, filename):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        file_key = f"{source_name}/csv/{filename}.csv"

        s3_client.put_object(Bucket=bucket, Key=file_key, Body=csv_buffer.getvalue())
