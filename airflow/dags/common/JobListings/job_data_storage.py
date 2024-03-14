# from io import BytesIO
from io import StringIO


class JobDataStorage:
    def save_from_spark_as_delta(self, df, target_path):
        print("saving Delta...")
        # overwrite: overwrite mode is important, otherwise errors
        # mergeSchema: because still in development and otherwise error
        # df.write.option("mergeSchema", "true").format("delta").mode("overwrite").save(
        df.write.option("overwriteSchema", "true").format("delta").mode(
            "overwrite"
        ).save(target_path)
        print("saved Delta...")

    def save_from_spark_as_csv(self, df, target_path):
        print("saving CSV...")
        df.write.csv(target_path, mode="overwrite", header=True)
        print("saved CSV...")

    def save_df_to_s3_as_csv(s3_client, df, bucket, source_name, filename):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        file_key = f"{source_name.lower()}/csv/{filename}.csv"

        s3_client.put_object(Bucket=bucket, Key=file_key, Body=csv_buffer.getvalue())
