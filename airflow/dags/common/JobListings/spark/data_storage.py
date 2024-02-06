

class DataStorage:
    def save_to_delta(self, df, target_path):
        # overwrite: overwrite mode is important, otherwise errors
        # mergeSchema: because still in development and otherwise error
        df.write.option("mergeSchema", "true").format(
            "delta").mode("overwrite").save(target_path)

    def save_as_csv(self, df, target_path):
        df.write.csv(target_path, mode="overwrite", header=True)
