import os
import yaml


class JobConfigManager:
    def __init__(self, file_name) -> None:
        self.file_name = file_name

    # @staticmethod
    def load_config(self, config_part):
        """Lädt Konfiguration aus einer YAML-Datei."""
        dir_path = os.path.dirname(os.path.abspath(__file__))
        config_file_path = os.path.join(dir_path, self.file_name)
        with open(config_file_path, "r") as file:
            config = yaml.safe_load(file)

        return config[config_part]

    def get_fact_table_name(self):
        fact_info = self.load_config("fact_info")
        return list(fact_info.keys())[0]

    def get_all_fact_table_columns(self):
        fact_info = self.load_config("fact_info")
        factTable = fact_info["fctJobListings"]
        return factTable["dimKeyColumns"] + factTable["otherColumns"]
