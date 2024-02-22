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

    def get_fct_table_name(self):
        fct_info = self.load_config("fct_info")
        return list(fct_info.keys())[0]

    def get_fct_table(self):
        fct_info = self.load_config("fct_info")
        return fct_info["fctJobListings"]

    def get_all_fct_table_columns(self):
        fctTable = self.get_fct_table()
        return fctTable["dimKeyColumns"] + fctTable["otherColumns"]

    def get_fct_unique_columns(self):
        fctTable = self.get_fct_table()
        return fctTable["uniqueColumns"]
