import os
import yaml


class JobConfigManager:

    @staticmethod
    def load_config(file_name, config_part):
        """Lädt Konfiguration aus einer YAML-Datei."""
        dir_path = os.path.dirname(os.path.abspath(__file__))
        config_file_path = os.path.join(dir_path, file_name)
        with open(config_file_path, 'r') as file:
            config = yaml.safe_load(file)

        return config[config_part]
