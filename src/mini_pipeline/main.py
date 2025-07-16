from dynaconf import Dynaconf

if __name__ == '__main__':
    app_config = Dynaconf(
        settings_files=['app_config.yaml'],
    )
