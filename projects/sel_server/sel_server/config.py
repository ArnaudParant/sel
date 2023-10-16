import configparser
import os


class EnvInterpolation(configparser.BasicInterpolation):
    """ Allow to overwrite configuration by environment variables """

    def before_get(self, parser, section, option, value, defaults):
        """ Warning: {option} is put in lowercase by configparser, but not {section} """
        value = super().before_get(parser, section, option, value, defaults)
        var_name = f"CONF_{section}_{option}"
        value = os.environ.get(var_name, value)
        return value


def read():
    config = configparser.ConfigParser(interpolation=EnvInterpolation())
    config.read("conf.ini")
    return config
