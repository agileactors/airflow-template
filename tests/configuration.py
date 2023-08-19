from pathlib import PurePath

from dotenv import dotenv_values

import tests

dot_env_file = PurePath(tests.__file__).parent.parent.joinpath(".env.integrationtesting")

config = dotenv_values(dot_env_file)
