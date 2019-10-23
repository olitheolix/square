from . import square

__version__ = '0.16.1'

# Expose the main functions directly for convenience.
get = square.get_resources
plan = square.make_plan
apply = square.main_apply
