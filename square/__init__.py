from . import square

__version__ = '0.23.0'

# Expose the main functions of Square directly for convenience.
get = square.get_resources
plan = square.make_plan
apply_plan = square.apply_plan
show_plan = square.show_plan
