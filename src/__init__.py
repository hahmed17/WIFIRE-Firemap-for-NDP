"""
Fire modeling workflow package.
Provides tools for wildfire simulation using FARSITE.
"""

import config
import geometry
import farsite
import firemap

__version__ = "0.1.0"

from geometry import (
    geom_to_state,
    state_to_geom,
    validate_geom,
    align_states,
    plot_geometry,
)

from farsite import (
    forward_pass_farsite,
    forward_pass_farsite_24h,
    cleanup_farsite_outputs,
)

from firemap import (
    fetch_fire_perimeters,
    fetch_weather,
)

__all__ = [
    'config', 'geometry', 'farsite', 'firemap',
    'geom_to_state', 'state_to_geom', 'validate_geom',
    'align_states', 'plot_geometry',
    'forward_pass_farsite', 'forward_pass_farsite_24h', 'cleanup_farsite_outputs',
    'fetch_fire_perimeters', 'fetch_weather',
]