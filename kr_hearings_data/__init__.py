"""Korean National Assembly Hearings Data."""

from kr_hearings_data._loader import (
    download,
    info,
    load_dyads,
    load_meetings,
    load_speeches,
    load_table,
    load_turns,
)

__all__ = ["load_turns", "load_meetings", "load_dyads", "load_table", "load_speeches", "download", "info"]
__version__ = "0.2.0"
