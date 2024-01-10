import sys
from pathlib import Path
sys.path.append(Path("~", ".zipline").expanduser().as_posix())

from zipline.data.bundles import register

from daily_us_equities import daily_us_equities_bundle

register("quotemedia", daily_us_equities_bundle, calendar_name="XNYS")