import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from TickerFlask.products import WebInterface
wi = WebInterface(refresh_rate=0, start_world=True)
wi.run()