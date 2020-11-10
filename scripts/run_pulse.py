import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from TickerFlask.products import Pulse
pulse = Pulse(verbose=True)
pulse.run()