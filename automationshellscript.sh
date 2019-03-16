#!/bin/bash
if pgrep -f "python3.4 ~/Documents/Code/DataReadfromFile/resumeread.py" &>/dev/null; then
    echo "it is already running"
    exit
else
    python3.4 ~/Documents/Code/DataReadfromFile/dataread.py && python3.4 ~/Documents/Code/DataReadfromFile/resumeread.py 
fi









