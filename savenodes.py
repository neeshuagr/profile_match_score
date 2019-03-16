#!/usr/bin/python3.4

import dcrgraphcompactor
import utility
import datetime
import dcrconfig

utility.write_to_file(dcrconfig.ConfigManager().SemanticGraphLogFile, 'a',
        'Semantic graph Generation Step 9..! (savenodes.py) ' + str(datetime.datetime.now()))

dcrgraphcompactor.save_node_dict()
