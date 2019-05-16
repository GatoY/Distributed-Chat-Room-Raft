import json
import sys

num_servers = int(sys.argv[1])
CONFIG = {'server_port':{}, 'server_on':[]}
for i in range(num_servers):
    CONFIG["server_port"][str(i)] = {'port': 12345 + i, 'server_port': 23333 + i}

json.dump(CONFIG, open('config.json', 'w'))

# CONFIG = {
#     "server_port": {
#         "1": {
#             "port": 12348,
#             "server_port": 23332
#         },
#         "2": {
#             "port": 12349,
#             "server_port": 23333
#         },
#         "3": {
#             "port": 12350,
#             "server_port": 23334
#         }
#     }
# }
