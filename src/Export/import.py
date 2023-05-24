import sys,json
sys.path.insert(0,'..')

path = r"E:/01 System Program Data/MDB27018/"


# import Ex_FH as target
# target.upload(path = path + "CW_FH")

# import Ex_CW as target
# target.upload(path = path + "CW2")

# import Ex_ZGB as target
# target.upload(path = path + "CW_ZGB")

# import Ex_CJ as target
# target.upload(path = path + "EM_CJ")

# import Ex_DG as target
# target.upload(path = path + "EM_DG")

# import Ex_ZG as target
# target.upload(path = path + "EM_ZIX")
# target.upload_GG(path = path + "EM_GON")

# import Ex_REPORT as target
# target.upload(path = path + "EM_REP")

# import Ex_FOREX as target
# target.upload(path = path + "QUATEFOR")

import Ex_QUATE as target
target.upload(path = path + "QUATE")
target.upload_IDX(path = path + "QUATE_IDX")