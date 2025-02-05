from pathlib import Path
import json


MILLI_SECONDS_DAY = 3600 * 24 * 1000
LOOKUP = {"remove": "deletionTimestamp", "add": "modificationTime", "commitInfo": "timestamp"}

def clean_log(log, day, counter):
    if "protocol" in log or "metaData" in log:
        return log
    for key in list(LOOKUP.keys()):
        if key in log:
            value = LOOKUP[key]
            break
    else:
        raise ValueError(f"No valid key found. Must be one of {list(LOOKUP.keys())}")
    log[key][value] = log[key][value] - day + counter
    return log


def spread_out_log_timestamps(data_dir: str):
    json_files = sorted(list(Path(data_dir).glob("_delta_log/*.json")))
    #space this out to one commit per day

    N_COMMITS = len(json_files)

    for n_file, f in enumerate(json_files):
        lines = []
        with open(f, "r") as fp:
            for n_line, l in enumerate(fp):
                log = json.loads(l)
                log = clean_log(log, (N_COMMITS - n_file) * MILLI_SECONDS_DAY, n_line)
                lines.append(json.dumps(log))
        with open(f, "w") as fp:
            fp.write("\n".join(lines))



