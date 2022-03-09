import sys

def print_progress(i, n, label=None):
    l = f"{label}: " if label else ""
    perc = int((100 / n) * i * 10) / 10.0
    sys.stdout.write('\r')
    sys.stdout.write(f"{l}{perc}% [{i} of {n}]")
    if i == n:
        sys.stdout.write("\n")
    sys.stdout.flush()