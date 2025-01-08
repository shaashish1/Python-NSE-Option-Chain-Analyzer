import multiprocessing
import time

# Import the main functions from the three scripts
import futures
import nifty_50_cash
import optionchain

def run_futures():
    print("Starting futures.py...")
    futures.main()

def run_nifty_50_cash():
    print("Starting nifty_50_cash.py...")
    nifty_50_cash.main()

def run_optionchain():
    print("Starting optionchain.py...")
    optionchain.main()

if __name__ == "__main__":
    print("Starting all scripts...")

    # Create processes for each script
    processes = [
        multiprocessing.Process(target=run_futures),
        multiprocessing.Process(target=run_nifty_50_cash),
        multiprocessing.Process(target=run_optionchain),
    ]

    # Start all processes
    for process in processes:
        process.start()

    # Wait for all processes to complete
    for process in processes:
        process.join()

    print("All scripts finished running.")
