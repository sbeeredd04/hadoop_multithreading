import json
import re
import threading
import time
from pathlib import Path


# NameNode: splits the data file into N subsets, one for each TaskTracker
def name_node(file_path, num_splits):
    """Split the data file into N line subsets, one per TaskTracker."""
    # Read every line from the input file
    with open(file_path, "r", encoding="utf-8") as f:
        all_lines = f.readlines()

    # Decide how many lines go into each subset
    total_lines = len(all_lines)
    chunk_size = total_lines // num_splits
    if chunk_size == 0:
        chunk_size = 1

    # Build the list of subsets
    subsets = []
    for i in range(num_splits):
        start = i * chunk_size
        # The last subset takes any leftover lines
        if i == num_splits - 1:
            end = total_lines
        else:
            end = start + chunk_size
        subsets.append(all_lines[start:end])

    return subsets



# Map function: converts a subset of text into <word, 1> key-value pairs
def map_function(lines):
    """Convert lines of text into a list of (word, 1) key-value pairs."""
    pairs = []
    for line in lines:
        # Lower-case the line so the count is case-insensitive
        line = line.lower()
        # Pull out only the word characters (letters, digits, underscore)
        # drop the punctuation like commas, periods, parentheses, etc.
        words = re.findall(r"[a-zA-Z0-9']+", line)
        for word in words:
            pairs.append((word, 1))
    return pairs


# Reduce function to sum the 1s for every word in its subset
def reduce_function(pairs):
    """Sum the counts for each word and return a {word: count} dict."""
    counts = {}
    for word, value in pairs:
        if word in counts:
            counts[word] += value
        else:
            counts[word] = value
    return counts



# TaskTracker to run Map then Reduce on its subset and store the result
def task_tracker(task_id, lines, results, lock):
    """Run Map then Reduce on one subset and store the partial result."""
    print(f"  TaskTracker-{task_id} started ({len(lines)} lines)")

    #Map (text -> key-value pairs)
    mapped = map_function(lines)

    # Reduce (sum the counts for each word in this subset)
    reduced = reduce_function(mapped)

    # Save the partial result so the Combiner can pick it up.
    # A lock protects the shared list from being changed by two threads at exactly the same moment.
    with lock:
        results.append(reduced)

    print(f"  TaskTracker-{task_id} finished ({len(reduced)} unique words)")


# Combiner merges the partial results from all reducers into one result 
def combiner(partial_results):
    """Merge all per-reducer dicts into one final {word: count} dict."""
    final_counts = {}
    for partial in partial_results:
        for word, count in partial.items():
            if word in final_counts:
                final_counts[word] += count
            else:
                final_counts[word] = count
    return final_counts


# Single-threaded run (used to compare execution times)
def run_single_thread(file_path, num_splits):
    """Run the whole MapReduce job on one thread and time it."""
    print("\n--- Single-thread run ---")
    start_time = time.time()

    # NameNode splits the file
    subsets = name_node(file_path, num_splits)

    # Run each TaskTracker in one single thread 
    partial_results = []
    for i, subset in enumerate(subsets):
        mapped = map_function(subset)
        reduced = reduce_function(mapped)
        partial_results.append(reduced)
        print(f"  Task {i} done ({len(reduced)} unique words)")

    # Combiner builds the final result
    final = combiner(partial_results)

    end_time = time.time()
    elapsed = end_time - start_time
    print(f"Single-thread time: {elapsed:.4f} seconds")
    return final, elapsed


# Multi-threaded run
def run_multi_thread(file_path, num_splits):
    """Run the MapReduce job using N parallel threads and time it."""
    print("\n--- Multi-thread run ---")
    start_time = time.time()

    # NameNode splits the file into N subsets
    subsets = name_node(file_path, num_splits)

    # Shared list to collect partial results from each TaskTracker
    partial_results = []
    lock = threading.Lock()

    # Create one thread per subset
    threads = []
    for i, subset in enumerate(subsets):
        t = threading.Thread( target=task_tracker, args=(i, subset, partial_results, lock),)
        threads.append(t)

    # Start all threads so they run in parallel
    for t in threads:
        t.start()

    # Wait for every thread to finish before combining
    for t in threads:
        t.join()

    # Combiner merges all partial results into one final result
    final = combiner(partial_results)

    end_time = time.time()
    elapsed = end_time - start_time
    print(f"Multi-thread time: {elapsed:.4f} seconds")
    return final, elapsed


# Simple console UI
def get_user_input():
    """Ask the user for the data file path and the thread count N."""
    print(f"=" * 60)
    print(" Word Count MapReduce (Python multithreading)")
    print(f"=" * 60)

    # Ask for the file path. Press Enter to use the default file.
    default_file = "FileForCounting.txt"
    file_path = input(f"Enter data file path [default: {default_file}]: ").strip()
    if file_path == "":
        file_path = default_file

    # Make sure the file actually exists before continuing
    if not Path(file_path).is_file():
        print(f"ERROR: File '{file_path}' was not found.")
        return None, None

    # Ask for the number of threads (must be at least 1)
    n_text = input("Enter number of parallel threads N (>=1) [default: 4]: ").strip()
    if n_text == "":
        num_threads = 4
    else:
        try:
            num_threads = int(n_text)
        except ValueError:
            print("ERROR: N must be a whole number.")
            return None, None
        if num_threads < 1:
            print("ERROR: N must be at least 1.")
            return None, None

    return file_path, num_threads


def display_results(final_counts, single_time, multi_time, num_threads):
    """Print the top words plus timings and save the full result as JSON."""
    print("\n" + f"=" * 60)
    print(" Results")
    print(f"=" * 60)

    # Sort the words by count (highest first) so the top words are easy to see
    sorted_items = sorted(final_counts.items(), key=lambda item: item[1], reverse=True)

    print(f"Total unique words: {len(final_counts)}")
    print(f"Total word count:   {sum(final_counts.values())}")
    print(f"Threads used (N):   {num_threads}")

    print("\nTop 20 most common words:")
    for word, count in sorted_items[:20]:
        print(f"  {word:<20} {count}")

    print("\nExecution times:")
    print(f"  Single-thread: {single_time:.4f} seconds")
    print(f"  Multi-thread:  {multi_time:.4f} seconds")
    if multi_time > 0:
        speedup = single_time / multi_time
        print(f"  Speedup:       {speedup:.2f}x")

    # Save the full result as JSON (required by the assignment)
    output_path = "wordcount_result.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(final_counts, f, indent=2, sort_keys=True)
    print(f"\nFull JSON result saved to: {output_path}")


def main():
    file_path, num_threads = get_user_input()
    if file_path is None:
        return

    # Run once with a single thread, then once with N threads, and compare
    _, single_time = run_single_thread(file_path, num_threads)
    final_counts, multi_time = run_multi_thread(file_path, num_threads)

    # Show the final word counts and timing info
    display_results(final_counts, single_time, multi_time, num_threads)


if __name__ == "__main__":
    main()
