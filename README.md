# Word Count MapReduce (Python)

A simple Hadoop-style word count program using Python threads.

## What it does

It counts how many times each word shows up in a text file. The work is split across N threads, just like Hadoop splits work across nodes.

## Files

- `wordcount.py` — the program
- `FileForCounting.txt` — the input text file
- `wordcount_result.json` — the output (made when you run the program)

## How to run

You need Python 3. Open a terminal in this folder and type:

```
python3 wordcount.py
```

The program will ask you two questions:

1. **Data file path** — press Enter to use `FileForCounting.txt`.
2. **Number of threads (N)** — type a number like `4` and press Enter.

That's it. The program runs and prints the results.

## How it works

The program copies the Hadoop MapReduce idea using five small parts:

1. **NameNode** — opens the file and cuts the lines into N equal pieces.
2. **TaskTracker** — one thread for each piece. It runs Map then Reduce.
3. **Map** — turns text into pairs like `(word, 1)`. All words are made lower case so `Table` and `table` are the same.
4. **Reduce** — adds up the `1`s for each word in its piece.
5. **Combiner** — joins the results from every thread into one final list.

The program runs the job two times: once with one thread and once with N threads. It prints both times so you can compare them.

## Output

- The top 20 most common words show in the terminal.
- The full list of every word and its count is saved to `wordcount_result.json`.
