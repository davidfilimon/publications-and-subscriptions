# Pub-Sub Data Generator

Generator of publications and subscriptions for publish-subscribe systems,
with thread-based parallelism and automatic statistical verification of distributions.

---

## Project structure

```
pubsub_generator/
├── generator.py   # Core generation logic (publications + subscriptions)
├── writer.py      # Saving results to text files
├── stats.py       # Statistical verification of field frequencies
├── main.py        # CLI entry point (single / benchmark mode)
├── README.md      # This file
└── output/        # Generated files (created automatically)
    ├── publications_t1.txt
    ├── subscriptions_t1.txt
    ├── publications_t4.txt
    └── subscriptions_t4.txt
```

---

## Data format

**Publication** (fixed field structure):
```
{(company,"Google");(value,90.0);(drop,10.0);(variation,0.73);(date,2.02.2022)}
```

**Subscription** (optional fields, each with an operator and value):
```
{(company,=,"Google");(value,>=,90.0);(variation,<,0.8)}
```

---

## Functionality

### Publications
- `company` — chosen at random from a predefined set of 15 companies
- `date` — chosen at random from a predefined set of 16 dates
- `value`, `drop`, `variation` — random floats within configurable ranges

### Subscriptions
- Field frequencies are **exact** (not approximate):
  index permutation is used to guarantee exactly X out of N subscriptions
  include a given field — no statistical drift
- The frequency of the `=` operator for configurable fields is also exact,
  applied within the subset of subscriptions that include the field
- Missing fields are correctly omitted from the subscription output

---

## Configuration (in `main.py` or via CLI arguments)

| Parameter                    | Default value            |
|------------------------------|--------------------------|
| Number of publications       | 10 000                   |
| Number of subscriptions      | 10 000                   |
| Frequency of `company`       | 90%                      |
| Frequency of `value`         | 70%                      |
| Frequency of `drop`          | 50%                      |
| Frequency of `variation`     | 60%                      |
| Frequency of `date`          | 40%                      |
| `=` frequency for `company`  | 70% (of those with field)|
| `value` range                | [10.0, 200.0]            |
| `drop` range                 | [0.0, 50.0]              |
| `variation` range            | [0.0, 5.0]               |

---

## Usage

```bash
# Default benchmark: 1 thread vs 4 threads, 10 000 messages
python main.py

# Single run with 4 threads
python main.py --mode single --threads 4

# Custom benchmark
python main.py --mode benchmark \
               --publications 50000 \
               --subscriptions 50000 \
               --benchmark-threads 1,2,4,8

# Custom output directory
python main.py --output-dir results/
```

---

## Performance evaluation (parallelism)

### Parallelism type
**Threads** (`threading.Thread` from Python's standard library)

### Mechanism
- Publications are generated independently per thread (no synchronization needed;
  each thread writes into its own pre-allocated slice of the result list)
- Subscriptions: the index assignment (which fields are present and which use `=`)
  is computed **before** threads are launched; threads generate random values in
  parallel and write into disjoint index ranges

### Benchmark results — 10 000 publications + 10 000 subscriptions

| Parallelism factor | Publications (s) | Subscriptions (s) | Total (s) | Speedup |
|-------------------:|-----------------:|------------------:|----------:|--------:|
| 1 thread           | 0.0217           | 0.1025            | 0.1242    | x1.00   |
| 4 threads          | 0.0357           | 0.0876            | 0.1233    | x1.01   |

### Processor specifications
- **Architecture**: x86_64
- **Logical CPUs**: 2 (execution environment with 2 vCPUs)
- **OS**: Linux

### Note on the GIL
Python's Global Interpreter Lock (GIL) limits true parallel execution of threads
for CPU-bound code. The modest speedup above is a consequence of this limitation.
For genuine parallelism in Python, `multiprocessing` (separate processes) can be
used to bypass the GIL. On machines with more physical cores and larger workloads
(50 000+ messages), the difference becomes more pronounced.

---

## Statistical verification

Each run automatically prints a verification report:

```
SUBSCRIPTION STATISTICS VERIFICATION
============================================================
Total subscriptions: 10000

  Field: company
    Required frequency:  90%
    Actual frequency:    90.0%  [OK]
    Required '=' freq (of field):  70%
    Actual   '=' freq (of field):  70.0%  [OK]

  Field: value
    Required frequency:  70%
    Actual frequency:    70.0%  [OK]
  ...
```

---

## Team

Filimon David-Christian
Pintescu Sebastian
