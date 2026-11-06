| Dataset | Records | Baseline | + batched DB writes | + normalisation fast paths (final) | Total speed-up | Peak RSS (final) |
|---|---:|---:|---:|---:|---:|---:|
| sample | 100 | 0.006s (18,150/s) | 0.005s (19,039/s) | 0.005s (21,558/s) | 1.20x | 66.3 MB |
| 10k | 10,000 | 0.434s (23,060/s) | 0.405s (24,690/s) | 0.388s (25,801/s) | 1.12x | 85.5 MB |
| 100k | 100,000 | 4.369s (22,888/s) | 4.467s (22,388/s) | 3.909s (25,584/s) | 1.12x | 231.4 MB |
