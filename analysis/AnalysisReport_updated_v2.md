# Assignment 3: DIRT Algorithm Analysis Report

### Authors

- Gal Schwartz – 322271891  
- Dina Gurevich – 322405911

## 1. Experimental Setup
The DIRT (Discovery of Inference Rules from Text) algorithm was implemented using a 4-stage MapReduce pipeline. To evaluate the effect of corpus size on the quality of extracted inference rules, the system was executed on two different dataset sizes derived from the Google Biarcs corpus:
1.  **Small Dataset:** 10 input files.
2.  **Large Dataset:** 100 input files.

The extracted inference rules were evaluated against a Ground Truth dataset containing positive (synonymous) and negative (non-synonymous) path pairs.

## 2. Quantitative Evaluation
The table below summarizes the performance metrics calculated at the optimal F1 threshold for both datasets.

| Metric | Small Dataset (10 Files) | Large Dataset (100 Files) |
| :--- | :--- | :--- |
| **Pairs Found** | 5 | 538 |
| **Optimal Threshold** | 0.028530 | 0.004192 |
| **Precision** | **1.0000 (100%)** | **0.9797 (98%)** |
| **Recall** | 0.0008 (0.08%) | 0.1214 (12.14%) |
| **F1 Score** | 0.0017 | **0.2161** |

### Analysis of Results
* **Precision:** The system demonstrates exceptionally high precision in both configurations. Even with limited data, the algorithm does not "hallucinate" false relationships; when it finds a match, it is almost statistically certain to be correct. The 98% precision on the large dataset proves that the Lin Similarity measure and Mutual Information logic are implemented correctly and effectively filter noise.
* **Recall & Data Sparsity:** The dramatic difference in Recall (0.08% vs 12.1%) highlights the **Data Sparsity** problem inherent in distributional similarity algorithms. With only 10 files, the feature vectors (words appearing in slots X and Y) rarely overlap with the Test Set paths, resulting in near-zero recall. Increasing the data by 10x (to 100 files) increased the number of discovered pairs by over **100x** (from 5 to 538), demonstrating that the algorithm's performance scales non-linearly with data volume.

## 3. Precision-Recall Curve Analysis

### Small Dataset Graph
* **Observation:** The graph for the small dataset appears empty or degenerated to a single point.
* **Analysis:** This is expected behavior. In the small run, almost all candidate pairs have **score 0.0** (i.e., they are not assigned any non-zero similarity), so there are effectively **no non-zero points** to trace into a curve. With only one scored true positive (`affect` ↔ `attack`) and the rest at 0.0, changing the threshold cannot create additional points, so the plot collapses to a single visible point near $(x \approx 0, y=1.0)$.
* <img width="800" height="600" alt="precision_recall_curve" src="https://github.com/user-attachments/assets/8138a9ab-79be-4174-a695-2eb8c5720e72" />


### Large Dataset Graph
* **Observation:** The graph shows a curve that starts at high precision (1.0) and descends in "steps."
* **Analysis:** The curve maintains near-perfect precision for the initial segment, indicating that the highest-scored pairs are exclusively correct. The "steps" or drops in the curve represent specific threshold points where false positives (e.g., antonyms or contextually related but non-synonymous words) are introduced into the result set. The fact that the curve extends to ~0.12 Recall (unlike the small dataset) confirms the improved coverage provided by the larger corpus.
  <img width="800" height="600" alt="precision_recall_curve (1)" src="https://github.com/user-attachments/assets/87410109-9e2b-4812-90bc-c175f1f39d42" />


## 4. Error Analysis
This section lists **5 examples** for each category (TP/FP/TN/FN) and compares their similarity scores between the Small (10 files) and Large (100 files) runs.

**Note:** If a pair does not appear in the system output, it is treated as having **score 0.0** (i.e., below the reporting threshold).

### 4.1 True Positives (System: Yes, Truth: Yes)
| Pair (Path A ↔ Path B) | Score (Large) | Score (Small) |
| :--- | ---: | ---: |
| `lead to` ↔ `result in` | **0.1536** | 0.0000 |
| `die from` ↔ `die of` | **0.1309** | 0.0000 |
| `protect against` ↔ `protect from` | **0.1075** | 0.0000 |
| `consist of` ↔ `contain` | **0.0983** | 0.0000 |
| `affect` ↔ `attack` | 0.0285 | **0.0285** |

### 4.2 False Positives (System: Yes, Truth: No)
| Pair (Path A ↔ Path B) | Score (Large) | Score (Small) |
| :--- | ---: | ---: |
| `contract` (dobj) ↔ `die of` | 0.0655 | - |
| `die of` ↔ `suffer from` | 0.0580 | - |
| `contract` (dobj) ↔ `die from` | 0.0418 | - |
| `avoid in` ↔ `use in` | 0.0331 | - |
| `die of` ↔ `get` (dobj) | 0.0264 | - |

**Small run:** no false positives were found above the optimal threshold (Precision = 1.0), so the small-score entries in the FP table are marked as `-` (not enough examples).

### 4.3 True Negatives (System: No, Truth: No)
All examples below have score **0.0**, meaning the system correctly rejected them (or never produced them) in both runs.

#### Large (100 files) — sample TNs
| Pair (Path A ↔ Path B) | Score (Large) | Score (Small) |
| :--- | ---: | ---: |
| `be in` ↔ `occur in` | 0.0000 | 0.0000 |
| `destroy` ↔ `produce` | 0.0000 | 0.0000 |
| `have` ↔ `kill` | 0.0000 | 0.0000 |
| `differ from` ↔ `include` | 0.0000 | 0.0000 |
| `produce` ↔ `use in` | 0.0000 | 0.0000 |

#### Small (10 files) — sample TNs
| Pair (Path A ↔ Path B) | Score (Small) | Score (Large) |
| :--- | ---: | ---: |
| `kill` ↔ `produced by` | 0.0000 | 0.0000 |
| `confound with` ↔ `differ from` | 0.0000 | 0.0000 |
| `derive from` ↔ `destroy` | 0.0000 | 0.0000 |
| `differ from` ↔ `resemble` | 0.0000 | 0.0000 |
| `die of` ↔ `get` | 0.0000 | 0.0264 |

### 4.4 False Negatives (System: No, Truth: Yes)
These are gold positive pairs that the system missed (score 0.0). In both runs, they illustrate **coverage limitations** and **feature mismatch** (insufficient shared slot fillers for the pair).

#### Large (100 files) — sample FNs
| Pair (Path A ↔ Path B) | Score (Large) | Score (Small) |
| :--- | ---: | ---: |
| `give for` ↔ `require` (dobj) | 0.0000 | 0.0000 |
| `relieve with` ↔ `take for` | 0.0000 | 0.0000 |
| `accompany` (dobj) ↔ `cause` (dobj) | 0.0000 | 0.0000 |
| `protect from` ↔ `reduce` (dobj) | 0.0000 | 0.0000 |
| `associate with` ↔ `attend with` | 0.0000 | 0.0000 |

#### Small (10 files) — sample FNs
| Pair (Path A ↔ Path B) | Score (Small) | Score (Large) |
| :--- | ---: | ---: |
| `transmit` (dobj) ↔ `transmitted by` | 0.0000 | 0.0000 |
| `control` (dobj) ↔ `reduce` (dobj) | 0.0000 | 0.0000 |
| `have` (dobj) ↔ `use` (dobj) | 0.0000 | 0.0000 |
| `give` (dobj) ↔ `produced by` | 0.0000 | 0.0000 |
| `break` (dobj) ↔ `convert` (dobj) | 0.0000 | 0.0000 |

**Common FN pattern:** even with 100 files, many test-set templates are still rare, and DIRT’s distributional overlap requirement (shared slot fillers across paths) often fails to trigger.

## 5. Conclusion
The implementation successfully reproduces the key DIRT behavior: **very high precision** with **recall strongly dependent on corpus size**.

* The **Small (10 files)** run is dominated by data sparsity: only **5 scored pairs** were produced, yielding **Precision = 1.0** but **Recall ≈ 0.0008** and **F1 ≈ 0.0017** at the best threshold (0.028530).
* The **Large (100 files)** run produces far more candidate similarities (**538 scored pairs**) and reaches **Recall ≈ 0.1214** while maintaining **Precision ≈ 0.9797**, for **F1 ≈ 0.2161** at threshold 0.004192.
* The error analysis shows the classic DIRT failure mode: **contextual relatedness** (especially in medical domains) can look like synonymy, producing false positives such as `contract` ↔ `die of` and `die of` ↔ `suffer from`.

Overall, scaling the corpus substantially improves coverage, but fully addressing the remaining false negatives and relatedness-based false positives would likely require either much larger corpora or additional constraints/features (e.g., directional entailment checks, selectional preference modeling, or explicit negative evidence).
