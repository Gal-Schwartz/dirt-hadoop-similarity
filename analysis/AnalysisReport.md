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
| **Pairs Found** | 17 | 538 |
| **Optimal Threshold** | 0.025647 | 0.004192 |
| **Precision** | **0.7692 (77%)** | **0.9797 (98%)** |
| **Recall** | 0.0084 (0.8%) | 0.1214 (12.14%) |
| **F1 Score** | **0.0166** | **0.2161** |

### Analysis of Results
* **Precision:** The system demonstrates exceptionally high precision in both configurations. Even with limited data, the algorithm does not "hallucinate" false relationships; when it finds a match, it is almost statistically certain to be correct. The 98% precision on the large dataset proves that the Lin Similarity measure and Mutual Information logic are implemented correctly and effectively filter noise.
* **Recall & Data Sparsity:** The dramatic difference in Recall (0.08% vs 12.1%) highlights the **Data Sparsity** problem inherent in distributional similarity algorithms. With only 10 files, the feature vectors (words appearing in slots X and Y) rarely overlap with the Test Set paths, resulting in near-zero recall. Increasing the data by 10x (to 100 files) increased the number of discovered pairs by over **30x** (from 17 to 538), demonstrating that the algorithm's performance scales non-linearly with data volume.

## 3. Precision-Recall Curve Analysis

### Small Dataset Graph
* **Observation:** This PR curve for the small experiment shows it achieves moderate–high precision on the few pairs it scores highly, but it retrieves only a tiny fraction of the true positives.
* **Analysis:** in the small run, DIRT is producing only a small number of non-zero similarity pairs, and the ones it does produce are often correct, but coverage is weak.
 <img width="800" height="600" alt="precision_recall_curve (2)" src="https://github.com/user-attachments/assets/2322eb42-cae1-4611-9a53-a31245f5ee1f" />



### Large Dataset Graph
* **Observation:** The graph shows a curve that starts at high precision (1.0) and descends in "steps."
* **Analysis:** The curve maintains near-perfect precision for the initial segment, indicating that the highest-scored pairs are exclusively correct. The "steps" or drops in the curve represent specific threshold points where false positives are introduced into the result set. 
  <img width="800" height="600" alt="precision_recall_curve (1)" src="https://github.com/user-attachments/assets/2c590502-2d73-4db6-b381-fe4982d3feb7" />



## 4. Error Analysis
This section lists **5 examples** for each category (TP/FP/TN/FN) and compares their similarity scores between the Small (10 files) and Large (100 files) runs.

### 4.1 True Positives (System: Yes, Truth: Yes)
#### Large (100 files) 
| Pair (Path A ↔ Path B) | Score (Large) |
| :--- | ---: |
| `lead to` ↔ `result in` | **0.1536** |
| `die from` ↔ `die of` | **0.1309** |
| `protect against` ↔ `protect from` | **0.1075** |
| `consist of` ↔ `contain` | **0.0983** |
| `affect` ↔ `attack` | **0.0285** |

#### Small (10 files)
| Pair (Path A ↔ Path B) | Score (Small) |
| :--- | ---: |
| `die from` ↔ `die of` | **0.1243** |
| `consist of` ↔ `contain` | **0.1036** |
| `covert into` ↔ `convert to` | **0.0665** |
| `differ from` ↔ `distinguish from` | **0.0570** |
| `eliminate` ↔ `eradicate` | **0.0501** |

### 4.2 False Positives (System: Yes, Truth: No)
#### Large (100 files)
| Pair (Path A ↔ Path B) | Score (Large) |
| :--- | ---: |
| `contract` ↔ `die of` | **0.0655** |
| `die of` ↔ `suffer from` | **0.0580** |
| `contract` ↔ `die from` | **0.0418** |
| `avoid in` ↔ `use in` | **0.0331** |
| `die of` ↔ `get` | **0.0264** |

#### Small (10 files)
| Pair (Path A ↔ Path B) | Score (Small) |
| :--- | ---: |
| `constrict` ↔ `dilate` | **0.1055** |
| `contract` ↔ `die of` | **0.0710** |
| `contract` ↔ `die from` | **0.0284** |
| `develop` ↔ `die of` | **0.0290** |

### 4.3 True Negatives (System: No, Truth: No)
All examples below have score **0.0**, meaning the system correctly rejected them in both runs.

#### Large (100 files) — sample TNs
| Pair (Path A ↔ Path B) | Score (Large) | 
| :--- | ---: |
| `be in` ↔ `occur in` | 0.0000 |
| `destroy` ↔ `produce` | 0.0000 |
| `have` ↔ `kill` | 0.0000 |
| `differ from` ↔ `include` | 0.0000 |
| `produce` ↔ `use in` | 0.0000 |

#### Small (10 files) — sample TNs
| Pair (Path A ↔ Path B) | Score (Small) |
| :--- | ---: |
| `kill` ↔ `produced by` | 0.0000 |
| `confound with` ↔ `differ from` | 0.0000 |
| `derive from` ↔ `destroy` | 0.0000 |
| `differ from` ↔ `resemble` | 0.0000 |
| `die of` ↔ `get` | 0.0000 |

### 4.4 False Negatives (System: No, Truth: Yes)

#### Large (100 files) — sample FNs
| Pair (Path A ↔ Path B) | Score (Large) |
| :--- | ---: |
| `give for` ↔ `require` | 0.0000 |
| `relieve with` ↔ `take for` | 0.0000 |
| `accompany` ↔ `cause` | 0.0000 |
| `protect from` ↔ `reduce` | 0.0000 |
| `associate with` ↔ `attend with` | 0.0000 |

#### Small (10 files) — sample FNs
| Pair (Path A ↔ Path B) | Score (Small) |
| :--- | ---: |
| `transmit` ↔ `transmitted by` | 0.0000 |
| `control` ↔ `reduce` | 0.0000 |
| `have` ↔ `use` | 0.0000 |
| `give` ↔ `produced by` | 0.0000 |
| `break` ↔ `convert`| 0.0000 |

## 5. Conclusion
The results clearly demonstrate the data-dependent nature of the DIRT algorithm: precision remains high across settings, while recall improves dramatically as the corpus grows.

In the small experiment (10 files), the system identifies only 5–17 predicate pairs with non-zero similarity (depending on filtering), reflecting severe data sparsity. At the optimal threshold (0.025647), the model achieves Precision ≈ 0.769 (77%), but Recall ≈ 0.0084 (0.8%), resulting in a low F1 score of 0.0166. This indicates that although most predicted pairs are correct, the system recovers less than 1% of the gold paraphrase relations. The Precision–Recall curve for this setting is highly jagged and compressed near the recall axis, consistent with the very small number of scored pairs.

In contrast, the large experiment (100 files) produces 538 candidate predicate pairs, an increase of two orders of magnitude over the small run. With a much lower optimal threshold (0.004192), the system reaches Precision ≈ 0.9797 (98%) while achieving Recall ≈ 0.1214 (12.14%), yielding an F1 score of 0.2161. This substantial improvement in recall confirms that DIRT’s ability to identify paraphrastic relations depends strongly on observing sufficient shared argument contexts, which only emerge at larger corpus scales.
