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
| **Optimal Threshold** | 0.0285 | 0.0042 |
| **Precision** | **1.0000 (100%)** | **0.9797 (98%)** |
| **Recall** | 0.0008 (0.08%) | 0.1214 (12.1%) |
| **F1 Score** | 0.0017 | **0.2161** |

### Analysis of Results
* **Precision:** The system demonstrates exceptionally high precision in both configurations. Even with limited data, the algorithm does not "hallucinate" false relationships; when it finds a match, it is almost statistically certain to be correct. The 98% precision on the large dataset proves that the Lin Similarity measure and Mutual Information logic are implemented correctly and effectively filter noise.
* **Recall & Data Sparsity:** The dramatic difference in Recall (0.08% vs 12.1%) highlights the **Data Sparsity** problem inherent in distributional similarity algorithms. With only 10 files, the feature vectors (words appearing in slots X and Y) rarely overlap with the Test Set paths, resulting in near-zero recall. Increasing the data by 10x (to 100 files) increased the number of discovered pairs by over **100x** (from 5 to 538), demonstrating that the algorithm's performance scales non-linearly with data volume.

## 3. Precision-Recall Curve Analysis

### Small Dataset Graph
* **Observation:** The graph for the small dataset appears empty or degenerated to a single point.
* **Analysis:** This is expected behavior. Since the system only identified **one** true positive pair (`affect` $\leftrightarrow$ `attack`) out of 1,194 possible pairs, the Recall axis effectively stays at 0. The curve consists of a single point at $(x \approx 0, y=1.0)$. This visualizes the extreme lack of coverage caused by the small corpus size.

### Large Dataset Graph
* **Observation:** The graph shows a curve that starts at high precision (1.0) and descends in "steps."
* **Analysis:** The curve maintains near-perfect precision for the initial segment, indicating that the highest-scored pairs are exclusively correct. The "steps" or drops in the curve represent specific threshold points where false positives (e.g., antonyms or contextually related but non-synonymous words) are introduced into the result set. The fact that the curve extends to ~0.12 Recall (unlike the small dataset) confirms the improved coverage provided by the larger corpus.

## 4. Error Analysis
This section analyzes specific examples and **compares their similarity scores** between the Small (10 files) and Large (100 files) datasets.

### 4.1 True Positives (High Scoring Matches)
The table below compares successful detections. Note that due to data sparsity, the Small dataset failed to find these pairs (Score 0.0).

| Pair (Path A $\leftrightarrow$ Path B) | Score (Large) | Score (Small) | Analysis |
| :--- | :--- | :--- | :--- |
| `lead to` $\leftrightarrow$ `result in` | **0.1536** | 0.0 | Correct identification of causality. The 100-file corpus provided enough context to link these transitive verbs. |
| `die from` $\leftrightarrow$ `die of` | **0.1309** | 0.0 | Correctly handles prepositional variation. The system learned that "from" and "of" are interchangeable here. |
| `protect against` $\leftrightarrow$ `protect from` | **0.1075** | 0.0 | Another successful identification of interchangeable prepositions. |
| `consist of` $\leftrightarrow$ `contain` | **0.0983** | 0.0 | Identifies a part-whole relationship (Meronymy) that functions as a synonym in many contexts. |
| `affect` $\leftrightarrow$ `attack` | 0.0285 | **0.0285** | **The only pair found in both.** It shows that highly common verbs can be detected even in small datasets. |

### 4.2 False Positives (System: Yes, Truth: No)
Errors where the system claimed similarity, but the ground truth labeled them as negative.

| Pair (Path A $\leftrightarrow$ Path B) | Score (Large) | Score (Small) | Analysis |
| :--- | :--- | :--- | :--- |
| `contract` $\leftrightarrow$ `die of` | 0.0655 | 0.0 | **Contextual Similarity:** Both paths appear with the same medical nouns (e.g., "contract cancer", "die of cancer"). The system confuses *relatedness* (cause-effect) with *similarity*. |
| `avoid in` $\leftrightarrow$ `use in` | 0.0331 | 0.0 | **Antonym Problem:** Antonyms often share identical contexts (e.g., "avoid in cooking" vs "use in cooking"). Statistical methods struggle to distinguish strong contrasts from synonyms without negative constraints. |

### 4.3 False Negatives (Missed Pairs)
Pairs that are synonyms but received a score of 0.0 in both runs.

| Pair (Path A $\leftrightarrow$ Path B) | Score (Large) | Score (Small) | Analysis |
| :--- | :--- | :--- | :--- |
| `produce` $\leftrightarrow$ `accompanied by` | 0.0 | 0.0 | **Feature Mismatch:** Even with 100 files, the specific slots filling these paths did not overlap sufficiently. |
| `need for` $\leftrightarrow$ `require for` | 0.0 | 0.0 | **Coverage Limit:** Suggests that 100 files are still a small sample relative to the entire language complexity. |

## 5. Conclusion
The implementation of the DIRT algorithm was successful. The system achieved a high precision rate (~98%), validating the extraction and calculation logic. The comparative analysis between the 10-file and 100-file runs provides empirical evidence that unsupervised relation extraction is highly sensitive to corpus size. While the large dataset significantly improved coverage (finding 538 pairs vs 5), many valid pairs (False Negatives) were still missed, suggesting that running on the full corpus would yield further improvements without compromising precision.