# Design Document: Lexico-Syntactic Similarity (DIRT)

### Authors

- Gal Schwartz – 322271891  
- Dina Gurevich – 322405911


## 1. System Overview
The system implements the **DIRT (Discovery of Inference Rules from Text)** algorithm as proposed by Lin and Pantel. The objective is to automatically extract semantically similar inference rules (dependency paths) from a large corpus of text using the Distributional Hypothesis ("words that occur in the same contexts tend to have similar meanings").

The system is architected as a pipeline of **4 sequential MapReduce jobs** (plus an intermediate aggregation step), deployed on Amazon AWS EMR. The pipeline processes the Google Syntactic N-Grams dataset (Biarcs) to calculate Pointwise Mutual Information (MI) and Lin's Similarity Measure.

---

## 2. Detailed Job Design

### Job 1: Extraction & Counting
**Goal:** Parse the raw dataset, apply linguistic constraints, and aggregate raw counts for all required statistics.

* **Input:** Biarcs N-Gram dataset (S3).
* **Map Logic:**
    1.  **Parse:** Splits the tab-separated line to get the dependency tree.
    2.  **Extract Paths:** Iterates through the tokens to find paths between two words ($X$ and $Y$).
    3.  **Constraints Check:**
        * **Head Validation:** Ensures the path root (head) is a **Verb**.
        * **Slot Validation:** Ensures both slots $X$ and $Y$ are **Nouns**.
        * **Stopwords:** Filters out auxiliary verbs (e.g., "is", "have", "can") using a predefined list.
    4.  **Normalization:** Applies **Porter Stemmer** to all words to unify forms (e.g., "causes" $\to$ "caus").
    5.  **Emit:** Emits 4 types of records using tagged keys:
        * `TRIPLE <path> <slot> <word>` $\to$ `1` (for the numerator)
        * `SW_MARGIN <slot> <word>` $\to$ `1` (for $f(s,w)$)
        * `PS_MARGIN <path> <slot>` $\to$ `1` (for $f(p,s)$)
        * `GLOBAL` $\to$ `2` (Total $N$)
* **Combiner:**
    * **Functionality:** Performs local aggregation of counts on the Map node level.
    * **Benefit:** Significantly reduces network traffic (Shuffle phase) by sending summed counts instead of individual `1`s.
* **Reduce Logic:**
    * Sums the counts for each key.
    * Uses `MultipleOutputs` to write each record type to a separate directory (`triples`, `wordmargins`, `pathmargins`, `global`).

---

### Job 2: MI Calculation & Filtering
**Goal:** Calculate Pointwise Mutual Information (MI) for each triple.
$$MI(p, s, w) = \log \left( \frac{Count(p, s, w) \times N}{Count(p, s) \times Count(s, w)} \right)$$

* **Input:** Output of Job 1 (`triples` (Path-Slot-Word counts) and `pathmargins` (Path-Slot counts) directories).
* **Setup (Memory Management):**
    * Loads `wordmargins` (Slot-Word counts) into a `HashMap` in memory (Distributed Cache).
    * Loads global $N$ from configuration.
* **Map Logic:**
    * Reads `PS_MARGIN`: Emits with Type=0.
    * Reads `TRIPLE`: Lookups $Count(s,w)$ from the in-memory Map. If found, Emits with Type=1.
    * **Secondary Sort:** The custom key ensures Type 0 (Margin) arrives before Type 1 (Triples) at the Reducer.
* **Reduce Logic:**
    1.  Reads the first value. If it is a Margin (Type 0), stores $Count(p,s)$ in a variable.
    2.  Iterates through the remaining values (Triples).
    3.  Calculates MI using the stored counts.
    4.  **Thresholding:** If $MI < 0.001$, the triple is discarded.
* **Keys & Values:**
    * **Reduce Output Key:** `Path`
    * **Reduce Output Value:**  `Slot \t Word \t MI_Score`

---

### Job 2.5: Aggregation (Sum MI)
**Goal:** Pre-calculate the denominator for Lin's Similarity: $\sum_{w} MI(p, s, w)$.

* **Input:** Output of Job 2.
* **Map Logic:**
    * Extracts `Path` and `Slot`.
    * Emits `Key: Path+Slot`, `Value: MI`.
* **Combiner & Reducer Logic:**
    * Since the operation is a simple summation, the Reducer class is also used as a Combiner.
    * **Functionality:** Sums all MI scores for the given Path+Slot.
    * **Benefit:** Reduces the amount of data transferred to the Reducer by pre-aggregating sums locally.
* **Reduce Logic:**
    * Sums all MI scores for the given Path+Slot.
* **Keys & Values:**
    * **Output Key:** `Path \t Slot`.
    * **Output Value:**  (Sum of MI).

---

### Job 3: Feature Overlap (Test-Set Filter)
**Goal:** Identify shared features between paths in our corpus and paths in the Test Set.

* **Input:** Output of Job 2 (The vectors).
* **Setup:**
    * Loads the `TestSet` (Positive and Negative pairs) into a memory `HashMap`.
    * **Translator:** Converts natural language test pairs (e.g., "X cause Y") into internal Path format.
* **Map Logic:**
    * For each input path, checks if it appears in the Test Set.
    * If path $P_{in}$ is part of a test pair $(P_{in}, P_{other})$, it emits:
        * `Key: (P_in, P_other)` (The Pair ID)
        * `Value: Feature Vector (Word, MI)`
* **Reduce Logic:**
    * Receives all features for both paths in the pair.
    * Performs an intersection: If word $w$ exists in both lists, calculates $(MI_1 + MI_2)$.
    * Sums these overlaps to get the **Numerator**.
* **Keys & Values:**
   * **Reduce Output Key:**  `Path1 \t Path2`
   * **Reduce Output Value:** `Numerator_X \t Numerator_Y`
---

### Job 4: Final Similarity Score
**Goal:** Normalize the overlap by the vector sizes (Geometric Mean).

* **Input:** Output of Job 3 (Numerators).
* **Setup:**
    * Loads `SumMI` (Output of Job 2.5) into memory. This contains the **Denominators** ($\sum MI$).
* **Map Logic:** Identity (passes data through).
* **Reduce Logic:**
    * Reads the Numerator from input.
    * Lookups the Denominators for $P_1$ and $P_2$ from memory.
    * Calculates: $Sim = \frac{Numerator}{Count(X) + Count(X)} \times \dots$ (Geometric Average Formula).
* **Keys & Values:**
    * **Output Key:** `Path1 \t Path2`.
    * **Output Value:** (Final Similarity Score).

    