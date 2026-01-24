import os
import glob
import matplotlib.pyplot as plt
from nltk.stem import PorterStemmer

# --- INIT ---
stemmer = PorterStemmer()

# --- HELPER FUNCTIONS ---

def convert_phrase_to_path(phrase):
    """
    Translates natural language (Test Set) to DIRT Path format.
    Must match the Java logic exactly.
    """
    inner = phrase.replace("X", "").replace("Y", "").strip()
    words = inner.split()

    if not words:
        return None

    # Case 1: "X cause Y"
    if len(words) == 1:
        v_stem = stemmer.stem(words[0])
        return f"N:<nsubj:V:{v_stem}:>dobj:N"

    # Case 2: "X confuse with Y"
    if len(words) == 2 and words[1] != "by":
        v_stem = stemmer.stem(words[0])
        prep = words[1]
        return f"N:<nsubj:V:{v_stem}:>prep:P:{prep}:>pobj:N"

    # Case 3: "X cause by Y" (Passive)
    if len(words) == 2 and words[1] == "by":
        v_stem = stemmer.stem(words[0])
        return f"N:<nsubjpass:V:{v_stem}:>agent:P:by:>pobj:N"

    return None

def normalize_pair(p1, p2):
    """Ensures A-B is treated the same as B-A."""
    return tuple(sorted((p1, p2)))

def load_ground_truth(file_path):
    pairs = set()
    print(f"Loading ground truth from: {file_path}")
    if not os.path.exists(file_path):
        print(f"WARNING: File {file_path} not found!")
        return pairs

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) >= 2:
                path1 = convert_phrase_to_path(parts[0])
                path2 = convert_phrase_to_path(parts[1])
                if path1 and path2:
                    pairs.add(normalize_pair(path1, path2))

    print(f" -> Loaded {len(pairs)} pairs.")
    return pairs

def load_system_output(output_dir):
    """
    Loads all part-r-* files.
    Returns: list of (pair_tuple, score) sorted descending by score.
    Only score > 0 appears in the Hadoop output, so we keep that.
    """
    system_pairs = []
    files = glob.glob(os.path.join(output_dir, "part-r-*"))

    print(f"Loading system output from {len(files)} files in {output_dir}...")

    for filename in files:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 3:
                    try:
                        p1 = parts[0]
                        p2 = parts[1]
                        score = float(parts[2])
                        if score > 0:
                            system_pairs.append((normalize_pair(p1, p2), score))
                    except ValueError:
                        continue

    system_pairs.sort(key=lambda x: x[1], reverse=True)
    print(f" -> Loaded {len(system_pairs)} scored pairs (score > 0).")
    return system_pairs

def get_score_map(system_pairs):
    """
    Build a lookup map: pair -> max score seen.
    (Pair could theoretically appear multiple times across part files.)
    """
    score_map = {}
    for pair, score in system_pairs:
        if pair not in score_map or score > score_map[pair]:
            score_map[pair] = score
    return score_map

def find_optimal_threshold(system_pairs, pos_set, neg_set):
    """Finds the threshold that maximizes F1 over labeled pairs only."""
    print("\nFinding optimal threshold...")

    best_stats = {'f1': -1, 'thresh': 0.0, 'prec': 0.0, 'rec': 0.0}
    total_positives = len(pos_set)
    curr_tp = 0
    curr_fp = 0

    for pair, score in system_pairs:
        if pair in pos_set:
            curr_tp += 1
        elif pair in neg_set:
            curr_fp += 1
        else:
            continue  # ignore unlabeled

        prec = curr_tp / (curr_tp + curr_fp) if (curr_tp + curr_fp) > 0 else 0.0
        rec = curr_tp / total_positives if total_positives > 0 else 0.0
        f1 = (2 * prec * rec / (prec + rec)) if (prec + rec) > 0 else 0.0

        if f1 > best_stats['f1']:
            best_stats.update({'f1': f1, 'thresh': score, 'prec': prec, 'rec': rec})

    print("-" * 40)
    print(f"BEST THRESHOLD: {best_stats['thresh']:.6f}")
    print(f"Max F1:         {best_stats['f1']:.4f}")
    print(f"Precision:      {best_stats['prec']:.4f}")
    print(f"Recall:         {best_stats['rec']:.4f}")
    print("-" * 40)

    return best_stats['thresh']

def calculate_pr_curve_data(system_pairs, pos_set, neg_set):
    """PR curve points using only labeled pairs encountered in ranked output."""
    precisions, recalls = [], []
    total_pos = len(pos_set)
    tp, fp = 0, 0

    for pair, _score in system_pairs:
        if pair in pos_set:
            tp += 1
        elif pair in neg_set:
            fp += 1
        else:
            continue

        if tp + fp > 0:
            precisions.append(tp / (tp + fp))
            recalls.append(tp / total_pos if total_pos > 0 else 0.0)

    return recalls, precisions

def print_error_analysis(system_pairs, pos_set, neg_set, threshold):
    """
    Prints 5 examples each of TP/FP/FN/TN.
    TNs are negative gold pairs whose score < threshold (including score 0 if absent).
    """
    print("\n=== ERROR ANALYSIS (For Report) ===")

    tp_examples = []
    fp_examples = []
    fn_examples = []
    tn_examples = []

    score_map = get_score_map(system_pairs)

    # --- TP / FP from system output above threshold; FN from below threshold ---
    for pair, score in system_pairs:
        if score >= threshold:
            if pair in pos_set and len(tp_examples) < 5:
                tp_examples.append((pair, score))
            elif pair in neg_set and len(fp_examples) < 5:
                fp_examples.append((pair, score))
        else:
            if pair in pos_set and len(fn_examples) < 5:
                fn_examples.append((pair, score))

        if len(tp_examples) >= 5 and len(fp_examples) >= 5 and len(fn_examples) >= 5:
            break

    # --- Fill FNs for positives not found at all (score = 0) ---
    for p in pos_set:
        if len(fn_examples) >= 5:
            break
        if p not in score_map:
            fn_examples.append((p, 0.0))

    # --- TNs: negatives with score < threshold (including missing -> 0.0) ---
    # We iterate over the gold negative set, and use score_map.get(pair, 0.0).
    for n in neg_set:
        if len(tn_examples) >= 5:
            break
        s = score_map.get(n, 0.0)
        if s < threshold:
            tn_examples.append((n, s))

    print("\n--- 5 True Positives (Good Matches) ---")
    if not tp_examples:
        print("(None found above threshold)")
    for p, s in tp_examples:
        print(f"Score: {s:.4f} | {p}")

    print("\n--- 5 False Positives (System said yes, Truth said no) ---")
    if not fp_examples:
        print("(None found above threshold)")
    for p, s in fp_examples:
        print(f"Score: {s:.4f} | {p}")

    print("\n--- 5 True Negatives (Correct Rejections) ---")
    if not tn_examples:
        print("(None found below threshold — unlikely, check inputs)")
    for p, s in tn_examples:
        print(f"Score: {s:.4f} | {p}")

    print("\n--- 5 False Negatives (System missed these) ---")
    if not fn_examples:
        print("(None found below threshold)")
    for p, s in fn_examples:
        print(f"Score: {s:.4f} | {p}")

    print("===================================\n")

def main():
    # --- CONFIG ---
    OUTPUT_DIR = "./output_large"  # directory containing part-r-* files
    POS_FILE = "positive-preds.txt"
    NEG_FILE = "negative-preds.txt"
    # --------------

    # 1. Load Data
    pos_set = load_ground_truth(POS_FILE)
    neg_set = load_ground_truth(NEG_FILE)
    system_pairs = load_system_output(OUTPUT_DIR)

    if not system_pairs:
        print("No scores found. Check your output files.")
        return

    # 2. Find Optimal Threshold
    best_thresh = find_optimal_threshold(system_pairs, pos_set, neg_set)
    if best_thresh <= 0:
        best_thresh = 0.01  # fallback

    # 3. Print Examples for Report (TP/FP/TN/FN)
    print_error_analysis(system_pairs, pos_set, neg_set, best_thresh)

    # 4. Plot PR Curve
    recalls, precisions = calculate_pr_curve_data(system_pairs, pos_set, neg_set)
    if recalls:
        plt.figure(figsize=(8, 6))
        plt.plot(recalls, precisions, label='DIRT Algorithm', color='blue')
        plt.xlabel('Recall')
        plt.ylabel('Precision')
        plt.title('Precision-Recall Curve')
        plt.grid(True)
        plt.legend()
        plt.savefig("precision_recall_curve.png")
        print("Graph saved as 'precision_recall_curve.png'")
        plt.show()
    else:
        print("Not enough labeled data points to plot curve.")

if __name__ == "__main__":
    main()

