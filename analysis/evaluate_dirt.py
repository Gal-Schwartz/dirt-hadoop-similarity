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
    
    # Sort descending by score
    system_pairs.sort(key=lambda x: x[1], reverse=True)
    print(f" -> Loaded {len(system_pairs)} scored pairs (score > 0).")
    return system_pairs

def find_optimal_threshold(system_pairs, pos_set, neg_set):
    """Finds the threshold that maximizes F1."""
    print("\nFinding optimal threshold...")
    
    best_stats = {'f1': -1, 'thresh': 0, 'prec': 0, 'rec': 0}
    total_positives = len(pos_set)
    curr_tp = 0
    curr_fp = 0
    
    for _, (pair, score) in enumerate(system_pairs):
        if pair in pos_set:
            curr_tp += 1
        elif pair in neg_set:
            curr_fp += 1
        else:
            continue # Skip pairs not in test set
            
        if curr_tp + curr_fp > 0:
            prec = curr_tp / (curr_tp + curr_fp)
            rec = curr_tp / total_positives
            if prec + rec > 0:
                f1 = 2 * (prec * rec) / (prec + rec)
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
    precisions, recalls = [], []
    total_pos = len(pos_set)
    tp, fp = 0, 0
    
    for pair, _ in system_pairs:
        if pair in pos_set:
            tp += 1
        elif pair in neg_set:
            fp += 1
        else:
            continue
            
        if tp + fp > 0:
            precisions.append(tp / (tp + fp))
            recalls.append(tp / total_pos)
            
    return recalls, precisions

def print_error_analysis(system_pairs, pos_set, neg_set, threshold):
    print("\n=== ERROR ANALYSIS (For Report) ===")
    
    tp_examples = []
    fp_examples = []
    fn_examples = []
    
    # Check TPs and FPs
    for pair, score in system_pairs:
        if score >= threshold:
            if pair in pos_set:
                if len(tp_examples) < 5: tp_examples.append((pair, score))
            elif pair in neg_set:
                if len(fp_examples) < 5: fp_examples.append((pair, score))
        else:
            # Check FNs (found by system but below threshold)
            if pair in pos_set and len(fn_examples) < 5:
                fn_examples.append((pair, score))
    
    # Check FNs (Not found by system at all)
    found_pairs = {p for p, s in system_pairs}
    for p in pos_set:
        if len(fn_examples) >= 5: break
        if p not in found_pairs:
            fn_examples.append((p, 0.0))

    print("\n--- 5 True Positives (Good Matches) ---")
    for p, s in tp_examples: print(f"Score: {s:.4f} | {p}")

    print("\n--- 5 False Positives (System said yes, Truth said no) ---")
    if not fp_examples: print("(None found above threshold)")
    for p, s in fp_examples: print(f"Score: {s:.4f} | {p}")

    print("\n--- 5 False Negatives (System missed these) ---")
    for p, s in fn_examples: print(f"Score: {s:.4f} | {p}")
    print("===================================\n")

def main():
    # --- CONFIG ---
    OUTPUT_DIR = "./output_large"  # Put your part-r files here
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
    if best_thresh == 0: best_thresh = 0.01 # Fallback

    # 3. Print Examples for Report
    print_error_analysis(system_pairs, pos_set, neg_set, best_thresh)

    # 4. Plot Curve
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
        print("Not enough data to plot curve.")

if __name__ == "__main__":
    main()